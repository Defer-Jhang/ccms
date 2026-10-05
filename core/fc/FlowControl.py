import json
from logging import root
import os
import sys
import argparse
from collections import deque
import copy
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import protocol.wifi.meas as meas
import balps.mssql_mgr
import balps.influxdb_mgr
import balps.meas_map_mgr
from balps.log_mgr import logging_api
from datetime import datetime, timedelta, timezone
import redis
import xml.etree.ElementTree as ET  # noqa: S405
import xml.dom.minidom as minidom  # noqa: F401, S408

# User Define Code
import socket
import os
import sys
import time

"""
Flow Control
  Collect Data > Algorithm Execution or Device Sensing  > Write Data
"""

# UpData = []


class FC_api:
    def __init__(self, args):
        """
        Initialize db connection and flow control setting

        Args:
            args: Receive input parameter settings

        Returns:
            None

        Example:
            >>> fc_obj = FC_api()
        """
        try:
            self.logger = logging_api(filename=os.path.join("logs", f"{os.path.splitext(os.path.basename(sys.argv[0]))[0]}"), backup_count=7)
            self.logging = self.logger.get_logger()  # Create influxdb and mssql object
            self.sbid = args.serialboard_id
            self.shid = args.storehouse_id
            self.pbid = args.protectboard_id
            self.ip = args.ser_ip
            self.port = args.eport
            self.meas_instance = 0
            self.meas_data_pd = []
            self.meas_map = None
            self.alg_executor = None
            self.is_first_round = True
            self.running = True
            self.skip_w2002_reply = False
            self.skip_w2003_reply = False
            self.skip_w2003_step = None
            # Load Input Config
            with open(args.config) as json_file:
                self.config_data = json.load(json_file)

            self.mssql_obj = balps.mssql_mgr.mssql_api()
            self.influxdb_obj = balps.influxdb_mgr.influxdb_api()
            self.meas_map_obj = balps.meas_map_mgr.meas_map_api(
                self.influxdb_obj,
                self.config_data["meas"][0]["protect_params"],
                self.sbid,
                self.pbid
            )

            self.conn_obj = 0
            self.addr = 0
            # Internal message exchange from message manager to flow control
            self.redis_obj = redis.StrictRedis(host="127.0.0.1", port=6379, db=0)
            self.mode = "Normal"
            self.pallet_status = 1
            self.alarm_sender = False
            self.heartbeat_last = time.time()
            self.heartbeat_current = time.time()
            self.mpd_heartbeat_timeout = self.config_data["meas"][0]["variable"]["mpd_heartbeat_timeout"]
            self.standby_heartbeat_timeout = self.config_data["meas"][0]["variable"]["standby_heartbeat_timeout"]
            self.esp_connected = 0
            self.last_time = time.time()
            self.pending_ack_queue = deque()
            self.message_mapping = {
                "StoreHouseStatusRequest": "W2002",
                "StoreHouseStepCheckRequest": "W2003",
            }
            self.last_meas_write_time = time.time()
            self.meas_write_interval = 5.0
            self.active_request_context = None
        except Exception:
            self.logging.error(f"Flow control initialization failed\n{self.logger.get_slim_error_log()}")

    def get_logger(self):
        return self.logger

    def get_logging(self):
        return self.logging

    @staticmethod
    def _get_task_info_value(task_info, key, default=""):
        if task_info is None:
            return default
        try:
            value = task_info.get(key)
            if isinstance(value, dict):
                return value.get("0", default)
            return default if value is None else value
        except Exception:
            return default

    def _create_request_context(self, raw_data):
        response_data = balps.meas_map_mgr.TaskInfo()

        # W2003 does not carry pallet identity. Keep only stable identity fields
        # from the task cache, then overwrite request-specific fields from XML.
        try:
            previous_task_info = self.meas_map_obj.get_task_info()
        except Exception:
            previous_task_info = None

        for key in (
            "SerialBoardID",
            "PalletID",
            "PalletPosition",
            "ProtectBoardID",
            "StopStep",
        ):
            value = self._get_task_info_value(previous_task_info, key)
            if value not in (None, ""):
                response_data.set(key, value)

        response_data.set("SerialBoardID", self.sbid)
        response_data.set("StoreHouseID", self.shid)
        if self.pbid:
            response_data.set("ProtectBoardID", self.pbid)

        return {
            "raw_data": raw_data,
            "xml_string": None,
            "message_name": "",
            "step": None,
            "response_data": response_data,
            "task_info_updated": False,
            "skip_reply": False,
            "ack_sent": False,
            "completed": False,
        }

    def _populate_request_context(self, context, root):
        response_data = context["response_data"]
        message_name = (root.findtext("./HEADER/MESSAGENAME") or "").strip()
        context["message_name"] = message_name

        # W2002 contains its own pallet identity. Do not leak identity from a
        # previous request when the current XML omits a required value.
        if message_name == "StoreHouseStatusRequest":
            for key, value in (
                ("SerialBoardID", self.sbid),
                ("PalletID", ""),
                ("PalletPosition", ""),
                ("ProtectBoardID", self.pbid or ""),
                ("StopStep", ""),
            ):
                response_data.set(key, value)

        extract_keys = {
            "MessageName": "./HEADER/MESSAGENAME",
            "TransactionID": "./HEADER/TRANSACTIONID",
            "TrxID": "./BODY/TRX_ID",
            "LineID": "./BODY/LINE_ID",
            "StoreHouseID": "./BODY/StoreHouseID",
            "SerialBoardID": "./BODY/PalletInfo/Pallet/SerialBoardID",
            "PalletID": "./BODY/PalletInfo/Pallet/PalletID",
            "PalletPosition": "./BODY/PalletInfo/Pallet/PalletPosition",
            "ProtectBoardID": "./BODY/PalletInfo/Pallet/DRCID",
            "Step": "./BODY/STEP",
        }
        for key, xpath in extract_keys.items():
            value = root.findtext(xpath)
            if value is not None:
                response_data.set(key, value.strip())

        recipe_steps = root.findall("./BODY/RecipeInfo/RecipeStep")
        if recipe_steps:
            response_data.set("StopStep", recipe_steps[-1].findtext("Step") or "0")

        step_text = root.findtext("./BODY/STEP")
        if step_text is not None:
            try:
                context["step"] = int(step_text)
            except (TypeError, ValueError):
                context["step"] = step_text

        if message_name == "StoreHouseStatusRequest":
            context["skip_reply"] = self.skip_w2002_reply
        elif message_name == "StoreHouseStepCheckRequest":
            context["skip_reply"] = (
                self.skip_w2003_reply
                and context["step"] == self.skip_w2003_step
            )

    @staticmethod
    def _require_xml_text(root, xpath, field_name):
        value = root.findtext(xpath)
        if value is None or not value.strip():
            raise ValueError(f"Missing required XML field: {field_name}")
        return value.strip()

    def _validate_xml_request(self, xml_string, context):
        """Parse and fully validate a request before changing runtime state."""
        root = ET.fromstring(xml_string)  # noqa: S314
        if root.tag != "MESSAGE":
            raise ValueError(f"Invalid XML root element: {root.tag}")

        # Populate ACK metadata immediately after syntax parsing. Semantic
        # validation errors can then still return the original TID/TRX_ID.
        self._populate_request_context(context, root)
        message_name = context["message_name"]
        if not message_name:
            raise ValueError("Missing required XML field: MESSAGENAME")

        validated = {
            "root": root,
            "message_name": message_name,
            "device_info": None,
            "config_info": None,
            "step": None,
        }

        if message_name == "Disconnect":
            return validated

        if message_name not in self.message_mapping:
            raise ValueError(f"Unsupported message name: {message_name}")

        for xpath, field_name in (
            ("./HEADER/TRANSACTIONID", "TRANSACTIONID"),
            ("./BODY/TRX_ID", "TRX_ID"),
            ("./BODY/StoreHouseID", "StoreHouseID"),
        ):
            self._require_xml_text(root, xpath, field_name)

        if message_name == "StoreHouseStatusRequest":
            pallets = root.findall("./BODY/PalletInfo/Pallet")
            if len(pallets) != 1:
                raise ValueError(
                    "W2002 requires exactly one Pallet per device request"
                )

            device_info = self.meas_map_obj.parse_pallet_info(xml_string)
            if device_info is None or device_info.empty:
                raise ValueError("W2002 pallet information is empty")

            config_info = self.meas_map_obj.parse_config_info(xml_string)
            if config_info is None or config_info.empty:
                raise ValueError("W2002 configuration is empty")

            validated["device_info"] = device_info
            validated["config_info"] = config_info

        elif message_name == "StoreHouseStepCheckRequest":
            step_text = self._require_xml_text(root, "./BODY/STEP", "STEP")
            try:
                validated["step"] = int(step_text)
            except (TypeError, ValueError) as error:
                raise ValueError(f"Invalid STEP value: {step_text}") from error

        return validated

    def _remove_request_context(self, context):
        if self.active_request_context is context:
            self.active_request_context = None

        for index, pending in enumerate(self.pending_ack_queue):
            if pending is context:
                del self.pending_ack_queue[index]
                break

    def _clear_recovery_reply_state(self, context):
        message_name = context.get("message_name")
        if message_name == "StoreHouseStatusRequest":
            self.skip_w2002_reply = False
        elif message_name == "StoreHouseStepCheckRequest":
            self.skip_w2003_reply = False
            self.skip_w2003_step = None

    def _set_request_mode_idle(self, context):
        if self.alg_executor is None:
            return
        message_name = context.get("message_name")
        if message_name == "StoreHouseStatusRequest":
            self.alg_executor.mode = "Wait"
        elif message_name == "StoreHouseStepCheckRequest":
            self.alg_executor.mode = "Polling"

    def _complete_request_context(self, context, ack_sent=False):
        context["ack_sent"] = context.get("ack_sent", False) or ack_sent
        context["completed"] = True
        self._clear_recovery_reply_state(context)
        self._set_request_mode_idle(context)
        self._remove_request_context(context)

    def _safe_log_request_error(self, message):
        try:
            self.influxdb_obj.write_log_influxdb("ERROR", message, self.sbid)
        except Exception:
            pass
        try:
            self.logging.error(message)
        except Exception:
            pass

    def _write_meas_session_index(self, device_info):
        """Write the accepted W2002 pallet mapping and its session start time."""
        try:
            if device_info is None or device_info.empty:
                return

            session_index = device_info.rename(
                columns={
                    "QRCodeID": "QRCode",
                    "DRCID": "ProtectBoardID",
                    "Position": "PositionID",
                }
            ).copy()
            # SQL datetime2 has no timezone information; store UTC to match InfluxDB.
            session_index["StartTime"] = datetime.now(timezone.utc).replace(tzinfo=None)
            session_index["StoreHouseID"] = session_index["StoreHouseID"].astype(int)
            session_index["PositionID"] = session_index["PositionID"].astype(int)
            session_index = session_index[
                [
                    "QRCode",
                    "StartTime",
                    "SerialBoardID",
                    "ProtectBoardID",
                    "StoreHouseID",
                    "PalletPosition",
                    "PositionID",
                    "PalletID",
                ]
            ]
            self.mssql_obj.write_db_pd(session_index, "MeasSessionIndex")
        except Exception:
            self._safe_log_request_error(
                f"write_meas_session_index failed [{self.sbid}]\n"
                f"{self.logger.get_slim_error_log()}"
            )
            raise

    def _close_failed_task(self, error):
        """ACK the failure, close the task, and stop this FC service."""
        # A validation failure must still be reported even when the request
        # was replayed with ``skip_reply`` during recovery.  The skip flag is
        # only for a successful internal replay; it must not hide an NG ACK.
        self._push_failure_ack(error, force=True)
        self._safe_log_request_error(str(error))
        self.running = False
        try:
            self.mssql_obj.clear_task_table(
                is_all_clear=False,
                sb_id=self.sbid,
            )
        except Exception as clear_error:
            self._safe_log_request_error(
                f"Failed to close task table [{self.sbid}]: {clear_error}"
            )
        self.skip_w2002_reply = False
        self.skip_w2003_reply = False
        self.skip_w2003_step = None

        for connection_name in ("conn_obj", "server_socket"):
            connection = getattr(self, connection_name, None)
            try:
                if connection:
                    connection.close()
            except Exception:
                pass

    @staticmethod
    def _has_recovery_payload(value):
        """Return whether a persisted recovery field contains usable data."""
        if value is None:
            return False
        try:
            if pd.isna(value):
                return False
        except (TypeError, ValueError):
            return False
        return bool(str(value).strip())

    def _recover_task_state(self, result_df):
        """Replay persisted requests after a recoverable ESP reconnect.

        A Task row is not, by itself, a parsing failure.  Invalid XML or
        protection parameters are handled by ``_close_failed_task`` when the
        request is consumed.  If the ESP disconnects afterwards, the active
        Task row is the checkpoint used to replay the last W2002/W2003 request.
        """
        if result_df is None or result_df.empty:
            return False

        task_row = result_df.iloc[0]
        pallet_info = task_row.get("pallet_info")
        step_info = task_row.get("step_info")
        task_step = task_row.get("task_step")
        has_pallet_info = self._has_recovery_payload(pallet_info)
        has_step_info = self._has_recovery_payload(step_info)

        if not has_pallet_info and not has_step_info:
            raise ValueError(
                f"Active task has no persisted recovery payload [{self.sbid}]"
            )

        recovery_step = None
        if has_step_info:
            try:
                recovery_step = int(task_step)
            except (TypeError, ValueError) as error:
                raise ValueError(
                    f"Invalid persisted task_step for recovery "
                    f"[{self.sbid}]: {task_step}"
                ) from error

        if has_pallet_info:
            self.redis_obj.rpush(self.sbid, str(pallet_info))
            self.skip_w2002_reply = True
            self.logging.debug(
                f"FC Task State Recovery [W2002] [{self.sbid}]"
            )
            self.influxdb_obj.write_log_influxdb(
                "INFO",
                f"FC Task State Recovery [W2002] [{self.sbid}]",
                self.sbid,
            )

        if has_step_info:
            self.redis_obj.rpush(self.sbid, str(step_info))
            self.skip_w2003_reply = True
            self.skip_w2003_step = recovery_step
            self.logging.debug(
                f"FC Task State Recovery [W2003-{recovery_step}] "
                f"[{self.sbid}]"
            )
            self.influxdb_obj.write_log_influxdb(
                "INFO",
                f"FC Task State Recovery [W2003-{recovery_step}] "
                f"[{self.sbid}]",
                self.sbid,
            )

        return has_pallet_info or has_step_info

    def _push_failure_ack(self, error, context=None, force=False):
        # Push exactly one failure ACK for the current upstream request.
        if context is None:
            context = self.active_request_context
        if context is None and self.pending_ack_queue:
            context = self.pending_ack_queue[0]
        if (
            context is None
            or context.get("completed")
            or context.get("ack_sent")
        ):
            return False

        # Recovery messages are internal replays and must not create a second
        # reply to an already completed upstream request.
        if context.get("skip_reply") and not force:
            self._complete_request_context(context)
            return False

        try:
            response_data = copy.deepcopy(context.get("response_data"))
            if response_data is None:
                response_data = balps.meas_map_mgr.TaskInfo()

            message_name = context.get("message_name", "")
            if message_name:
                response_data.set("MessageName", message_name)
            if not self._get_task_info_value(response_data, "SerialBoardID"):
                response_data.set("SerialBoardID", self.sbid)
            if not self._get_task_info_value(response_data, "StoreHouseID"):
                response_data.set("StoreHouseID", self.shid)
            if (
                not self._get_task_info_value(response_data, "ProtectBoardID")
                and self.pbid
            ):
                response_data.set("ProtectBoardID", self.pbid)

            error_text = " ".join(str(error).split())
            return_message = (
                type(error).__name__ + ": " + error_text
            )[:512]
            response_data.set("ReturnCode", "1")
            response_data.set("ReturnMessage", return_message)
            payload = json.dumps(
                response_data.get_data(),
                ensure_ascii=False,
            ).encode("utf-8")
            self.redis_obj.rpush("ACK", payload)
            self._complete_request_context(context, ack_sent=True)
            return True
        except Exception as ack_error:
            self._safe_log_request_error(str(ack_error))
            return False

    def create_db_conn(self):
        """
        Create db connection (influx db and mssql)

        Args:
            None

        Returns:
            None

        Example:
            >>> fc_obj = FC_api()
            >>> fc_obj.create_db_conn()
        """
        try:
            self.mssql_obj.create_db_connect_pd()
            self.influxdb_obj.create_connect()
        except Exception:
            self.logging.error(f"create_db_conn failed\n{self.logger.get_slim_error_log()}")

    def close_db_conn(self):
        """
        Close db connection (influx db and mssql)

        Args:
            None

        Returns:
            None

        Example:
            >>> fc_obj = FC_api()
            >>> fc_obj.close_db_conn()
        """
        try:
            self.mssql_obj.close_db_connect_pd()
            self.influxdb_obj.close_connect()
        except Exception:
            self.logging.error(f"close_db_conn failed\n{self.logger.get_slim_error_log()}")

    def process_xml_request(self):
        """
        Parse xml request
        - Measurement map table
        - Config map table

        Args:
            None
        Returns:
            None

        Example:
            >>> fc_obj = FC_api()
            >>> fc_obj.process_xml_request()
        """
        request_context = None
        try:
            # Get the xml data from the redis queue
            data = self.redis_obj.lpop(self.sbid)
            if data:
                request_context = self._create_request_context(data)
                self.active_request_context = request_context
                xml_string = (
                    data.decode("utf-8")
                    if isinstance(data, bytes)
                    else str(data)
                )
                request_context["xml_string"] = xml_string
                validated_request = self._validate_xml_request(
                    xml_string,
                    request_context,
                )
                root = validated_request["root"]
                message_name = validated_request["message_name"]
                # Process w2002 message
                if message_name == "StoreHouseStatusRequest":
                    # All XML/config validation has completed before mutation.
                    device_info = validated_request["device_info"]
                    self.config_info = validated_request["config_info"]

                    self.meas_map_obj.delete_storehouse_entry(self.shid)
                    self.meas_map_obj.add_storehouse_entry(self.shid, device_info)
                    # Update the configuration information
                    self.meas_map_obj.delete_config_entry(self.sbid)
                    self.meas_map_obj.add_config_entry(self.sbid, self.config_info)
                    # Set operation mode
                    self.alg_executor.set_configuration(self.meas_map_obj, "Config")
                    # Update the task information
                    self.meas_map_obj.update_task_info(xml_string)
                    self._write_meas_session_index(device_info)
                    # Save the request context before the cache is changed by next request.
                    response_data = self.meas_map_obj.get_task_info()
                    request_context["response_data"] = response_data
                    request_context["task_info_updated"] = True
                    request_context["skip_reply"] = self.skip_w2002_reply
                    self.pending_ack_queue.append(request_context)
                    self.active_request_context = None
                    self.influxdb_obj.write_log_influxdb("DEBUG", f"[CCMS2Task] [W2002] [{self.sbid}] [{self.pbid}]", self.sbid)
                    self.influxdb_obj.write_ccs_logs_influxdb("DEBUG", f"[CCMS2Task] [W2002] [{self.sbid}] [{self.pbid}]", self.sbid)
                    self.logging.debug(f"[CCMS2Task] [W2002] [{self.sbid}] [{self.pbid}]")
                elif message_name == "StoreHouseStepCheckRequest":
                    # Update the task information
                    self.meas_map_obj.update_task_info(xml_string)
                    # Set operation mode
                    self.alg_executor.set_configuration(self.meas_map_obj, "Check")
                    step = validated_request["step"]
                    # Save the request context before the cache is changed.
                    response_data = self.meas_map_obj.get_task_info()
                    request_context["step"] = step
                    request_context["response_data"] = response_data
                    request_context["task_info_updated"] = True
                    request_context["skip_reply"] = (
                        self.skip_w2003_reply
                        and step == self.skip_w2003_step
                    )
                    self.pending_ack_queue.append(request_context)
                    self.active_request_context = None
                    self.influxdb_obj.write_log_influxdb("DEBUG", f"[CCMS2Task] [W2003-{root.find("./BODY/STEP").text}] [{self.sbid}] [{self.pbid}]", self.sbid)
                    self.logging.debug(f"[CCMS2Task] [W2003-{root.find("./BODY/STEP").text}] [{self.sbid}] [{self.pbid}]")
                elif message_name == "Disconnect":
                    self.conn_obj.close()
                    self._complete_request_context(request_context)
                else:
                    raise ValueError(
                        "Unsupported message name: "
                        + (message_name or "<empty>")
                    )

        except Exception as error:
            if request_context is not None:
                # Invalid XML/protection parameters cannot become valid by
                # reconnecting with the same persisted request. ACK and close
                # the task immediately instead of waiting for recovery query.
                self._close_failed_task(error)
            else:
                self._safe_log_request_error(
                    "process_xml_request failed ["
                    + str(self.sbid)
                    + "]\n"
                    + self.logger.get_slim_error_log()
                )
            raise

    def esp_heartbeat_check(self):
        self.heartbeat_current = time.time()
        heartbeat_timeout = self.standby_heartbeat_timeout
        if not self.is_first_round:
            heartbeat_timeout = self.mpd_heartbeat_timeout
        if self.heartbeat_current - self.heartbeat_last >= heartbeat_timeout:
            self.influxdb_obj.write_log_influxdb("ERROR", f"CCMS2MPD heartbeat timeout [Waiting for MPD Device Connection] [{self.sbid}] [{self.port}] [{self.heartbeat_current - self.heartbeat_last}]", self.sbid)
            self.influxdb_obj.write_ccs_logs_influxdb("ERROR", f"CCMS2MPD heartbeat timeout [Waiting for MPD Device Connection] [{self.sbid}] [{self.port}] [{self.heartbeat_current - self.heartbeat_last}]", self.sbid)
            self.logging.error(f"CCMS2MPD heartbeat timeout [Waiting for MPD Device Connection] [{self.sbid}] [{self.port}] [{self.heartbeat_current - self.heartbeat_last}]")
            self.mssql_obj.clear_task_table(is_all_clear=False, sb_id=self.sbid)

    def init_algorithm(self, meas_id):
        """
        Initilze measurement method

        Args:
            meas_id: Measurement table index (first meas is 0 for FlowControl.json)
        Returns:
            None

        Example:
            >>> fc_obj = FC_api()
            >>> fc_obj.init_algorithm(meas_id)
        """
        try:
            self.heartbeat_last = time.time()
            # Create the socket to receive data from Mesaurement Device
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            self.server_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
            self.server_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
            self.server_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
            self.server_socket.bind((self.ip, self.port))
            self.server_socket.listen()
            self.logging.debug(f"FC Task Listening on {self.ip}:{self.port} [{self.sbid}]")
            self.influxdb_obj.write_log_influxdb("INFO", f"FC Task Listening on {self.ip}:{self.port} [{self.sbid}]", self.sbid)
            self.influxdb_obj.write_ccs_logs_influxdb("INFO", f"FC Task Listening on {self.ip}:{self.port} [{self.sbid}]", self.sbid)
            self.server_socket.settimeout(3.0)
            while True:
                try:
                    self.conn_obj, self.addr = self.server_socket.accept()
                    self.logging.debug(f"ESP Device Connected ({self.ip}:{self.port}) [{self.sbid}]")
                    self.influxdb_obj.write_log_influxdb("INFO", f"ESP Device Connected ({self.ip}:{self.port}) [{self.sbid}]", self.sbid)
                    if not self.is_first_round:
                        query = f"""
                            SELECT pallet_info, step_info, task_step
                            FROM BALPS.dbo.Task
                            WHERE task_name = '{self.sbid}'
                            AND is_delete = 0
                        """
                        result_df = self.mssql_obj.query_db_pd(query)

                        if not result_df.empty:
                            try:
                                self._recover_task_state(result_df)
                            except ValueError as error:
                                # A corrupt persisted checkpoint cannot be
                                # replayed safely.  This is different from a
                                # normal ESP disconnect, so close only this
                                # invalid recovery state.
                                self._close_failed_task(error)
                                return

                    self.is_first_round=False
                    self.influxdb_obj.write_log_influxdb("INFO", f"Connection ESP Device Ready [{self.sbid}]", self.sbid)
                    if self.alg_executor is None:
                        self.alg_executor = meas.meas_api(
                            self.config_data["meas"][meas_id]["variable"], self.config_data["meas"][meas_id]["db_out"], self.influxdb_obj, self.mssql_obj, self.sbid, self.shid
                        )
                    self.alg_executor.set_heartbeat_timeout()
                    break
                except TimeoutError:
                    pass
                self.esp_heartbeat_check()
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"init_algorithm failed [{self.sbid}]\n{self.logger.get_slim_error_log()}", self.sbid)

    def run_algorithm(self):
        """
        Run measurement method

        Args:
            None
        Returns:
            None

        Example:
            >>> fc_obj = FC_api()
            >>> fc_obj.run_algorithm()
        """
        request_context = (
            self.pending_ack_queue[0]
            if self.pending_ack_queue
            else None
        )
        try:
            # Process the measurement method
            res_result, self.meas_out_pd = self.alg_executor.run_meas(self.conn_obj)
            if res_result:
                self.influxdb_obj.write_json_influxdb("INFO", "[RspCCS]\n" + self.influxdb_obj.reformat_json_horizontal(res_result), "request", self.sbid, "RspCCS")
                pd_id = res_result.get("ProtectBoardID", {}).get("0")
                if pd_id is not None:  # and pd_id != "":
                    self.meas_map_obj.update_task_info_pbid(pd_id)
                # self.influxdb_obj.write_json_influxdb("INFO", "Rsp: " + json.dumps(res_result, indent=4), "request", self.sbid)
            if self.meas_out_pd is not None and not self.meas_out_pd.empty:
                self.influxdb_obj.write_json_influxdb("INFO", "[WrDB]\n" + self.influxdb_obj.format_df_mixed(self.meas_out_pd), "db", self.sbid, "WrDB")
                # self.influxdb_obj.write_json_influxdb("INFO", "WriteDB: " + json.dumps(self.meas_out_pd.astype(str).to_dict(orient='records'), indent=4, ensure_ascii=False), "db", self.sbid)

            if res_result:  # and self.alarm_sender == False:
                # Get current task information
                response_data = self.meas_map_obj.get_task_info()
                response_data.set("ReturnCode", "0" if not res_result.get("ReturnCode", {}) else res_result.get("ReturnCode", {}).get("0", 0))
                # response_data["ReturnCode"] = "0" if not res_result.get("ReturnCode", {}) else res_result.get("ReturnCode")
                response_data.set(
                    "PalletID",
                    res_result.get("PalletID", {}).get("0")
                    or response_data.get("PalletID"),
                )
                # response_data["PalletID"] = res_result.get("PalletID", {}) if res_result.get("PalletID", {}).get("0") else response_data.get("PalletID")
                if res_result.get("DataType", {}).get("0") == "ALARM":
                    # Setup the response data
                    response_data.set("MessageName", "AlarmStopReport")
                    # response_data["MessageName"]["0"] = "AlarmStopReport"
                    # Grafana UI show red
                    self.pallet_status = 2
                    self.alarm_sender = True
                    position_data = res_result.get("Position", {})
                    if position_data.get("0", 0):
                        for position_key, position_value in position_data.items():
                            response_data.set("Position", position_value, position_key)
                    qrcode_data = res_result.get("QRCodeID", {})
                    if qrcode_data.get("0", 0):
                        for qrcode_key, qrcode_value in qrcode_data.items():
                            response_data.set("QRCodeID", qrcode_value, qrcode_key)
                    # response_data["Position"] = res_result.get("Position", {}) if res_result.get("Position", {}).get("0") else response_data.get("Position")
                    # response_data["QRCodeID"] = res_result.get("QRCodeID", {}) if res_result.get("QRCodeID", {}).get("0") else response_data.get("QRCodeID")
                    self.redis_obj.rpush(
                        "ACK",
                        json.dumps(response_data.get_data()).encode("utf-8"),
                    )
                elif res_result.get("DataType", {}).get("0") == "ACK":
                    if not self.pending_ack_queue:
                        return
                    pending = self.pending_ack_queue[0]
                    request_context = pending
                    response_data = pending["response_data"]
                    message_name = pending["message_name"]
                    response_step = pending["step"]
                    should_skip = pending["skip_reply"]

                    # Update return code from actual device ACK.
                    response_data.set("ReturnCode", "0" if not res_result.get("ReturnCode", {}) else res_result.get("ReturnCode", {}).get("0", 0))
                    response_data.set(
                        "PalletID",
                        res_result.get("PalletID", {}).get("0")
                        or response_data.get("PalletID"),
                    )
                    # response_data["ReturnCode"] = (
                    #     "0"
                    #     if not res_result.get("ReturnCode", {})
                    #     else res_result.get("ReturnCode")
                    # )

                    # response_data["PalletID"] = (
                    #     res_result.get("PalletID", {})
                    #     if res_result.get("PalletID", {}).get("0")
                    #     else response_data.get("PalletID")
                    # )

                    if should_skip:
                        self._complete_request_context(pending)
                        self.logging.debug(
                            f"Skip recovery ACK "
                            f"[{self.message_mapping.get(message_name, message_name)}] "
                            f"[Step={response_step}] "
                            f"[{self.sbid}]"
                        )

                    else:
                        self.redis_obj.rpush(
                            "ACK",
                            json.dumps(response_data.get_data()).encode("utf-8"),
                        )
                        self._complete_request_context(
                            pending,
                            ack_sent=True,
                        )
                elif res_result.get("DataType", {}).get("0") == "DATA":
                    response_data.set("MessageName", "ESPRequest")
                    # response_data["MessageName"]["0"] = "ESPRequest"
                    self.redis_obj.rpush("ACK", json.dumps(response_data.get_data()).encode("utf-8"))
                # Send the response data to the redis queue
                # self.logging.debug(f"[RspCCS]\n" + self.influxdb_obj.reformat_json_horizontal(response_data))
        except Exception as error:
            # Keep the persisted Task alive here.  ``run_meas`` can fail
            # because the ESP socket temporarily disconnected; the next
            # connection will replay the checkpoint in ``init_algorithm``.
            # The current request still receives an NG ACK when possible, but
            # this path must not call ``_close_failed_task``.
            self._push_failure_ack(error, request_context)
            self._safe_log_request_error(
                "run_algorithm failed ["
                + str(self.sbid)
                + "]\n"
                + self.logger.get_slim_error_log()
            )
            raise

    def write_data(self, meas_id):
        """
        Write measurement to db

        Args:
            meas_id: Measurement table index (first meas is 0 for FlowControl.json)

        Returns:
            None

        Example:
            >>> fc_obj = FC_api()
            >>> fc_obj.write_data(meas_id)
        """
        try:
            if self.meas_out_pd is None:
                return
            
            current_time = time.time()
            if current_time - self.last_meas_write_time < self.meas_write_interval:
                return

            # Write the measurement data to influxdb
            for _ in range(len(self.config_data["meas"][meas_id]["db_out"])):
                self.influxdb_obj.write_pd_influxdb(self.meas_out_pd, "meas_data", ["pallet_position", "position_id", "return_code", "storehouse_id"])
            
            self.last_meas_write_time = time.time()
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"write_data failed [{self.sbid}]\n{self.logger.get_slim_error_log()}", self.sbid)

    def update_pallet_status(self):
        """
        Update Pallet Status

        Args:
            None

        Returns:
            None

        Example:
            >>> fc_obj = FC_api()
            >>> fc_obj.update_pallet_status()
        """
        try:
            # Update the pallet status
            self.mssql_obj.update_pallet_status(self.mssql_obj, self.shid, self.sbid, self.pallet_status)
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"update_pallet_status failed [{self.sbid}]\n{self.logger.get_slim_error_log()}", self.sbid)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Flow control.", usage="FlowControl.py -cfg <config> -tid <task_id> -ep <eport> -sip <ser_ip>")

    # Input Argument Setting
    parser.add_argument("-cfg", "--config", required=True, help="FC configuration e.g. config.json")
    parser.add_argument("-shid", "--storehouse_id", type=int, required=True, help="task id e.g. 1")
    parser.add_argument("-sbid", "--serialboard_id", type=str, required=True, help="task id e.g. 1")
    parser.add_argument("-pbid", "--protectboard_id", type=str, required=True, help="task id e.g. 1")
    parser.add_argument("-ep", "--eport", type=int, required=True, help="ESP Recv port e.g. 20001")
    parser.add_argument("-sip", "--ser_ip", type=str, required=True, help="Server Recv IP e.g. 15.1.1.10")
    try:
        # Parse input parameters
        args = parser.parse_args()
        # Flow control initialization
        obj_exec = FC_api(args)
        logger = obj_exec.get_logger()
        logging = obj_exec.get_logging()
        obj_exec.create_db_conn()
        last_pallet_update_time = datetime.now()
        
        while obj_exec.running:
            # initialize the algorithm for measurement method
            obj_exec.init_algorithm(0)
            if not obj_exec.running:
                break
            try:
                while obj_exec.running:
                    # Get the xml request
                    obj_exec.process_xml_request()
                    # Run the measurement method
                    obj_exec.run_algorithm()
                    # Write the measurement data to db
                    obj_exec.write_data(0)

                    # Update the pallet status
                    if datetime.now() - last_pallet_update_time >= timedelta(seconds=3):
                        obj_exec.update_pallet_status()
                        last_pallet_update_time = datetime.now()
            except Exception:
                logging.error(f"Main function failed\n{logger.get_slim_error_log()}")
                if obj_exec.running:
                    time.sleep(5)

        obj_exec.close_db_conn()

    except Exception:
        logging.error(f"Main function failed\n{logger.get_slim_error_log()}")
        sys.exit(1)
    except SystemExit:
        parser.print_help()
