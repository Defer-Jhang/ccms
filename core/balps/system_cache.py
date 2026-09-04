import json
import time
import os
import sys
from typing import TypedDict
import xml.etree.ElementTree as ET  # noqa: S405
import xml.dom.minidom as minidom  # noqa: S408
import datetime
import random
import copy

# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from .log_mgr import logging_api


class RackFields(TypedDict, total=False):
    palletL: str | None  # JSON string
    palletR: str | None  # JSON string
    timeL: float | None  # time.time()
    timeR: float | None  # time.time()


class RackInfo(TypedDict, total=False):
    palletL: str | None
    serialboardL: str | None
    protectboardL: str | None
    raw_dataL: str | None
    statusL: int | None
    stepL: int | None
    timeL: float | None
    palletR: str | None
    serialboardR: str | None
    protectboardR: str | None
    raw_dataR: str | None
    statusR: int | None
    stepR: int | None
    timeR: float | None
    stopstep: int | None


class msg_cache:
    """Maintain rack cache structure: rack_id > operation > fields"""

    def __init__(self, influxdb_obj):
        try:
            # Example: cache[rack_id][operation] = {palletL, palletR, timeL, timeR}
            self.cache: dict[int, dict[str, RackFields]] = {}
            self.rackinfo: dict[int, RackFields] = {}
            self.logger = logging_api(filename=os.path.join("logs", f"{os.path.splitext(os.path.basename(sys.argv[0]))[0]}"), backup_count=7)
            self.logging = self.logger.get_logger()
            self.influxdb_obj = influxdb_obj
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"init failed [msg_cache]\n{self.logger.get_slim_error_log()}", "system")

    def update_cache(self, rack_id: str, operation: str, pallet: str, data: str, update_time: float | None = None) -> bool:
        """
        Update cache with ESP data (JSON string from palletL or palletR).

        Args:
            rack_id: Rack ID
            operation: Operation (e.g. W1001, W2002)
            pallet: Pallet side ("L" or "R")
            data: JSON string data
            update_time: Timestamp (time.time())

        Returns:
            True when the cache entry is updated; otherwise False.
        """
        try:
            if rack_id not in self.cache:
                self.cache[rack_id] = {}
            if operation not in self.cache[rack_id]:
                self.cache[rack_id][operation] = {"palletL": None, "palletR": None, "timeL": None, "timeR": None}

            if pallet == "L":
                self.cache[rack_id][operation]["palletL"] = data
                self.cache[rack_id][operation]["timeL"] = update_time
            elif pallet == "R":
                self.cache[rack_id][operation]["palletR"] = data
                self.cache[rack_id][operation]["timeR"] = update_time
            else:
                raise ValueError(f"Invalid pallet position: {pallet}")
            return True
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"update_cache failed [msg_cache]\n{self.logger.get_slim_error_log()}", "system")
            return False

    def build_pallet_json(self, return_code: int) -> str:
        """
        Create No Response Json for missing pallet data.

        Args:
            return_code: return code number

        Returns:
            None
        """
        return json.dumps({"MessageName": {"0": "NoResponse"}, "ReturnCode": {"0": return_code}})

    def get_ready_operations_cache(self, response_wait_period: float = 5.0, response_alarm_wait_period: float = 2, validity_period: float = 10.0) -> list[tuple[int, str, RackFields]] | list:
        """
        Return one ready rack operation at a time in FIFO order.
        Rules:
        1. Return immediately when both pallet responses are available and valid.
        2. When only one pallet responds, wait for a short synchronization period.
        3. After the synchronization period, mark the missing pallet as NoResponse.
        4. Replace stale pallet data with NoResponse.

        Args:
            response_wait_period: Maximum time to wait for the missing pallet.
            response_alarm_wait_period: Time to wait before triggering an alarm for a missing pallet.
            validity_period: Maximum valid age of pallet data.

        Returns:
            A list containing one ready rack operation, or an empty list.
        """
        try:
            now = time.time()
            for rack_id, operations in self.cache.items():
                for operation, fields in operations.items():
                    # W3001 only stores the latest pallet status.
                    # It must not be sent directly to the upper layer.
                    if operation == "W3001":
                        continue

                    pallet_left = fields.get("palletL")
                    pallet_right = fields.get("palletR")
                    time_left = fields.get("timeL")
                    time_right = fields.get("timeR")
                    message_name_l = json.loads(fields.get("palletL") or "{}").get("MessageName", {}).get("0", None)
                    message_name_r = json.loads(fields.get("palletR") or "{}").get("MessageName", {}).get("0", None)
                    left_ready = pallet_left is not None
                    right_ready = pallet_right is not None

                    left_stale = (
                        time_left is not None
                        and now - time_left >= validity_period
                    )

                    right_stale = (
                        time_right is not None
                        and now - time_right >= validity_period
                    )

                    if left_ready and left_stale:
                        fields["palletL"] = self.build_pallet_json(3)

                    if right_ready and right_stale:
                        fields["palletR"] = self.build_pallet_json(3)

                    if left_ready and right_ready:
                        return [(rack_id, operation, fields)]
                    
                    if left_ready and not right_ready:
                        if (
                            time_left is not None
                            and now - time_left >= response_wait_period
                            or operation == 'W2004'
                            or (operation == 'W1001' and now - time_left >= response_alarm_wait_period)
                        ):
                            self.influxdb_obj.write_log_influxdb("WARM", f"Left Ready [{message_name_l}] | Right Miss [{message_name_r}] | {rack_id}", "system")
                            fields["palletR"] = self.build_pallet_json(3)
                            return [(rack_id, operation, fields)]

                        continue

                    if right_ready and not left_ready:
                        if (
                            time_right is not None
                            and now - time_right >= response_wait_period
                            or operation == 'W2004'
                            or (operation == 'W1001' and now - time_right >= response_alarm_wait_period)
                        ):
                            self.influxdb_obj.write_log_influxdb("WARM", f"Right Ready [{message_name_r}] | Left Miss [{message_name_l}] | {rack_id}", "system")
                            print(f"""Right Ready {now - time_right}""")
                            fields["palletL"] = self.build_pallet_json(3)

                            return [(rack_id, operation, fields)]

                        continue

                    reference_times = [
                        timestamp
                        for timestamp in (time_left, time_right)
                        if timestamp is not None
                    ]

                    if (
                        reference_times
                        and now - min(reference_times) >= response_wait_period
                    ):
                        fields["palletL"] = self.build_pallet_json(3)
                        fields["palletR"] = self.build_pallet_json(3)
                        return [(rack_id, operation, fields)]

        except Exception:
            # Log any unexpected error to InfluxDB with slimmed error trace
            self.influxdb_obj.write_log_influxdb("ERROR", f"get_ready_operations_cache failed [msg_cache]\n{self.logger.get_slim_error_log()}", "system")
        return []

    def clear_operation_cache(self, rack_id: str, operation: str):
        """
        Clear operation cache after sending data upstream.

        Args:
            rack_id: Rack ID
            operation: Operation (e.g. W1001, W2002)

        Returns:
            None
        """
        try:
            if rack_id in self.cache and operation in self.cache[rack_id]:
                # self.cache[rack_id][operation] = {"palletL": None, "palletR": None, "timeL": None, "timeR": None}
                del self.cache[rack_id][operation]
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"clear_operation_cache failed [msg_cache]\n{self.logger.get_slim_error_log()}", "system")

    def update_rack_info(self, rack_id: int, **kwargs) -> None:
        """
        Update fields of a specific rack.
        Only updates keys defined in RackInfo.

        Args:
            rack_id: Rack ID
            kwargs: Key-value pairs to update in RackInfo

        Returns:
            None
        """
        try:
            if rack_id not in self.rackinfo:
                self.rackinfo[rack_id] = {}

            for key, value in kwargs.items():
                if key in RackInfo.__annotations__:
                    self.rackinfo[rack_id][key] = value
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"update_rack_info failed [msg_cache]\n{self.logger.get_slim_error_log()}", "system")

    def get_rack_info(self, rack_id: int) -> RackInfo | None:
        """
        Get a deep copy of rack info by rack_id.
        Returns None if rack_id does not exist.

        Args:
            rack_id: Rack ID

        Returns:
            Rack info list
        """
        try:
            if rack_id not in self.rackinfo:
                return None
            return copy.deepcopy(self.rackinfo[rack_id])
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"get_rack_info failed [msg_cache]\n{self.logger.get_slim_error_log()}", "system")


class msg_aggeregation:
    """Aggregator that polls cache and decides when to send data back to central system."""

    def __init__(self, cache: msg_cache, influxdb_obj):
        try:
            self.cache = cache
            self.logger = cache.logger
            self.logging = cache.logging
            self.influxdb_obj = influxdb_obj
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"init failed [msg_aggeregation]\n{self.logger.get_slim_error_log()}", "system")

    def process_ready(self, meas_map_obj):
        """Check ready operations and prepare reply messages.

        Args:
            None

        Returns:
            Transaction id
        """
        try:
            request_xml = ""
            msg_name = ""
            rack_id = "0"
            ready_ops = self.cache.get_ready_operations_cache()

            for rack_id, operation, fields in ready_ops:
                if operation == "W1001":
                    msg_name, request_xml = self.create_request_w1001_xml(rack_id, fields)
                elif operation == "W2002":
                    msg_name, request_xml = self.create_reply_w2002_xml(rack_id, fields, meas_map_obj)
                elif operation == "W2003":
                    msg_name, request_xml = self.create_reply_w2003_xml(rack_id, fields, meas_map_obj)
                elif operation == "W2004":
                    msg_name, request_xml = self.create_reply_w2004_xml(rack_id, fields, meas_map_obj)
                elif operation == "W2005":
                    msg_name, request_xml = self.create_reply_w2005_xml(
                        rack_id,
                        fields,
                        meas_map_obj,
                    )
                if operation != "W3001":
                    self.cache.clear_operation_cache(rack_id, operation)
                else:
                    continue
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"process_ready failed [msg_aggeregation]\n{self.logger.get_slim_error_log()}", "system")
        return str(rack_id), msg_name, request_xml

    def generate_transaction_id(self):
        """
        Transaction id generation failed

        Args:
            None

        Returns:
            Transaction id
        """
        try:
            current_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")[:17]
            random_number = f"{random.randint(0, 999):03d}"  # noqa: S311
            transaction_id = current_time + random_number
            return transaction_id
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"generate_transaction_id failed [msg_aggeregation]\n{self.logger.get_slim_error_log()}", "system")

    def get_ng_status(self, retcodeL: int, retcodeR: int, retmsgL=None, retmsgR=None) -> tuple[int, str, str]:
        """Return NG status (code, text) based on retcodeL and retcodeR values.
        Priority order: Water > NG > NoResponse > Other > OK

        Args:
            retcodeL: Return code for left pallet
            retcodeR: Return code for right pallet

        Returns:
            Code number and status text
        """
        code_map = {
            0: "OK",
            1: "NG",
            2: "Water",
            3: "NoResponse",
        }

        # Define severity order (lower value = higher priority)
        severity = {
            2: 1,  # Water
            1: 2,  # NG
            3: 3,  # NoResponse
            -1: 4,  # Other (catch-all)
            0: 5,  # OK (lowest priority)
        }

        # Normalize both codes
        codes = []
        for c in (retcodeL, retcodeR):
            if c in code_map:
                codes.append(c)
            else:
                codes.append(-1)  # treat unknown as Other

        # Pick the one with highest severity (smallest severity value)
        best_code = min(codes, key=lambda c: severity[c])
        status = code_map.get(best_code, "Other")

        left_no_response = retcodeL == 3
        right_no_response = retcodeR == 3

        if left_no_response and right_no_response:
            msg = "Left and Right Pallet No Response"
        elif left_no_response:
            msg = "Left Pallet No Response"
        elif right_no_response:
            msg = "Right Pallet No Response"
        elif retmsgL or retmsgR:
            msg = retmsgL if retmsgL == retmsgR else " or ".join(
                msg for msg in [retmsgL, retmsgR] if msg
            )
        else:
            msg = ""

        return best_code, status, msg

    @staticmethod
    def get_request_value(
        pallet_l,
        pallet_r,
        fallback_task_info,
        key,
        default="",
        expected_message_name=None,
    ):
        """Get request metadata from the matching cached ACK or TaskInfo."""
        for pallet in (pallet_l, pallet_r):
            if not isinstance(pallet, dict):
                continue

            if expected_message_name:
                message_name = pallet.get("MessageName", {})
                if isinstance(message_name, dict):
                    message_name = message_name.get("0")
                if message_name != expected_message_name:
                    continue

            value = pallet.get(key, {})
            if isinstance(value, dict):
                value = value.get("0")
            if value not in (None, ""):
                return str(value)

        if fallback_task_info is not None:
            try:
                value = fallback_task_info.get(key)
                if isinstance(value, dict):
                    value = value.get("0")
                if value not in (None, ""):
                    return str(value)
            except Exception:
                pass

        return default

    def create_request_w1001_xml(self, rack_id, rsp: dict):
        """
        W1001 request handler - alarm report (Rack Level, merged L+R)

        Args:
            rack_id: Rack ID
            rsp: merged AlarmStopReport dict

        Returns:
            XML string
        """
        try:
            palletL = json.loads(rsp["palletL"])
            palletR = json.loads(rsp["palletR"])
            if palletL.get("MessageName", {}).get("0") == "ESPRequest" and palletR.get("MessageName", {}).get("0") == "ESPRequest":
                return "", ""

            root = ET.Element("MESSAGE")

            trid = self.generate_transaction_id()
            header = ET.SubElement(root, "HEADER")
            ET.SubElement(header, "MESSAGENAME").text = "AlarmStopReport"
            ET.SubElement(header, "TRANSACTIONID").text = trid
            ET.SubElement(header, "REPLYSUBJECTNAME").text = ""
            ET.SubElement(header, "INBOXNAME").text = ""
            ET.SubElement(header, "LISTENER").text = ""

            body = ET.SubElement(root, "BODY")
            ET.SubElement(body, "TRX_ID").text = trid

            retcodeL = int(palletL.get("ReturnCode", {}).get("0", 0))
            retcodeR = int(palletR.get("ReturnCode", {}).get("0", 0))
            retmsgL = palletL.get("ReturnMessage", {}).get("0", "")
            retmsgR = palletR.get("ReturnMessage", {}).get("0", "")
            code, status, msg = self.get_ng_status(
                retcodeL,
                retcodeR,
                retmsgL,
                retmsgR,
            )

            ET.SubElement(body, "NGStatus").text = status
            ET.SubElement(body, "StoreHouseID").text = str(rack_id)

            err_info_list = ET.SubElement(body, "ErrorInfoList")
            if palletL.get("MessageName", {}).get("0") == "AlarmStopReport":
                pallet = ET.SubElement(err_info_list, "Pallet")
                ET.SubElement(pallet, "PalletID").text = palletL.get("PalletID", {}).get("0", "")
                ET.SubElement(pallet, "SerialBoardID").text = palletL.get("SerialBoardID", {}).get("0", "")
                ET.SubElement(pallet, "DRCID").text = palletL.get("SerialBoardID", {}).get("0", "")

                qr_list = ET.SubElement(pallet, "QRCodeList")
                positions = palletL.get("Position", {})
                qrcodes = palletL.get("QRCodeID", {})

                for key, pos in positions.items():
                    qrcode = ET.SubElement(qr_list, "QRCode")
                    ET.SubElement(qrcode, "Position").text = str(pos)
                    ET.SubElement(qrcode, "QRCODEID").text = qrcodes.get(str(key), "")

            if palletR.get("MessageName", {}).get("0") == "AlarmStopReport":
                pallet = ET.SubElement(err_info_list, "Pallet")
                ET.SubElement(pallet, "PalletID").text = palletR.get("PalletID", {}).get("0", "")
                ET.SubElement(pallet, "SerialBoardID").text = palletR.get("SerialBoardID", {}).get("0", "")
                ET.SubElement(pallet, "DRCID").text = palletR.get("SerialBoardID", {}).get("0", "")

                qr_list = ET.SubElement(pallet, "QRCodeList")
                positions = palletR.get("Position", {})
                qrcodes = palletR.get("QRCodeID", {})

                for key, pos in positions.items():
                    qrcode = ET.SubElement(qr_list, "QRCode")
                    ET.SubElement(qrcode, "Position").text = str(pos)
                    ET.SubElement(qrcode, "QRCODEID").text = qrcodes.get(str(key), "")

            # RETURN
            ret = ET.SubElement(root, "RETURN")
            ET.SubElement(ret, "RETURNCODE").text = str(code)
            ET.SubElement(ret, "RETURNMESSAGE").text = msg

            return "AlarmStopReport", ET.tostring(root, encoding="utf-8", short_empty_elements=False)

        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"create_request_w1001_xml failed [msg_aggeregation]\n{self.logger.get_slim_error_log()}", "system")

    def create_reply_w2002_xml(self, rack_id, rsp: dict, meas_map_obj):
        """
        W2002 reply handler - Config storehouse

        Args:
            rack_id: Rack ID
            rsp: merged StoreHouseStatusReply dict

        Returns:
            XML string
        """
        try:
            palletL = json.loads(rsp["palletL"])
            palletR = json.loads(rsp["palletR"])

            root = ET.Element("MESSAGE")

            transactionid = self.get_request_value(
                palletL,
                palletR,
                meas_map_obj,
                "TransactionID",
                expected_message_name="StoreHouseStatusRequest",
            )
            trxid = self.get_request_value(
                palletL,
                palletR,
                meas_map_obj,
                "TrxID",
                expected_message_name="StoreHouseStatusRequest",
            )
            header = ET.SubElement(root, "HEADER")
            ET.SubElement(header, "MESSAGENAME").text = "StoreHouseStatusReply"
            ET.SubElement(header, "TRANSACTIONID").text = transactionid
            ET.SubElement(header, "REPLYSUBJECTNAME").text = ""
            ET.SubElement(header, "INBOXNAME").text = ""
            ET.SubElement(header, "LISTENER").text = ""

            body = ET.SubElement(root, "BODY")
            ET.SubElement(body, "TRX_ID").text = trxid
            ET.SubElement(body, "StoreHouseID").text = str(rack_id)

            retcodeL = int(palletL.get("ReturnCode", {}).get("0", 0))
            retcodeR = int(palletR.get("ReturnCode", {}).get("0", 0))
            retmsgL = palletL.get("ReturnMessage", {}).get("0", "")
            retmsgR = palletR.get("ReturnMessage", {}).get("0", "")
            code, status, msg = self.get_ng_status(retcodeL, retcodeR, retmsgL, retmsgR)

            ret = ET.SubElement(root, "RETURN")
            ET.SubElement(ret, "RETURNCODE").text = str(code)
            ET.SubElement(ret, "RETURNMESSAGE").text = msg

            return "StoreHouseStatusReply", ET.tostring(root, encoding="utf-8", short_empty_elements=False)
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"create_reply_w2002_xml failed [msg_aggeregation]\n{self.logger.get_slim_error_log()}", "system")
            raise

    def create_reply_w2003_xml(self, rack_id, rsp: dict, meas_map_obj):
        """
        W2003 reply handler - Switch storehouse step

        Args:
            rack_id: Rack ID
            rsp: merged StoreHouseStepCheckReply dict

        Returns:
            XML string
        """
        try:
            palletL = json.loads(rsp["palletL"])
            palletR = json.loads(rsp["palletR"])

            root = ET.Element("MESSAGE")

            transactionid = self.get_request_value(
                palletL,
                palletR,
                meas_map_obj,
                "TransactionID",
                expected_message_name="StoreHouseStepCheckRequest",
            )
            trxid = self.get_request_value(
                palletL,
                palletR,
                meas_map_obj,
                "TrxID",
                expected_message_name="StoreHouseStepCheckRequest",
            )
            header = ET.SubElement(root, "HEADER")
            ET.SubElement(header, "MESSAGENAME").text = "StoreHouseStepCheckReply"
            ET.SubElement(header, "TRANSACTIONID").text = transactionid
            ET.SubElement(header, "REPLYSUBJECTNAME").text = ""
            ET.SubElement(header, "INBOXNAME").text = ""
            ET.SubElement(header, "LISTENER").text = ""

            body = ET.SubElement(root, "BODY")
            ET.SubElement(body, "TRX_ID").text = trxid

            retcodeL = int(palletL.get("ReturnCode", {}).get("0", 0))
            retcodeR = int(palletR.get("ReturnCode", {}).get("0", 0))
            retmsgL = palletL.get("ReturnMessage", {}).get("0", "")
            retmsgR = palletR.get("ReturnMessage", {}).get("0", "")
            code, status, msg = self.get_ng_status(
                retcodeL,
                retcodeR,
                retmsgL,
                retmsgR,
            )

            ET.SubElement(body, "NGStatus").text = status
            ET.SubElement(body, "StoreHouseID").text = str(rack_id)

            error_info_list = ET.SubElement(body, "ErrorInfoList")
            if retcodeL in (1, 2):
                pallet = ET.SubElement(error_info_list, "Pallet")
                ET.SubElement(pallet, "PalletID").text = palletL.get("PalletID", {}).get("0", "")
                ET.SubElement(pallet, "SerialBoardID").text = palletL.get("SerialBoardID", {}).get("0", "")
                ET.SubElement(pallet, "DRCID").text = palletL.get("SerialBoardID", {}).get("0", "")

                qr_list = ET.SubElement(pallet, "QRCodeList")
                positions = palletL.get("Position", {})
                qrcodes = palletL.get("QRCodeID", {})
                for key, pos in positions.items():
                    qrcode = ET.SubElement(qr_list, "QRCode")
                    ET.SubElement(qrcode, "Position").text = str(pos)
                    ET.SubElement(qrcode, "QRCODEID").text = qrcodes.get(str(key), "")

            if retcodeR in (1, 2):
                pallet = ET.SubElement(error_info_list, "Pallet")
                ET.SubElement(pallet, "PalletID").text = palletR.get("PalletID", {}).get("0", "")
                ET.SubElement(pallet, "SerialBoardID").text = palletR.get("SerialBoardID", {}).get("0", "")
                ET.SubElement(pallet, "DRCID").text = palletR.get("SerialBoardID", {}).get("0", "")

                qr_list = ET.SubElement(pallet, "QRCodeList")
                positions = palletR.get("Position", {})
                qrcodes = palletR.get("QRCodeID", {})
                for key, pos in positions.items():
                    qrcode = ET.SubElement(qr_list, "QRCode")
                    ET.SubElement(qrcode, "Position").text = str(pos)
                    ET.SubElement(qrcode, "QRCODEID").text = qrcodes.get(str(key), "")

            ret = ET.SubElement(root, "RETURN")
            ET.SubElement(ret, "RETURNCODE").text = str(code)
            ET.SubElement(ret, "RETURNMESSAGE").text = msg

            return "StoreHouseStepCheckReply", ET.tostring(root, encoding="utf-8", short_empty_elements=False)
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"create_reply_w2003_xml failed [msg_aggeregation]\n{self.logger.get_slim_error_log()}", "system")
            raise

    def create_reply_w2004_xml(self, rack_id, rsp: dict, meas_map_obj):
        """
        W2004 reply handler - Get storehouse status

        Args:
            rack_id: Rack ID
            rsp: merged StoreHouseNGCheckReply dict

        Returns:
            XML string
        """
        try:
            palletL = json.loads(rsp["palletL"])
            palletR = json.loads(rsp["palletR"])

            root = ET.Element("MESSAGE")

            transactionid = self.get_request_value(
                palletL,
                palletR,
                meas_map_obj,
                "TransactionID",
                expected_message_name="StoreHouseNGCheckRequest",
            )
            trxid = self.get_request_value(
                palletL,
                palletR,
                meas_map_obj,
                "TrxID",
                expected_message_name="StoreHouseNGCheckRequest",
            )
            header = ET.SubElement(root, "HEADER")
            ET.SubElement(header, "MESSAGENAME").text = "StoreHouseNGCheckReply"
            ET.SubElement(header, "TRANSACTIONID").text = transactionid
            ET.SubElement(header, "REPLYSUBJECTNAME").text = ""
            ET.SubElement(header, "INBOXNAME").text = ""
            ET.SubElement(header, "LISTENER").text = ""

            body = ET.SubElement(root, "BODY")
            ET.SubElement(body, "TRX_ID").text = trxid

            retcodeL = int(palletL.get("ReturnCode", {}).get("0", 0))
            retcodeR = int(palletR.get("ReturnCode", {}).get("0", 0))
            retmsgL = palletL.get("ReturnMessage", {}).get("0", "")
            retmsgR = palletR.get("ReturnMessage", {}).get("0", "")
            code, status, msg = self.get_ng_status(
                retcodeL,
                retcodeR,
                retmsgL,
                retmsgR,
            )

            ET.SubElement(body, "NGStatus").text = status
            ET.SubElement(body, "StoreHouseID").text = str(rack_id)

            error_info_list = ET.SubElement(body, "ErrorInfoList")
            if retcodeL == 1 or retcodeL == 2:
                pallet = ET.SubElement(error_info_list, "Pallet")
                ET.SubElement(pallet, "PalletID").text = palletL.get("PalletID", {}).get("0", "")
                ET.SubElement(pallet, "SerialBoardID").text = palletL.get("SerialBoardID", {}).get("0", "")
                ET.SubElement(pallet, "DRCID").text = palletL.get("SerialBoardID", {}).get("0", "")

                qr_list = ET.SubElement(pallet, "QRCodeList")
                positions = palletL.get("Position", {})
                qrcodes = palletL.get("QRCodeID", {})
                for key, pos in positions.items():
                    qrcode = ET.SubElement(qr_list, "QRCode")
                    ET.SubElement(qrcode, "Position").text = str(pos)
                    ET.SubElement(qrcode, "QRCODEID").text = qrcodes.get(str(key), "")

            if retcodeR == 1 or retcodeR == 2:
                pallet = ET.SubElement(error_info_list, "Pallet")
                ET.SubElement(pallet, "PalletID").text = palletR.get("PalletID", {}).get("0", "")
                ET.SubElement(pallet, "SerialBoardID").text = palletR.get("SerialBoardID", {}).get("0", "")
                ET.SubElement(pallet, "DRCID").text = palletR.get("SerialBoardID", {}).get("0", "")

                qr_list = ET.SubElement(pallet, "QRCodeList")
                positions = palletR.get("Position", {})
                qrcodes = palletR.get("QRCodeID", {})
                for key, pos in positions.items():
                    qrcode = ET.SubElement(qr_list, "QRCode")
                    ET.SubElement(qrcode, "Position").text = str(pos)
                    ET.SubElement(qrcode, "QRCODEID").text = qrcodes.get(str(key), "")

            ret = ET.SubElement(root, "RETURN")
            ET.SubElement(ret, "RETURNCODE").text = str(code)
            ET.SubElement(ret, "RETURNMESSAGE").text = msg

            return "StoreHouseNGCheckReply", ET.tostring(root, encoding="utf-8", short_empty_elements=False)
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"create_reply_w2004_xml failed [msg_aggeregation]\n{self.logger.get_slim_error_log()}", "system")
            raise

    def create_reply_w2005_xml(self, rack_id, rsp: dict, meas_map_obj):
        """
        W2005 reply handler - Stop storehouse

        Args:
            rack_id: Rack ID
            rsp: merged JudgmentCompletionNotificationReply dict
            meas_map_obj: fallback TaskInfo for a normal completion reply

        Returns:
            XML string
        """
        try:
            palletL = json.loads(rsp["palletL"])
            palletR = json.loads(rsp["palletR"])

            root = ET.Element("MESSAGE")

            transactionid = self.get_request_value(
                palletL,
                palletR,
                meas_map_obj,
                "TransactionID",
                expected_message_name="JudgmentCompletionNotificationRequest",
            )
            trxid = self.get_request_value(
                palletL,
                palletR,
                meas_map_obj,
                "TrxID",
                expected_message_name="JudgmentCompletionNotificationRequest",
            )
            header = ET.SubElement(root, "HEADER")
            ET.SubElement(header, "MESSAGENAME").text = "JudgmentCompletionNotificationReply"
            ET.SubElement(header, "TRANSACTIONID").text = transactionid
            ET.SubElement(header, "REPLYSUBJECTNAME").text = ""
            ET.SubElement(header, "INBOXNAME").text = ""
            ET.SubElement(header, "LISTENER").text = ""

            body = ET.SubElement(root, "BODY")
            ET.SubElement(body, "TRX_ID").text = trxid
            ET.SubElement(body, "StoreHouseID").text = str(rack_id)

            request_name = "JudgmentCompletionNotificationRequest"
            cached_exception_acks = [
                pallet
                for pallet in (palletL, palletR)
                if pallet.get("MessageName", {}).get("0", "") == request_name
                and int(pallet.get("ReturnCode", {}).get("0", 0)) == 1
            ]

            if cached_exception_acks:
                code = 1
                messages = [
                    pallet.get("ReturnMessage", {}).get("0", "")
                    for pallet in cached_exception_acks
                ]
                messages = [message for message in messages if message]
                return_message = (
                    messages[0]
                    if messages and all(message == messages[0] for message in messages)
                    else " or ".join(messages)
                )
            else:
                code = 0
                return_message = ""

            ret = ET.SubElement(root, "RETURN")
            ET.SubElement(ret, "RETURNCODE").text = str(code)
            ET.SubElement(ret, "RETURNMESSAGE").text = return_message

            return "JudgmentCompletionNotificationReply", ET.tostring(root, encoding="utf-8", short_empty_elements=False)
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"create_reply_w2005_xml failed [msg_aggeregation]\n{self.logger.get_slim_error_log()}", "system")
            raise


class cache_api:
    """Main system loop: receive data from Redis, update cache, run aggregator."""

    def __init__(self, influxdb_obj):
        try:
            self.influxdb_obj = influxdb_obj
            self.cache = msg_cache(influxdb_obj)
            self.aggregator = msg_aggeregation(self.cache, influxdb_obj)
            self.logger = self.cache.logger
            self.logging = self.cache.logging
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"init failed [cache_api]\n{self.logger.get_slim_error_log()}", "system")

    def process_msg(self, meas_map_obj):
        """
        Aggregate message

        Args:
            None

        Returns:
            None
        """
        try:
            # Poll aggregator every cycle
            rack_id, msg_name, msg_xml = self.aggregator.process_ready(meas_map_obj)
            if msg_xml:
                return rack_id, msg_name, msg_xml
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"process_msg failed [cache_api]\n{self.logger.get_slim_error_log()}", "system")
        return None, None, None

    # def handle_esp_data(
    #     self,
    #     raw: str,
    #     rack_id: str | None = None,
    #     message_name: str | None = None,
    #     update_time: float | None = None,
    # ):
    #     try:
    #         timestamp = update_time if update_time is not None else time.time()

    #         if raw:
    #             data = json.loads(raw)

    #             rack_id = data.get("StoreHouseID", {}).get("0", "")
    #             if not rack_id:
    #                 raise ValueError("Missing RackID")

    #             msg_name = message_name or data.get("MessageName", {}).get("0", "")
    #             if not msg_name:
    #                 raise ValueError(f"Missing Message Name | RID: {rack_id}")

    #             pallet = data.get("PalletPosition", {}).get("0", "")
    #             if pallet not in ("L", "R"):
    #                 raise ValueError(
    #                     f"Missing Pallet Position | RID: {rack_id} | "
    #                     f"MSGName: {msg_name}"
    #                 )

    #             operation = self.map_message_to_operation(msg_name)
    #             if not operation:
    #                 raise ValueError(
    #                     f"Missing operation | RID: {rack_id} | "
    #                     f"MSGName: {msg_name} | PalletPosition: {pallet}"
    #                 )

    #             rack_info = self.cache.get_rack_info(rack_id)
    #             current_info = rack_info.get(pallet) if rack_info else None

    #             current_msg_name = (
    #                 current_info.get("message_name")
    #                 if isinstance(current_info, dict)
    #                 else None
    #             )

    #             if current_msg_name == msg_name:
    #                 return

    #             self.cache.update_cache(
    #                 rack_id,
    #                 operation,
    #                 pallet,
    #                 raw,
    #                 timestamp,
    #             )

    #         else:
    #             if not rack_id:
    #                 raise ValueError("Missing RackID")

    #             msg_name = message_name
    #             if not msg_name:
    #                 raise ValueError(f"Missing Message Name | RID: {rack_id}")

    #             operation = self.map_message_to_operation(msg_name)
    #             if not operation:
    #                 raise ValueError(
    #                     f"Missing operation | RID: {rack_id} | "
    #                     f"MSGName: {msg_name}"
    #                 )

    #             self.cache.update_cache(
    #                 rack_id, operation, "L", raw, timestamp
    #             )
    #             self.cache.update_cache(
    #                 rack_id, operation, "R", raw, timestamp
    #             )

    #     except ValueError as ex:
    #         self.influxdb_obj.write_log_influxdb(
    #             "ERROR",
    #             f"handle_esp_data failed [cache_api]: {ex}",
    #             "system",
    #         )
    #     except Exception:
    #         self.influxdb_obj.write_log_influxdb(
    #             "ERROR",
    #             "handle_esp_data failed [cache_api]\n"
    #             f"{self.logger.get_slim_error_log()}",
    #             "system",
    #         )

    def handle_esp_data(self, raw: str | None, rack_id: str | None = None, message_name: str | None = None, update_time: float | None = None) -> bool:
        """
        Parse ESP JSON data and update cache.

        Args:
            raw: ESP response data
            rack_id: Rack ID
            message_name: Message name
            update_time: Timestamp

        Returns:
            True when all target cache entries are updated; otherwise False.
        """
        try:
            timestamp = update_time if update_time is not None else time.time()
            if raw:
                data = json.loads(raw)
                rack_id = data.get("StoreHouseID", {}).get("0", "")
                if not rack_id:
                    raise ValueError("Missing RackID")
                msg_name = message_name or data.get("MessageName", {}).get("0", "")
                if not msg_name:
                    raise ValueError(f"Missing Message Name | RID: {rack_id}")
                pallet = data.get("PalletPosition", {}).get("0", "")
                if not pallet:
                    raise ValueError(f"Missing Pallet Position | RID: {rack_id} | MSGName: {msg_name}")
                operation = self.map_message_to_operation(msg_name)
                if not operation:
                    raise ValueError(f"Missing operation | RID: {rack_id} | MSGName: {msg_name} | PalletPosition: {pallet}")
                if not self.cache.update_cache(
                    rack_id,
                    operation,
                    pallet,
                    raw,
                    timestamp,
                ):
                    raise RuntimeError(
                        f"Failed to update cache | RID: {rack_id} | "
                        f"Operation: {operation} | Pallet: {pallet}"
                    )
            else:
                operation = self.map_message_to_operation(message_name)

                if not rack_id:
                    raise ValueError("Missing RackID")
                msg_name = message_name
                if not msg_name:
                    raise ValueError(f"Missing Message Name | RID: {rack_id}")
                if not operation:
                    raise ValueError(f"Missing operation | RID: {rack_id} | MSGName: {msg_name}")
                updated_l = self.cache.update_cache(
                    rack_id,
                    operation,
                    "L",
                    raw,
                    timestamp,
                )
                updated_r = self.cache.update_cache(
                    rack_id,
                    operation,
                    "R",
                    raw,
                    timestamp,
                )
                if not (updated_l and updated_r):
                    raise RuntimeError(
                        f"Failed to update both cache entries | "
                        f"RID: {rack_id} | Operation: {operation}"
                    )
            return True
        except ValueError as ex:
            self.influxdb_obj.write_log_influxdb(
                "ERROR",
                f"handle_esp_data failed [cache_api]: {ex}",
                "system"
            )
            return False
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"handle_esp_data failed [cache_api]\n{self.logger.get_slim_error_log()}", "system")
            return False

    def map_message_to_operation(self, msg: str) -> str:
        """
        Map ESP message name to operation code (W2002/W2003/W2004/W2005/W1001).

        Args:
            msg: Message name

        Returns:
            None
        """
        try:
            mapping = {
                "AlarmStopReport": "W1001",
                "ESPRequest": "W3001",
                "StoreHouseStatusRequest": "W2002",
                "StoreHouseStepCheckRequest": "W2003",
                "StoreHouseNGCheckRequest": "W2004",
                "JudgmentCompletionNotificationRequest": "W2005",
            }
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"map_message_to_operation failed [cache_api]\n{self.logger.get_slim_error_log()}", "system")
        return mapping.get(msg, "")

    def update_rack_status(self, raw: str):
        """
        Parse ESP JSON data and update rack status.

        Args:
            raw: Pallet monitor data

        Returns:
            None
        """
        try:
            data = json.loads(raw)

            rack_id = int(data.get("StoreHouseID", {}).get("0", ""))
            pallet_id = data.get("PalletID", {}).get("0", "")
            serialboard_id = data.get("SerialBoardID", {}).get("0", "")
            protectboard_id = data.get("ProtectBoardID", {}).get("0", "")
            status = int(data.get("ReturnCode", {}).get("0", 0))
            step = int(data.get("Step", {}).get("0", 0))
            stopstep = int(data.get("StopStep", {}).get("0", 0))
            raw_data = raw
            now_time = time.time()

            if data.get("PalletPosition", {}).get("0", "") == "L":
                self.cache.update_rack_info(
                    rack_id,
                    palletL=pallet_id,
                    serialboardL=serialboard_id,
                    protectboardL=protectboard_id,
                    raw_dataL=raw_data,
                    statusL=status,
                    stepL=step,
                    timeL=now_time,
                    stopstep=stopstep,
                )
            elif data.get("PalletPosition", {}).get("0", "") == "R":
                self.cache.update_rack_info(
                    rack_id,
                    palletR=pallet_id,
                    serialboardR=serialboard_id,
                    protectboardR=protectboard_id,
                    raw_dataR=raw_data,
                    statusR=status,
                    stepR=step,
                    timeR=now_time,
                    stopstep=stopstep,
                )
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"update_rack_status failed [cache_api]\n{self.logger.get_slim_error_log()}", "system")

    def handle_rack_status(self, rack_id: int, timeout: int = 10) -> bool:
        """
        Map the latest W3001 rack status to the W2004 response cache.
        Missing or expired pallet status is mapped to NoResponse.

        Args:
            rack_id: Rack ID.
            timeout: Maximum valid age of W3001 status in seconds.

        Returns:
            True if the W2004 response cache is created; otherwise False.
        """
        try:
            rack_info = self.cache.get_rack_info(rack_id)
            current_time = time.time()
            operation = "W2004"
            no_response = self.cache.build_pallet_json(3)

            # No W3001 data exists for this rack.
            if not rack_info:
                updated_l = self.cache.update_cache(
                    rack_id=str(rack_id),
                    operation=operation,
                    pallet="L",
                    data=no_response,
                    update_time=current_time,
                )
                updated_r = self.cache.update_cache(
                    rack_id=str(rack_id),
                    operation=operation,
                    pallet="R",
                    data=no_response,
                    update_time=current_time,
                )
                if not (updated_l and updated_r):
                    raise RuntimeError(
                        f"Failed to create W2004 NoResponse cache | "
                        f"RID: {rack_id}"
                    )
                return True

            raw_data_l = rack_info.get("raw_dataL")
            raw_data_r = rack_info.get("raw_dataR")
            time_l = rack_info.get("timeL")
            time_r = rack_info.get("timeR")

            # W3001 left-pallet status is valid only when data and timestamp exist
            # and the status has not expired.
            left_valid = (
                raw_data_l is not None
                and time_l is not None
                and current_time - float(time_l) <= timeout
            )

            if left_valid:
                updated_l = self.cache.update_cache(
                    rack_id=str(rack_id),
                    operation=operation,
                    pallet="L",
                    data=raw_data_l,
                    update_time=float(time_l),
                )
            else:
                # Keep the original W3001 timestamp when available.
                # If W3001 never existed, use the W2004 request time.
                updated_l = self.cache.update_cache(
                    rack_id=str(rack_id),
                    operation=operation,
                    pallet="L",
                    data=no_response,
                    update_time=(
                        float(time_l)
                        if time_l is not None
                        else current_time
                    ),
                )

            if not updated_l:
                raise RuntimeError(
                    f"Failed to create W2004 left cache | RID: {rack_id}"
                )

            # W3001 right-pallet status is valid only when data and timestamp exist
            # and the status has not expired.
            right_valid = (
                raw_data_r is not None
                and time_r is not None
                and current_time - float(time_r) <= timeout
            )

            if right_valid:
                updated_r = self.cache.update_cache(
                    rack_id=str(rack_id),
                    operation=operation,
                    pallet="R",
                    data=raw_data_r,
                    update_time=float(time_r),
                )
            else:
                # Keep the original W3001 timestamp when available.
                # If W3001 never existed, use the W2004 request time.
                updated_r = self.cache.update_cache(
                    rack_id=str(rack_id),
                    operation=operation,
                    pallet="R",
                    data=no_response,
                    update_time=(
                        float(time_r)
                        if time_r is not None
                        else current_time
                    ),
                )

            if not updated_r:
                raise RuntimeError(
                    f"Failed to create W2004 right cache | RID: {rack_id}"
                )

            return True

        except Exception:
            self.influxdb_obj.write_log_influxdb(
                "ERROR",
                (
                    "handle_rack_status failed [cache_api]\n"
                    f"{self.logger.get_slim_error_log()}"
                ),
                "system",
            )
            return False
    
    def handle_rack_stop(self, rack_id) -> bool:
        """
        Stop Rack

        Args:
            rack_id: Rack ID

        Returns:
            True when the stop response cache is prepared; otherwise False.
        """
        try:
            rack_info = self.cache.get_rack_info(rack_id)
            if rack_info:
                updated_l = self.handle_esp_data(
                    raw=rack_info.get("raw_dataL"),
                    rack_id=str(rack_id),
                    message_name="JudgmentCompletionNotificationRequest",
                    update_time=time.time(),
                )
                updated_r = self.handle_esp_data(
                    raw=rack_info.get("raw_dataR"),
                    rack_id=str(rack_id),
                    message_name="JudgmentCompletionNotificationRequest",
                    update_time=time.time(),
                )
                if not (updated_l and updated_r):
                    raise RuntimeError(
                        f"Failed to prepare W2005 response cache | RID: {rack_id}"
                    )
            return True
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"handle_rack_stop failed [cache_api]\n{self.logger.get_slim_error_log()}", "system")
            return False

    def get_rack_info(self, rack_id: int) -> RackInfo | None:
        """
        Get rack info by rack_id.

        Args:
            rack_id: Rack ID

        Returns:
            None
        """
        try:
            return self.cache.get_rack_info(rack_id)
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"get_rack_info failed [cache_api]\n{self.logger.get_slim_error_log()}", "system")
        return None
