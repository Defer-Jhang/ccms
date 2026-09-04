import json

import re
from sqlalchemy import text as sql_text
import socket
import time
import xml.etree.ElementTree as ET  # noqa: S405
import xml.dom.minidom as minidom  # noqa: F401, S408
import random
import redis
import datetime
import struct
import os
import sys
from collections import defaultdict

"""
XML Socket Programming

"""


class message_api:
    def __init__(self, host, port, influxdb_obj, mssql_obj, task_mgr_obj):
        from .meas_map_mgr import meas_map_api
        from .sync_time import sync_time_api
        from .log_mgr import logging_api
        from .system_cache import cache_api

        """
        Init xml socket

        Args:
            host: Connect ip (e.g. '127.0.0.1')
            port: Connect port (e.g. 12345)
            influxdb_obj: Used to write system logs
            mssql_obj: Access mssql object
            task_mgr_obj: Task manager object

        Returns:
            None

        Example:
            >>> message_obj = message_api('127.0.0.1', 12345, influxdb_obj, mssql_obj, task_mgr_obj)
        """
        try:
            self.logger = logging_api(filename=os.path.join("logs", f"{os.path.splitext(os.path.basename(sys.argv[0]))[0]}"), backup_count=7)
            self.logging = self.logger.get_logger()
            self.influxdb_obj = influxdb_obj
            self.mssql_obj = mssql_obj
            self.task_mgr_obj = task_mgr_obj
            # Internal message exchange from message manager to flow control
            self.redis_obj = redis.StrictRedis("127.0.0.1", port=6379, db=0)
            # Clear all data in redis
            self.redis_obj.flushall()
            self.host = host
            self.port = port
            self.meas_map_obj = meas_map_api(influxdb_obj)
            self.sync_time_obj = sync_time_api(influxdb_obj)
            self.sh_ack = {}
            self.running = True
            self.config_info = None
            self.last_time_sync = None
            self.last_alive_check = None
            self.last_alarm_stop = None
            self.alarm_stop_shid = {}
            self.msg_cache = cache_api(self.influxdb_obj)
            self.last_time_perf = time.time()
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"Xml socket initialization failed\n{self.logger.get_slim_error_log()}", "system")

    def is_connected(self):
        """
        Check if the socket is connected
        """
        try:
            error = self.socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            self.socket.send(b"")
            return error == 0
        except Exception:
            self.logging.info(f"CCS not Ready | {self.host} | {self.port}")
            time.sleep(1)
            # self.influxdb_obj.("ERROR",f"is_connected failed\n{self.logger.get_slim_error_log()}", "system")
            return False

    def connect(self):
        """
        Connect message socket server

        Args:
            host: connect ip (e.g. '127.0.0.1')
            port: connect port (e.g. 12345)

        Returns:
            None

        Example:
            >>> message_obj = message_api("127.0.0.1", 12345)
            >>> message_obj.connect()
        """
        while not self.is_connected():
            try:
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.socket.setblocking(True)
                self.socket.connect((self.host, self.port))
                self.socket.setblocking(False)
                self.socket.send(b"")
                self.logging.info(f"CCS Connected | {self.host} | {self.port}")
            except BlockingIOError:
                time.sleep(1)
            except ConnectionRefusedError:
                time.sleep(1)
            except Exception:
                self.logging.error(f"connect failed\n{self.logger.get_slim_error_log()}")
                self.influxdb_obj.write_log_influxdb("ERROR", f"connect failed\n{self.logger.get_slim_error_log()}", "system")
                time.sleep(1)

    def generate_transaction_id(self):
        """
        Transaction id generation failed

        Args:
            None

        Returns:
            Transaction id

        Example:
            >>> message_obj = message_api("127.0.0.1", 12345)
            >>> tid = message_obj.generate_transaction_id()
        """
        try:
            current_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")[:17]
            random_number = f"{random.randint(0, 999):03d}"  # noqa: S311
            transaction_id = current_time + random_number
            return transaction_id
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"generate_transaction_id failed\n{self.logger.get_slim_error_log()}", "system")

    def create_request_w0001_xml(self, message_name):
        """
        W0001 request handler - time sync

        Args:
            message_name: Request name (e.g. 'syncTimeRequest')
            transaction_id: Transaction id (e.g. 20200402172959123456)

        Returns:
            None

        Example:
            >>> message_obj = message_api("127.0.0.1", 12345)
            >>> message_obj.connect()
            >>> message_obj.create_request_w0001_xml("SyncTimeRequest", message_obj.generate_transaction_id())
        """
        try:
            root = ET.Element("MESSAGE")
            header = ET.SubElement(root, "HEADER")
            ET.SubElement(header, "MESSAGENAME").text = message_name
            ET.SubElement(header, "TRANSACTIONID").text = self.generate_transaction_id()
            ET.SubElement(header, "REPLYSUBJECTNAME").text = ""
            ET.SubElement(header, "INBOXNAME").text = ""
            ET.SubElement(header, "LISTENER").text = ""

            body = ET.SubElement(root, "BODY")
            ET.SubElement(body, "TRX_ID").text = self.generate_transaction_id()
            ET.SubElement(body, "TIME").text = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            ET.SubElement(body, "LINE_ID").text = "SECCHARGE0100"

            ret = ET.SubElement(root, "RETURN")
            ET.SubElement(ret, "RETURNCODE").text = "0"
            ET.SubElement(ret, "RETURNMESSAGE").text = ""

            return ET.tostring(root, encoding="utf-8", short_empty_elements=False)
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"create_request_w0001_xml failed\n{self.logger.get_slim_error_log()}", "system")

    def create_request_w0002_xml(self, message_name):
        """
        W0002 request handler - ccms activity status

        Args:
            message_name: Request name (e.g. 'AliveRequest')
            transaction_id: Transaction id (e.g. 20200402172959123456)

        Returns:
            None

        Example:
            >>> message_obj = message_api("127.0.0.1", 12345)
            >>> message_obj.connect()
            >>> message_obj.create_request_w0002_xml("AliveRequest", message_obj.generate_transaction_id())
        """
        try:
            root = ET.Element("MESSAGE")
            header = ET.SubElement(root, "HEADER")
            ET.SubElement(header, "MESSAGENAME").text = message_name
            ET.SubElement(header, "TRANSACTIONID").text = self.generate_transaction_id()
            ET.SubElement(header, "REPLYSUBJECTNAME").text = ""
            ET.SubElement(header, "INBOXNAME").text = ""
            ET.SubElement(header, "LISTENER").text = ""

            body = ET.SubElement(root, "BODY")
            ET.SubElement(body, "TRX_ID").text = self.generate_transaction_id()
            ET.SubElement(body, "LINE_ID").text = "SECCHARGE0100"

            ret = ET.SubElement(root, "RETURN")
            ET.SubElement(ret, "RETURNCODE").text = "0"
            ET.SubElement(ret, "RETURNMESSAGE").text = ""

            return ET.tostring(root, encoding="utf-8", short_empty_elements=False)
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"create_request_w0002_xml failed\n{self.logger.get_slim_error_log()}", "system")

    def reformat_xml_grouped(self, xml_str, storehouse_id):
        try:
            """
            Reformat xml string with grouped key-value pairs for better readability

            Args:
                xml_str: The original XML string
                storehouse_id: Storehouse ID

            Returns:
                A reformatted string with grouped key-value pairs.
            """

            def extract_key_value_pairs(elem, result):
                for child in elem:
                    if list(child):
                        extract_key_value_pairs(child, result)
                    else:
                        if child.text and child.text.strip():
                            result[child.tag].append(child.text.strip())

            root = ET.fromstring(xml_str)  # noqa: S314
            result = defaultdict(list)

            extract_key_value_pairs(root, result)

            title = result.get("MESSAGENAME", ["[XMLSummary]"])[0]

            lines = [f"[{title}]"]
            lines.append(f"{'StoreHouseID':<20}: {storehouse_id}")
            for k, vlist in result.items():
                if k == "MESSAGENAME":
                    continue
                line = ", ".join(vlist)
                lines.append(f"{k:<20}: {line}")

            return "\n".join(lines)
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"reformat_xml_grouped failed\n{self.logger.get_slim_error_log()}", "system")

    def send_request(self, message_name, request_xml, sbid="A0001", shid="0"):
        """
        Send request XML message

        Args:
            message_name: Request name
            request_xml: Request XML
            sbid: Serialboard ID
            shid: Storehouse ID

        Returns:
            None

        Example:
            >>> message_obj = message_api("127.0.0.1", 12345)
            >>> message_obj.send_request(message_name, transaction_id, request_xml, sbid, shid)
        """
        try:
            if message_name == "SyncTimeRequest":
                request_xml = self.create_request_w0001_xml(message_name)
            elif message_name == "AliveRequest":
                request_xml = self.create_request_w0002_xml(message_name)
            # elif message_name == "StoreHouseStepCheckReply":
                # rack_info = self.msg_cache.get_rack_info(int(shid))
                # if rack_info.get("stepL", {}) == rack_info.get("stopstep", {}) and rack_info.get("stepR", {}) == rack_info.get("stopstep", {}):
                    # sbid_list = self.meas_map_obj.get_serial_board_id_list(shid)
                    # if sbid_list:
                    #     self.task_mgr_obj.delete_task(sbid_list[0])
                    #     self.task_mgr_obj.delete_task(sbid_list[1])
            if request_xml:
                self.influxdb_obj.write_log_influxdb("INFO", f"[Send] {message_name} | {shid}", "system")
                self.influxdb_obj.write_msg_influxdb("INFO", f"{self.reformat_xml_grouped(request_xml.decode(), shid)}", sbid, shid)
                # self.influxdb_obj.write_msg_influxdb("INFO", f"{minidom.parseString(request_xml.decode()).toprettyxml(indent="    ")}", sbid)
                self.socket.sendall(struct.pack(">I", len(request_xml)) + request_xml)
                self.logging.debug(f"[Send] {message_name} | shid: {shid}")
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"send_request failed\n{self.logger.get_slim_error_log()}", "system")

    def create_xml_with_pallet(self, root, selected_pallets):
        """
        Create left or right pallet xml

        Args:
            root: Complete xml information
            selected_pallets: Select left or right pallet xml

        Returns:
            Left or right pallet xml

        Example:
            >>> message_obj = message_api("127.0.0.1", 12345)
            >>> message_obj.create_xml_with_pallet(root, selected_pallets)
        """
        try:
            new_root = ET.Element(root.tag)
            new_header = ET.SubElement(new_root, "HEADER")
            new_body = ET.SubElement(new_root, "BODY")
            new_return = ET.SubElement(new_root, "RETURN")

            header = root.find(".//HEADER")
            new_header.extend(list(header))

            body = root.find(".//BODY")
            for child in body:
                if child.tag != "PalletInfo":
                    new_body.append(child)

            new_pallet_info = ET.SubElement(new_body, "PalletInfo")
            for pallet in selected_pallets:
                new_pallet_info.append(pallet)

            ret = root.find(".//RETURN")
            new_return.extend(list(ret))

            return ET.tostring(new_root, encoding="utf-8", method="xml", short_empty_elements=False).decode("utf-8")
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"create_xml_with_pallet failed\n{self.logger.get_slim_error_log()}", "system")

    def update_meas_map(self, storehouse_id, root):
        """
        Update the latest measurement mapping table

        Args:
            storehouse_id: Store house id
            root: Complete xml information

        Returns:
            Left or right pallet xml

        Example:
            >>> message_obj = message_api("127.0.0.1", 12345)
            >>> message_obj.update_meas_map(storehouse_id, root)
        """
        try:
            self.meas_map_obj.delete_serial_board_id_entry(storehouse_id)
            self.meas_map_obj.add_serial_board_id_entry(storehouse_id, self.meas_map_obj.parse_serial_board_id(root))
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"update_meas_map failed\n{self.logger.get_slim_error_log()}", "system")

    def split_LR_pallet(self, root):
        """
        Separate the left and right pallets of the storehouse

        Args:
            root: Complete xml information

        Returns:
            Left or right pallet xml

        Example:
            >>> message_obj = message_api("127.0.0.1", 12345)
            >>> message_obj.split_LR_pallet(root)
        """
        try:
            pallet_info = root.find(".//PalletInfo")
            pallets = pallet_info.findall(".//Pallet")
            return self.create_xml_with_pallet(root, [pallets[0]]), self.create_xml_with_pallet(root, [pallets[1]])
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"split_LR_pallet failed\n{self.logger.get_slim_error_log()}", "system")
    
    def handle_ng_response(self, message_name, tid, trx_id, shid, sbid_l, sbid_r, msg):
        try:
            missing_values = (None, "", "None")
            current_task_info = self.meas_map_obj.get_task_info()

            # Reuse the stored TaskInfo only when it belongs to this request.
            # Otherwise start from an empty TaskInfo to avoid leaking metadata
            # from the previous request when XML parsing failed early.
            same_request = any(
                request_value not in missing_values
                and stored_value not in missing_values
                and str(request_value) == str(stored_value)
                for request_value, stored_value in (
                    (tid, current_task_info.get("TransactionID")),
                    (trx_id, current_task_info.get("TrxID")),
                )
            )
            task_info = (
                current_task_info
                if same_request
                else type(current_task_info)()
            )

            # Values extracted from the raw XML have priority. Missing values
            # do not overwrite data already stored for the same request.
            for key, value in (
                ("MessageName", message_name),
                ("TransactionID", tid),
                ("TrxID", trx_id),
                ("StoreHouseID", shid),
            ):
                if value not in missing_values:
                    task_info.set(key, value)

            effective_message_name = task_info.get("MessageName")
            effective_storehouse_id = task_info.get("StoreHouseID")
            if (
                effective_message_name in missing_values
                or effective_storehouse_id in missing_values
                or str(effective_storehouse_id) == "0"
            ):
                raise ValueError(
                    "Cannot route NG response without MessageName and StoreHouseID"
                )

            # W2003/W2004/W2005 do not include SerialBoardID in the request.
            # Use the rack mapping when available, but still create rack-level
            # left/right ACKs if the mapping lookup itself fails.
            if not sbid_l or not sbid_r:
                try:
                    serial_board_ids = (
                        self.meas_map_obj.get_serial_board_id_list(
                            effective_storehouse_id
                        )
                        or []
                    )
                    if not sbid_l and len(serial_board_ids) >= 1:
                        sbid_l = serial_board_ids[0]
                    if not sbid_r and len(serial_board_ids) >= 2:
                        sbid_r = serial_board_ids[1]
                except Exception:
                    # Board IDs improve ACK detail but are not required to
                    # route a rack-level failure reply.
                    pass

            task_info.set("ReturnCode", "1")

            board_ids = []
            for pallet_position, serial_board_id in (
                ("L", sbid_l),
                ("R", sbid_r),
            ):
                task_info.set("PalletPosition", pallet_position)
                task_info.set("SerialBoardID", serial_board_id or "")
                task_info.set(
                    "ReturnMessage",
                    (
                        f"{msg} [{serial_board_id}]"
                        if serial_board_id
                        else msg
                    ),
                )
                cache_updated = self.msg_cache.handle_esp_data(
                    json.dumps(task_info.get_data(), ensure_ascii=False),
                    update_time=time.time(),
                )
                if not cache_updated:
                    raise RuntimeError(
                        f"Failed to cache {pallet_position} pallet NG ACK"
                    )
                if serial_board_id:
                    board_ids.append(str(serial_board_id))

            detail = " | ".join([msg, *board_ids])
            self.influxdb_obj.write_log_influxdb(
                "INFO",
                f"{detail} | {effective_storehouse_id}",
                effective_storehouse_id,
            )
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"handle_ng_response failed\n{self.logger.get_slim_error_log()}", "system")
            raise

    # def parse_and_handle_messages(self, xml_string):
    #     """
    #     Parse and split xml message

    #     Args:
    #         xml_data: Xml message (including one or more requests)

    #     Returns:
    #         None

    #     Example:
    #         >>> message_obj = message_api("127.0.0.1", 12345)
    #         >>> message_obj.parse_and_handle_messages(xml_string)
    #     """
    #     try:
    #         # print(minidom.parseString(xml_string).toprettyxml(indent="    "))
    #         handle_msg = "Parsing XML error"
    #         root = ET.fromstring(xml_string)  # noqa: S314
    #         message_name = root.find("./HEADER/MESSAGENAME").text
    #         tid = root.find("./HEADER/TRANSACTIONID").text
    #         trx_id = root.find("./BODY/TRX_ID").text

    #         storehouse_id = (root.findtext(".//StoreHouseID") or "None").strip()
    #         sbid_l = None
    #         sbid_r = None
    #         self.influxdb_obj.write_log_influxdb("DEBUG", f"[{message_name}] [{storehouse_id}]", "system")
    #         self.logging.debug(f"[{message_name}] [{storehouse_id}]")
    #         if message_name == "StoreHouseStatusRequest":
    #             # Register Task information
    #             self.meas_map_obj.update_task_info(xml_string)
    #             sbid_l = root.findall(".//SerialBoardID")[0].text
    #             sbid_r = root.findall(".//SerialBoardID")[1].text
    #             new_root = root
    #             left_serial_board_id = str(
    #                 root.findall(".//SerialBoardID")[0].text
    #             ).strip()

    #             right_serial_board_id = str(
    #                 root.findall(".//SerialBoardID")[1].text
    #             ).strip()
    #             query = sql_text(
    #                 """
    #                 SELECT
    #                     CAST(SerialBoardID AS NVARCHAR(100)) AS SerialBoardID,
    #                     CAST(ProtectBoardID AS NVARCHAR(100)) AS ProtectBoardID
    #                 FROM dbo.BoardMapping
    #                 WHERE CAST(SerialBoardID AS NVARCHAR(100)) IN (
    #                     CAST(:left_serial_board_id AS NVARCHAR(100)),
    #                     CAST(:right_serial_board_id AS NVARCHAR(100))
    #                 )
    #                 """
    #             ).bindparams(
    #                 left_serial_board_id=left_serial_board_id,
    #                 right_serial_board_id=right_serial_board_id,
    #             )
    #             board_mapping_df = self.mssql_obj.query_db_pd(query)
    #             protect_board_map = {
    #                 str(row.SerialBoardID): str(row.ProtectBoardID)
    #                 for row in board_mapping_df.itertuples(index=False)
    #             }

    #             missing_serial_board_ids = [
    #                 serial_board_id
    #                 for serial_board_id in (root.findall('.//SerialBoardID')[0].text, root.findall('.//SerialBoardID')[1].text)
    #                 if serial_board_id not in protect_board_map
    #             ]

    #             if missing_serial_board_ids:
    #                 handle_msg = "Missing ProtectBoardID"
    #                 raise ValueError(
    #                     "BoardMapping cannot find the corresponding ProtectBoardID: "
    #                     + ", ".join(missing_serial_board_ids)
    #                 )

    #             new_root.findall(".//DRCID")[0].text = protect_board_map[left_serial_board_id]
    #             new_root.findall(".//DRCID")[1].text = protect_board_map[right_serial_board_id]

    #             root = new_root

    #             # Create Response Cache
    #             # self.msg_cache.handle_esp_data(None, new_root.find(".//StoreHouseID").text, message_name, time.time())

    #             # Split the left and right pallets of the storehouse
    #             left_xml, right_xml = self.split_LR_pallet(root)

    #             # Create tasks for the left and right pallets
    #             self.influxdb_obj.write_log_influxdb("DEBUG", f"Create Task [{root.findall('.//SerialBoardID')[0].text}] [{root.findall('.//SerialBoardID')[1].text}]", "system")
    #             handle_msg = "SerialBoard is already active"
    #             if not self.task_mgr_obj.check_task_exists(root.findall(".//SerialBoardID")[0].text, root.findall(".//SerialBoardID")[1].text, root.findall(".//StoreHouseID")[0].text):
    #                 handle_msg = "CCMS Internal error"
    #                 self.logging.debug(f"Create Task [{root.findall('.//SerialBoardID')[0].text}] [{root.findall('.//SerialBoardID')[1].text}]")
    #                 self.task_mgr_obj.create_task(left_xml, right_xml, root.findall(".//SerialBoardID")[0].text, root.findall(".//SerialBoardID")[1].text, root.findall(".//StoreHouseID")[0].text, root.findall(".//DRCID")[0].text, root.findall(".//DRCID")[1].text)
    #                 self.redis_obj.delete(root.findall(".//SerialBoardID")[0].text)
    #                 self.redis_obj.delete(root.findall(".//SerialBoardID")[1].text)
    #                 self.influxdb_obj.write_log_influxdb("DEBUG", f"Check Task Online: {root.findall('.//SerialBoardID')[0].text}, {root.findall('.//SerialBoardID')[1].text}", "system")
    #                 # Wait for both task processes to start and complete
    #                 # self.task_mgr_obj.check_task_ready(root.findall(".//SerialBoardID")[0].text, root.findall(".//SerialBoardID")[1].text)
    #                 # self.influxdb_obj.write_log_influxdb("DEBUG", f"Task Ready: {root.findall('.//SerialBoardID')[0].text}, {root.findall('.//SerialBoardID')[1].text}", "system")
    #                 # Create a rack status table
    #                 rack_id = root.find(".//StoreHouseID").text
    #                 self.mssql_obj.delete_rack_status(self.mssql_obj, rack_id)
    #                 self.mssql_obj.create_rack_status(self.mssql_obj, rack_id, root.findall(".//SerialBoardID")[0].text)
    #                 self.mssql_obj.create_rack_status(self.mssql_obj, rack_id, root.findall(".//SerialBoardID")[1].text)
    #                 self.meas_map_obj.update_device_info(self.mssql_obj, xml_string)
    #                 # Send config to two tasks
    #                 self.redis_obj.rpush(root.findall(".//SerialBoardID")[0].text, left_xml)
    #                 self.redis_obj.rpush(root.findall(".//SerialBoardID")[1].text, right_xml)
    #                 # Create a relationship table between left pallet, right pallet and storehouseID
    #                 self.update_meas_map(root.find(".//StoreHouseID").text, root)
    #                 # Calculates whether the pallet task corresponding to the storehouseID has completed the message operation.
    #                 # If completed, add +1. When the value is 2, it means that the operation of returning the storehouseID is completed.
    #                 self.sh_ack[root.find(".//StoreHouseID").text] = 0
    #                 self.alarm_stop_shid.pop(root.find(".//StoreHouseID").text, None)
    #                 self.logging.debug(f"StoreHouseStatusRequest {root.findall('.//SerialBoardID')[0].text} {root.findall('.//SerialBoardID')[1].text}")
    #                 self.influxdb_obj.write_log_influxdb("DEBUG", f"[Internal Send] StoreHouseStatusRequest {root.findall('.//SerialBoardID')[0].text}", root.findall(".//SerialBoardID")[0].text)
    #                 self.influxdb_obj.write_log_influxdb("DEBUG", f"[Internal Send] StoreHouseStatusRequest {root.findall('.//SerialBoardID')[1].text}", root.findall(".//SerialBoardID")[1].text)
    #             else:
    #                 raise  (f"SerialBoardID is already active | {sbid_l} or {sbid_r}")
    #         elif message_name == "StoreHouseStepCheckRequest":
    #             handle_msg = "CCMS internal error"
    #             # Register Task information
    #             self.meas_map_obj.update_task_info(xml_string)
    #             sbid_list = self.meas_map_obj.get_serial_board_id_list(root.find(".//StoreHouseID").text)
    #             if sbid_list:
    #                 sbid_l = sbid_list[0]
    #                 sbid_r = sbid_list[1]
    #                 self.redis_obj.rpush(sbid_list[0], xml_string)
    #                 self.redis_obj.rpush(sbid_list[1], xml_string)
    #                 # Update task step
    #                 self.task_mgr_obj.update_task_step(sbid_list[0], sbid_list[1], xml_string, root.findall(".//STEP")[0].text)
    #             self.sh_ack[root.find(".//StoreHouseID").text] = 0
    #             self.logging.debug(f"StoreHouseStepCheckRequest {sbid_list[0]} {sbid_list[1]}")
    #             self.influxdb_obj.write_log_influxdb("DEBUG", f"[Internal Send] StoreHouseStepCheckRequest {sbid_list[0]} STEP: {root.findall('.//STEP')[0].text}", sbid_list[0])
    #             self.influxdb_obj.write_log_influxdb("DEBUG", f"[Internal Send] StoreHouseStepCheckRequest {sbid_list[1]} STEP: {root.findall('.//STEP')[0].text}", sbid_list[1])
    #         elif message_name == "SyncTimeReply":
    #             # Update system time
    #             self.sync_time_obj.run(root.find(".//BODY/SYSTIME").text)
    #         elif message_name == "StoreHouseNGCheckRequest":
    #             handle_msg = "CCMS Internal error"
    #             sbid_list = self.meas_map_obj.get_serial_board_id_list(root.find(".//StoreHouseID").text)
    #             if sbid_list:
    #                 sbid_l = sbid_list[0]
    #                 sbid_r = sbid_list[1]
    #             # Get Rack Info
    #             self.meas_map_obj.update_task_info(xml_string)
    #             self.msg_cache.handle_rack_status(int(root.find(".//StoreHouseID").text))
    #         elif message_name == "JudgmentCompletionNotificationRequest":
    #             handle_msg = "CCMS Internal error"
    #             # Get SerialBoardID list
    #             sbid_list = self.meas_map_obj.get_serial_board_id_list(root.find(".//StoreHouseID").text)
    #             if sbid_list:
    #                 sbid_l = sbid_list[0]
    #                 sbid_r = sbid_list[1]
    #                 self.task_mgr_obj.delete_task(sbid_l)
    #                 self.task_mgr_obj.delete_task(sbid_r)
    #                 self.msg_cache.handle_rack_stop(int(root.find(".//StoreHouseID").text))
    #             # Create Response Cache
    #             self.meas_map_obj.update_task_info(xml_string)
    #             self.msg_cache.handle_esp_data(None, root.find(".//StoreHouseID").text, message_name, time.time())
    #     except Exception:
    #         self.handle_ng_response(message_name, tid, trx_id, storehouse_id, sbid_l, sbid_r, handle_msg)
    #         self.influxdb_obj.write_log_influxdb("ERROR", f"parse_and_handle_messages failed\n{self.logger.get_slim_error_log()}", "system")            

    def extract_xml_value(self, xml_string, tag_name):
        """Extract a tag value directly from raw XML text."""
        try:
            match = re.search(
                rf"<{re.escape(tag_name)}>(.*?)</{re.escape(tag_name)}>",
                xml_string,
                re.DOTALL
            )

            if match:
                return match.group(1).strip()

            return None

        except Exception as ex:
            self.influxdb_obj.write_log_influxdb(
                "ERROR",
                f"Extract XML value failed\n"
                f"{self.logger.get_slim_error_log()}"
            )
            return None

    def parse_and_handle_messages(self, xml_string):
        """
        Parse and split XML message.

        Args:
            xml_string: XML message including one or more requests.

        Returns:
            None
        """
        # ---------------------------------------------------------
        # Prepare fallback values before XML parsing.
        # These values are used for NG response if XML parsing fails.
        # ---------------------------------------------------------
        message_name = None
        tid = None
        trx_id = None
        storehouse_id = None
        sbid_l = None
        sbid_r = None

        handle_msg = "Parsing XML error"

        try:
            # ---------------------------------------------------------
            # 1. Try to extract important fields from raw XML first.
            #
            # Even if ET.fromstring() fails later, these values may
            # still be available for handle_ng_response().
            # ---------------------------------------------------------
            message_name = self.extract_xml_value(
                xml_string,
                "MESSAGENAME"
            )

            tid = self.extract_xml_value(
                xml_string,
                "TRANSACTIONID"
            )

            trx_id = self.extract_xml_value(
                xml_string,
                "TRX_ID"
            )

            storehouse_id = self.extract_xml_value(
                xml_string,
                "StoreHouseID"
            )

            # ---------------------------------------------------------
            # Try to get SerialBoardID values from raw XML.
            # ---------------------------------------------------------
            serial_board_ids = re.findall(
                r"<SerialBoardID>(.*?)</SerialBoardID>",
                xml_string,
                re.DOTALL
            )

            serial_board_ids = [
                value.strip()
                for value in serial_board_ids
                if value.strip()
            ]

            if len(serial_board_ids) >= 1:
                sbid_l = serial_board_ids[0]

            if len(serial_board_ids) >= 2:
                sbid_r = serial_board_ids[1]

            # ---------------------------------------------------------
            # 2. Parse complete XML.
            # ---------------------------------------------------------
            root = ET.fromstring(xml_string)  # noqa: S314

            # ---------------------------------------------------------
            # 3. After successful parsing, use XML parser results.
            # ---------------------------------------------------------
            message_name = (
                root.findtext("./HEADER/MESSAGENAME")
                or message_name
            )

            tid = (
                root.findtext("./HEADER/TRANSACTIONID")
                or tid
            )

            trx_id = (
                root.findtext("./BODY/TRX_ID")
                or trx_id
            )

            storehouse_id = (
                root.findtext(".//StoreHouseID")
                or storehouse_id
                or "None"
            ).strip()

            self.influxdb_obj.write_log_influxdb(
                "DEBUG",
                f"[{message_name}] [{storehouse_id}]",
                "system"
            )

            self.logging.debug(
                f"[{message_name}] [{storehouse_id}]"
            )

            # =========================================================
            # StoreHouseStatusRequest
            # =========================================================
            if message_name == "StoreHouseStatusRequest":

                # Register Task information
                self.meas_map_obj.update_task_info(xml_string)

                serial_board_nodes = root.findall(".//SerialBoardID")

                if len(serial_board_nodes) < 2:
                    handle_msg = "Invalid SerialBoardID"
                    raise ValueError(
                        "StoreHouseStatusRequest requires two SerialBoardID"
                    )

                sbid_l = serial_board_nodes[0].text
                sbid_r = serial_board_nodes[1].text

                left_serial_board_id = str(
                    serial_board_nodes[0].text
                ).strip()

                right_serial_board_id = str(
                    serial_board_nodes[1].text
                ).strip()

                query = sql_text(
                    """
                    SELECT
                        CAST(SerialBoardID AS NVARCHAR(100)) AS SerialBoardID,
                        CAST(ProtectBoardID AS NVARCHAR(100)) AS ProtectBoardID
                    FROM dbo.BoardMapping
                    WHERE CAST(SerialBoardID AS NVARCHAR(100)) IN (
                        CAST(:left_serial_board_id AS NVARCHAR(100)),
                        CAST(:right_serial_board_id AS NVARCHAR(100))
                    )
                    """
                ).bindparams(
                    left_serial_board_id=left_serial_board_id,
                    right_serial_board_id=right_serial_board_id,
                )

                board_mapping_df = self.mssql_obj.query_db_pd(query)

                protect_board_map = {
                    str(row.SerialBoardID): str(row.ProtectBoardID)
                    for row in board_mapping_df.itertuples(index=False)
                }

                missing_serial_board_ids = [
                    serial_board_id
                    for serial_board_id in (
                        left_serial_board_id,
                        right_serial_board_id
                    )
                    if serial_board_id not in protect_board_map
                ]

                if missing_serial_board_ids:
                    handle_msg = "Missing ProtectBoardID"

                    raise ValueError(
                        "BoardMapping cannot find the corresponding "
                        "ProtectBoardID: "
                        + ", ".join(missing_serial_board_ids)
                    )

                root.findall(".//DRCID")[0].text = (
                    protect_board_map[left_serial_board_id]
                )

                root.findall(".//DRCID")[1].text = (
                    protect_board_map[right_serial_board_id]
                )

                # Split left and right pallet.
                left_xml, right_xml = self.split_LR_pallet(root)

                self.influxdb_obj.write_log_influxdb(
                    "DEBUG",
                    (
                        f"Create Task "
                        f"[{left_serial_board_id}] "
                        f"[{right_serial_board_id}]"
                    ),
                    "system"
                )

                handle_msg = "SerialBoard is already active"

                if not self.task_mgr_obj.check_task_exists(
                    left_serial_board_id,
                    right_serial_board_id,
                    storehouse_id
                ):
                    handle_msg = "CCMS Internal error"

                    self.logging.debug(
                        f"Create Task "
                        f"[{left_serial_board_id}] "
                        f"[{right_serial_board_id}]"
                    )

                    self.task_mgr_obj.create_task(
                        left_xml,
                        right_xml,
                        left_serial_board_id,
                        right_serial_board_id,
                        storehouse_id,
                        root.findall(".//DRCID")[0].text,
                        root.findall(".//DRCID")[1].text
                    )

                    self.redis_obj.delete(left_serial_board_id)
                    self.redis_obj.delete(right_serial_board_id)

                    rack_id = storehouse_id

                    self.mssql_obj.delete_rack_status(
                        self.mssql_obj,
                        rack_id
                    )

                    self.mssql_obj.create_rack_status(
                        self.mssql_obj,
                        rack_id,
                        left_serial_board_id
                    )

                    self.mssql_obj.create_rack_status(
                        self.mssql_obj,
                        rack_id,
                        right_serial_board_id
                    )

                    self.meas_map_obj.update_device_info(
                        self.mssql_obj,
                        xml_string
                    )

                    self.redis_obj.rpush(
                        left_serial_board_id,
                        left_xml
                    )

                    self.redis_obj.rpush(
                        right_serial_board_id,
                        right_xml
                    )

                    self.update_meas_map(
                        storehouse_id,
                        root
                    )

                    self.sh_ack[storehouse_id] = 0

                    self.alarm_stop_shid.pop(
                        storehouse_id,
                        None
                    )

                    self.logging.debug(
                        f"StoreHouseStatusRequest "
                        f"{left_serial_board_id} "
                        f"{right_serial_board_id}"
                    )

                else:
                    raise ValueError(
                        f"SerialBoardID is already active | "
                        f"{sbid_l} or {sbid_r}"
                    )

            # =========================================================
            # StoreHouseStepCheckRequest
            # =========================================================
            elif message_name == "StoreHouseStepCheckRequest":

                handle_msg = "CCMS Internal error"

                self.meas_map_obj.update_task_info(xml_string)

                sbid_list = (
                    self.meas_map_obj.get_serial_board_id_list(
                        storehouse_id
                    )
                )

                if not sbid_list or len(sbid_list) < 2:
                    handle_msg = "Missing SerialBoardID mapping"
                    raise ValueError(
                        f"StoreHouseID [{storehouse_id}] requires two "
                        "SerialBoardID mappings"
                    )

                sbid_l = sbid_list[0]
                sbid_r = sbid_list[1]

                self.redis_obj.rpush(
                    sbid_l,
                    xml_string
                )

                self.redis_obj.rpush(
                    sbid_r,
                    xml_string
                )

                step = root.findtext(".//STEP")

                self.task_mgr_obj.update_task_step(
                    sbid_l,
                    sbid_r,
                    xml_string,
                    step
                )

                self.sh_ack[storehouse_id] = 0

            # =========================================================
            # SyncTimeReply
            # =========================================================
            elif message_name == "SyncTimeReply":

                self.sync_time_obj.run(
                    root.find(".//BODY/SYSTIME").text
                )

            # =========================================================
            # StoreHouseNGCheckRequest
            # =========================================================
            elif message_name == "StoreHouseNGCheckRequest":

                handle_msg = "CCMS Internal error"

                sbid_list = (
                    self.meas_map_obj.get_serial_board_id_list(
                        storehouse_id
                    )
                )

                if sbid_list:
                    sbid_l = sbid_list[0]
                    sbid_r = sbid_list[1]

                self.meas_map_obj.update_task_info(
                    xml_string
                )

                if not self.msg_cache.handle_rack_status(
                    int(storehouse_id)
                ):
                    raise RuntimeError(
                        f"Failed to prepare W2004 response cache\n"
                        f"{self.logger.get_slim_error_log()}"
                    )

            # =========================================================
            # JudgmentCompletionNotificationRequest
            # =========================================================
            elif message_name == "JudgmentCompletionNotificationRequest":

                handle_msg = "CCMS Internal error"

                sbid_list = (
                    self.meas_map_obj.get_serial_board_id_list(
                        storehouse_id
                    )
                )

                if not sbid_list or len(sbid_list) < 2:
                    handle_msg = "Missing SerialBoardID mapping"
                    raise ValueError(
                        f"StoreHouseID [{storehouse_id}] requires two "
                        "SerialBoardID mappings"
                    )

                sbid_l = sbid_list[0]
                sbid_r = sbid_list[1]

                self.task_mgr_obj.delete_task(sbid_l)
                self.task_mgr_obj.delete_task(sbid_r)

                if not self.msg_cache.handle_rack_stop(
                    int(storehouse_id)
                ):
                    raise RuntimeError(
                        f"Failed to prepare W2005 rack stop cache\n"
                        f"{self.logger.get_slim_error_log()}"
                    )

                self.meas_map_obj.update_task_info(
                    xml_string
                )

                if not self.msg_cache.handle_esp_data(
                    None,
                    storehouse_id,
                    message_name,
                    time.time()
                ):
                    raise RuntimeError(
                        f"Failed to prepare W2005 response cache\n"
                        f"{self.logger.get_slim_error_log()}"
                    )

        except Exception as ex:

            # ---------------------------------------------------------
            # Log actual internal error.
            # ---------------------------------------------------------
            self.influxdb_obj.write_log_influxdb(
                "ERROR",
                (
                    f"Handle message failed\n"
                    f"{self.logger.get_slim_error_log()}"
                ),
                "system"
            )

            self.logging.exception(
                f"Handle message failed\n"
                f"{self.logger.get_slim_error_log()}"
            )

            # ---------------------------------------------------------
            # Send one rack-level NG response from this outermost handler.
            # ---------------------------------------------------------
            try:
                if any(
                    value not in (None, "", "None")
                    for value in (
                        message_name,
                        tid,
                        trx_id,
                        storehouse_id,
                        sbid_l,
                        sbid_r,
                    )
                ):
                    self.handle_ng_response(
                        message_name,
                        tid,
                        trx_id,
                        storehouse_id,
                        sbid_l,
                        sbid_r,
                        handle_msg
                    )

            except Exception as response_ex:
                self.influxdb_obj.write_log_influxdb(
                    "ERROR",
                    (
                        f"Handle NG response failed\n"
                        f"{self.logger.get_slim_error_log()}"
                    ),
                    "system"
                    )
                raise response_ex

    def receive_response(self):
        """
        Response Message

        Args:
            None

        Returns:
            None

        Example:
            >>> message_obj = message_api("127.0.0.1", 12345)
            >>> message_obj.receive_response()
        """
        response_data = b""
        try:
            # Receive ACK from task
            while True:
                data = self.redis_obj.lpop("ACK")
                if not data:
                    break
            # data = self.redis_obj.lpop("ACK")
            # if data:
                raw_data = data.decode()
                parsed_data = json.loads(raw_data)
                message_name = parsed_data.get("MessageName", {}).get("0", "")

                if message_name == "ESPRequest":
                    # W3001 is periodic status data.
                    # Only update the latest rack snapshot.
                    self.msg_cache.update_rack_status(raw_data)
                else:
                    # W1001/W2002/W2003 and other command responses.
                    if not self.msg_cache.handle_esp_data(
                        raw_data,
                        update_time=time.time(),
                    ):
                        raise RuntimeError(
                            f"Failed to cache ESP response | "
                            f"MessageName: [{message_name}]"
                        )
                    self.msg_cache.update_rack_status(raw_data)

            self.socket.settimeout(0.01)
            header = self.socket.recv(4)
            # lent=len(header)
            length = struct.unpack(">I", header)[0]
            data = self.socket.recv(length)
            # print(data)
            length = length - len(data)
            response_data += data
            while length > 0:
                data = self.socket.recv(length)
                if not data:
                    raise ConnectionError("Connection Failed")
                # print(data)
                response_data += data
                length -= len(data)

            response_text = response_data.decode()
            # print(response_text)
            # Parse and split xml message
            self.parse_and_handle_messages(response_text)
        except BlockingIOError:
            pass
        except TimeoutError:
            pass
        except Exception as e:
            self.influxdb_obj.write_log_influxdb("ERROR", f"receive_response failed\n{self.logger.get_slim_error_log()}", "system")
            raise e

    def convert_xml_to_json(self, xml_data):
        """
        Convert xml to json

        Args:
            xml_data: Xml message

        Returns:
            None

        Example:
            >>> message_obj = message_api("127.0.0.1", 12345)
            >>> message_obj.convert_xml_to_json(xml_data)
        """
        try:
            root = ET.fromstring(xml_data)  # noqa: S314
            return {child.tag: child.text for child in root.iter() if child.text is not None}
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"convert_xml_to_json failed\n{self.logger.get_slim_error_log()}", "system")

    def run(self):
        """
        Process request and reply message

        Args:
            None

        Returns:
            None

        Example:
            >>> message_obj = message_api("127.0.0.1", 12345)
            >>> message_obj.run()
        """
        try:
            self.last_time_sync = self.last_alive_check = self.last_alarm_stop = time.time()
            while self.running:
                try:
                    self.connect()
                    current_time = time.time()

                    # Process time sync for 24 hour
                    if current_time - self.last_time_sync >= 86400:
                        self.send_request("SyncTimeRequest", self.generate_transaction_id(), None, "system")
                        self.last_time_sync = current_time

                    # Send ccms alive status for 1 minute
                    if current_time - self.last_alive_check >= 60:
                        self.send_request("AliveRequest", self.generate_transaction_id(), None, "system")
                        self.last_alive_check = current_time

                    # Receive Message
                    self.receive_response()

                    # Reply Message
                    rack_id, msg_name, xml_msg = self.msg_cache.process_msg(self.meas_map_obj.get_task_info())
                    if xml_msg:
                        self.send_request(msg_name, xml_msg, "system", rack_id)
                    # time.sleep(0.5)
                except Exception:
                    self.influxdb_obj.write_log_influxdb("ERROR", f"run failed\n{self.logger.get_slim_error_log()}", "system")
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"run failed\n{self.logger.get_slim_error_log()}", "system")

    def stop(self):
        """
        Stop socket connection

        Args:
            None

        Returns:
            None

        Example:
            >>> message_obj = message_api("127.0.0.1", 12345)
            >>> message_obj.connect()
            >>> message_obj.stop()
        """
        try:
            self.running = False
            self.socket.close()
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"stop failed\n{self.logger.get_slim_error_log()}", "system")

    # def handle_task_operation(self):
    #     """
    #     Handle task operation to do action

    #     Args:
    #         None

    #     Returns:
    #         None

    #     Example:
    #         >>> message_obj = message_api("127.0.0.1", 12345)
    #         >>> message_obj.connect()
    #         >>> message_obj.handle_task_operation()
    #     """
    #     try:
    #         while True:
    #             user_input = input()
    #             inputs = user_input.split()
    #             try:
    #                 action = inputs[0]
    #                 serialboard_id = inputs[1]
    #                 response_root = ET.Element("MESSAGE")
    #                 header = ET.SubElement(response_root, "HEADER")
    #                 ET.SubElement(header, "MESSAGENAME").text = action
    #                 self.redis_obj.rpush(serialboard_id, ET.tostring(response_root, encoding="utf-8", short_empty_elements=False))
    #             except Exception:
    #                 self.logging.error(f"handle_task_operation failed\n{self.logger.get_slim_error_log()}")
    #                 self.logging.error(
    #                     """Usage:
    #                        Command [Action] [Serialboard_ID]
    #                        Examples:
    #                        > Disconnect A0001\t...Close A0001 to ESP Socket Connection"""
    #                 )
    #     except Exception:
    #         self.influxdb_obj.write_log_influxdb("ERROR", f"handle_task_operation failed\n{self.logger.get_slim_error_log()}", "system")
