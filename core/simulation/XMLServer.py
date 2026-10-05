from datetime import datetime, timedelta
import xml.etree.ElementTree as ET  # noqa: S405
import json
import logging
import os
import math

import xml.dom.minidom as minidom  # noqa: S408
import secrets
import select
import socket
import struct
import sys
import threading
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import balps.influxdb_mgr
import balps.mssql_mgr
from remote_mode_api import RemoteBatteryModeAPI
from balps import log_mgr


class XMLSocketServer:
    def __init__(self, host, port):
        """Xml socket server initialization

        Args:
            host: Server ip
            port: Server port

        Returns:
            None

        Example:
            >>> xml_socket_svr_obj = XMLSocketServer()

        """
        try:
            self.logger = log_mgr.logging_api(
                filename=os.path.join("logs", f"{os.path.splitext(os.path.basename(sys.argv[0]))[0]}"),
                level=logging.DEBUG,
                backup_count=7,
            )
            self.logging = self.logger.get_logger()  # Create influxdb and mssql object
            # Create xml socket server
            self.host = host
            self.port = port
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.setblocking(False)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(0)
            self.sockets_list = [self.server_socket]
            self.clients = {}
            self.request_xml = b""
            # Operation command e.g. w2002 1 1 0
            self.cmd = ""
            self.cmd_start_idx = 0
            self.cmd_end_idx = 0
            self.cmd_step = 0
            # Get charging cabinet information
            # The ServerMap table has StoreHouseID, PalletID, SerialBoardID, PalletPosition, Position and QRCODEID fields
            self.mssql_obj = balps.mssql_mgr.mssql_api()
            self.mssql_obj.create_db_connect_pd()
            self.meas_map = self.mssql_obj.query_db_pd("SELECT * FROM CCSMap")
            self.influxdb_obj = balps.influxdb_mgr.influxdb_api()
            self.influxdb_obj.create_connect()
            self.ccs_api = None
        except Exception:
            self.logging.error(f"Xml server initialization failed\n{self.logger.get_slim_error_log()}")

    def generate_transaction_id(self):
        """Transaction id generation

        Args:
            None

        Returns:
            Transaction ID

        Example:
            >>> xml_socket_svr_obj = XMLSocketServer()
            >>> xml_socket_svr_obj.generate_transaction_id()

        """
        try:
            # Generate Transaction ID e.g. 20200402172959123456
            current_time = datetime.now().strftime("%Y%m%d%H%M%S%f")[:17]
            random_number = f"{secrets.randbelow(1000):03d}"
            transaction_id = current_time + random_number
            return transaction_id
        except Exception:
            self.logging.error(f"generate_transaction_id failed\n{self.logger.get_slim_error_log()}")

    def create_reply_w0001_xml(self, message_name, transaction_id):
        """Generates an xml response of w0001 command

        Args:
            message_name: Message name
            transaction_id: Transaction id

        Returns:
            Xml reply

        Example:
            >>> xml_socket_svr_obj = XMLSocketServer()
            >>> xml_socket_svr_obj.create_reply_w0001_xml(message_name, transaction_id)

        """
        try:
            # Create reply xml
            response_root = ET.Element("MESSAGE")
            header = ET.SubElement(response_root, "HEADER")
            ET.SubElement(header, "MESSAGENAME").text = message_name
            ET.SubElement(header, "TRANSACTIONID").text = transaction_id
            ET.SubElement(header, "REPLYSUBJECTNAME").text = ""
            ET.SubElement(header, "INBOXNAME").text = ""
            ET.SubElement(header, "LISTENER").text = ""

            body = ET.SubElement(response_root, "BODY")
            ET.SubElement(body, "TRX_ID").text = transaction_id
            ET.SubElement(body, "LINE_ID").text = "SECCHARGE0100"
            ET.SubElement(body, "SYSTIME").text = datetime.now().strftime("%Y%m%d%H%M%S")
            # ET.SubElement(body, "SYSTIME").text = (datetime.now() + timedelta(minutes=10)).strftime("%Y%m%d%H%M%S")

            ret = ET.SubElement(response_root, "RETURN")
            ET.SubElement(ret, "RETURNCODE").text = "0"
            ET.SubElement(ret, "RETURNMESSAGE").text = ""

            return ET.tostring(response_root, encoding="utf-8", short_empty_elements=False)
        except Exception:
            self.logging.error(f"create_reply_w0001_xml failed\n{self.logger.get_slim_error_log()}")

    def create_reply_w0002_xml(self, message_name, transaction_id):
        """Generates an xml response of w0002 command

        Args:
            message_name: Message name
            transaction_id: Transaction id

        Returns:
            Xml reply

        Example:
            >>> xml_socket_svr_obj = XMLSocketServer()
            >>> xml_socket_svr_obj.create_reply_w0002_xml(message_name, transaction_id)

        """
        try:
            # Create reply xml
            response_root = ET.Element("MESSAGE")
            header = ET.SubElement(response_root, "HEADER")
            ET.SubElement(header, "MESSAGENAME").text = message_name
            ET.SubElement(header, "TRANSACTIONID").text = transaction_id
            ET.SubElement(header, "REPLYSUBJECTNAME").text = ""
            ET.SubElement(header, "INBOXNAME").text = ""
            ET.SubElement(header, "LISTENER").text = ""

            body = ET.SubElement(response_root, "BODY")
            ET.SubElement(body, "TRX_ID").text = transaction_id
            ET.SubElement(body, "LINE_ID").text = "SECCHARGE0100"

            ret = ET.SubElement(response_root, "RETURN")
            ET.SubElement(ret, "RETURNCODE").text = "0"
            ET.SubElement(ret, "RETURNMESSAGE").text = ""

            return ET.tostring(response_root, encoding="utf-8", short_empty_elements=False)
        except Exception:
            self.logging.error(f"create_reply_w0002_xml failed\n{self.logger.get_slim_error_log()}")

    def create_reply_w1001_xml(self, message_name, transaction_id, root):
        """Generates an xml response of w1001 command

        Args:
            message_name: Message name
            transaction_id: Transaction id

        Returns:
            Xml reply

        Example:
            >>> xml_socket_svr_obj = XMLSocketServer()
            >>> xml_socket_svr_obj.create_reply_w1001_xml(message_name, transaction_id)

        """
        try:
            # Create reply xml
            response_root = ET.Element("MESSAGE")
            header = ET.SubElement(response_root, "HEADER")
            ET.SubElement(header, "MESSAGENAME").text = message_name
            ET.SubElement(header, "TRANSACTIONID").text = transaction_id
            ET.SubElement(header, "REPLYSUBJECTNAME").text = ""
            ET.SubElement(header, "INBOXNAME").text = ""
            ET.SubElement(header, "LISTENER").text = ""

            body = ET.SubElement(response_root, "BODY")
            ET.SubElement(body, "TRX_ID").text = transaction_id
            ET.SubElement(body, "StoreHouseID").text = root.findtext(".//StoreHouseID")

            ret = ET.SubElement(response_root, "RETURN")
            ET.SubElement(ret, "RETURNCODE").text = "0"
            ET.SubElement(ret, "RETURNMESSAGE").text = ""

            return ET.tostring(response_root, encoding="utf-8", short_empty_elements=False)
        except Exception:
            self.logging.error(f"create_reply_w1001_xml failed\n{self.logger.get_slim_error_log()}")

    def create_request_w2002_xml_large(self, message_name, transaction_id, sh_id):
        """Generates an xml request of w2002 command

        Args:
            message_name: Message name
            transaction_id: Transaction id
            sh_id: Store house id

        Returns:
            Xml request

        Example:
            >>> xml_socket_svr_obj = XMLSocketServer()
            >>> xml_socket_svr_obj.create_request_w2002_xml(message_name, transaction_id, sh_id)

        """
        try:
            # Create request xml
            response_root = ET.Element("MESSAGE")
            header = ET.SubElement(response_root, "HEADER")
            ET.SubElement(header, "MESSAGENAME").text = message_name
            ET.SubElement(header, "TRANSACTIONID").text = transaction_id
            ET.SubElement(header, "REPLYSUBJECTNAME").text = ""
            ET.SubElement(header, "INBOXNAME").text = ""
            ET.SubElement(header, "LISTENER").text = ""

            body = ET.SubElement(response_root, "BODY")
            ET.SubElement(body, "TRX_ID").text = transaction_id
            ET.SubElement(body, "StoreHouseID").text = str(sh_id)
            ET.SubElement(body, "RecipeName").text = "P140-228-5"

            total_steps = 15
            recipe_info = ET.SubElement(body, "RecipeInfo")
            for step in range(1, total_steps + 1):
                recipe_step = ET.SubElement(recipe_info, "RecipeStep")
                ET.SubElement(recipe_step, "Step").text = str(step)
                if step == total_steps:
                    ET.SubElement(recipe_step, "Control_Mode").text = "END"
                elif step % 2 == 0:
                    ET.SubElement(recipe_step, "Control_Mode").text = "REST"
                else:

                    ET.SubElement(recipe_step, "Control_Mode").text = "START_CC"
                    
                ET.SubElement(recipe_step, "Cell_Max_Voltage").text = "4.18"
                ET.SubElement(recipe_step, "Cell_Min_Voltage").text = "0"
                ET.SubElement(recipe_step, "Cell_Delta_Voltage").text = "300"
                ET.SubElement(recipe_step, "Cell_Delta_Voltage_Time").text = "5"
                ET.SubElement(recipe_step, "Cell_Max_Current").text = "25"
                ET.SubElement(recipe_step, "Cell_Min_Current").text = "-1.5"
                ET.SubElement(recipe_step, "Cell_Protect_Temp").text = "45"
                ET.SubElement(recipe_step, "Cell_Delta_Temp").text = "3.2"
                ET.SubElement(recipe_step, "Cell_Delta_Temp_Time").text = "4"
                ET.SubElement(recipe_step, "Cell_Delta_Wire_Voltage").text = "1500"
                ET.SubElement(recipe_step, "Cell_Delta_Wire_Voltage_Time").text = "4"
                ET.SubElement(recipe_step, "Cell_Protect_Delay_Time").text = "60"
                ET.SubElement(recipe_step, "Cell_Setting_Time").text = "12600"
                ET.SubElement(recipe_step, "Wire_Max_Voltage").text = "1.5"
                ET.SubElement(recipe_step, "Cell_Delta_Wire_Delay").text = "1"
                ET.SubElement(recipe_step, "Cell_Delta_Temp_Delay").text = "1"
                ET.SubElement(recipe_step, "Cell_Protect_Temp_Water").text = "90.1"
                ET.SubElement(recipe_step, "Cell_Delta_Temp_Frame").text = "3"

            pallet_info = ET.SubElement(body, "PalletInfo")
            pallet = ET.SubElement(pallet_info, "Pallet")
            ET.SubElement(pallet, "PalletID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "L")].iloc[0]["PalletID"]
            ET.SubElement(pallet, "SerialBoardID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "L")].iloc[0]["SerialBoardID"]
            ET.SubElement(pallet, "DRCID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "L")].iloc[0]["ProtectBoardID"]
            ET.SubElement(pallet, "PalletPosition").text = "L"
            qrcode_list = ET.SubElement(pallet, "QRCodeList")
            qrcode = ET.SubElement(qrcode_list, "QRCode")
            ET.SubElement(qrcode, "Position").text = "1"
            ET.SubElement(qrcode, "QRCODEID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "L") & (self.meas_map["Position"] == 1)].iloc[0][
                "QRCODEID"
            ]
            qrcode = ET.SubElement(qrcode_list, "QRCode")
            ET.SubElement(qrcode, "Position").text = "2"
            ET.SubElement(qrcode, "QRCODEID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "L") & (self.meas_map["Position"] == 2)].iloc[0][
                "QRCODEID"
            ]
            qrcode = ET.SubElement(qrcode_list, "QRCode")
            ET.SubElement(qrcode, "Position").text = "3"
            ET.SubElement(qrcode, "QRCODEID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "L") & (self.meas_map["Position"] == 3)].iloc[0][
                "QRCODEID"
            ]
            qrcode = ET.SubElement(qrcode_list, "QRCode")
            ET.SubElement(qrcode, "Position").text = "4"
            ET.SubElement(qrcode, "QRCODEID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "L") & (self.meas_map["Position"] == 4)].iloc[0][
                "QRCODEID"
            ]
            qrcode = ET.SubElement(qrcode_list, "QRCode")
            ET.SubElement(qrcode, "Position").text = "5"
            ET.SubElement(qrcode, "QRCODEID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "L") & (self.meas_map["Position"] == 5)].iloc[0][
                "QRCODEID"
            ]
            qrcode = ET.SubElement(qrcode_list, "QRCode")
            ET.SubElement(qrcode, "Position").text = "6"
            ET.SubElement(qrcode, "QRCODEID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "L") & (self.meas_map["Position"] == 6)].iloc[0][
                "QRCODEID"
            ]

            pallet = ET.SubElement(pallet_info, "Pallet")
            ET.SubElement(pallet, "PalletID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "R")].iloc[0]["PalletID"]
            ET.SubElement(pallet, "SerialBoardID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "R")].iloc[0]["SerialBoardID"]
            ET.SubElement(pallet, "DRCID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "R")].iloc[0]["ProtectBoardID"]
            ET.SubElement(pallet, "PalletPosition").text = "R"

            qrcode_list = ET.SubElement(pallet, "QRCodeList")
            qrcode = ET.SubElement(qrcode_list, "QRCode")
            ET.SubElement(qrcode, "Position").text = "1"
            ET.SubElement(qrcode, "QRCODEID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "R") & (self.meas_map["Position"] == 1)].iloc[0][
                "QRCODEID"
            ]
            qrcode = ET.SubElement(qrcode_list, "QRCode")
            ET.SubElement(qrcode, "Position").text = "2"
            ET.SubElement(qrcode, "QRCODEID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "R") & (self.meas_map["Position"] == 2)].iloc[0][
                "QRCODEID"
            ]
            qrcode = ET.SubElement(qrcode_list, "QRCode")
            ET.SubElement(qrcode, "Position").text = "3"
            ET.SubElement(qrcode, "QRCODEID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "R") & (self.meas_map["Position"] == 3)].iloc[0][
                "QRCODEID"
            ]
            qrcode = ET.SubElement(qrcode_list, "QRCode")
            ET.SubElement(qrcode, "Position").text = "4"
            ET.SubElement(qrcode, "QRCODEID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "R") & (self.meas_map["Position"] == 4)].iloc[0][
                "QRCODEID"
            ]
            qrcode = ET.SubElement(qrcode_list, "QRCode")
            ET.SubElement(qrcode, "Position").text = "5"
            ET.SubElement(qrcode, "QRCODEID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "R") & (self.meas_map["Position"] == 5)].iloc[0][
                "QRCODEID"
            ]
            qrcode = ET.SubElement(qrcode_list, "QRCode")
            ET.SubElement(qrcode, "Position").text = "6"
            ET.SubElement(qrcode, "QRCODEID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "R") & (self.meas_map["Position"] == 6)].iloc[0][
                "QRCODEID"
            ]

            ret = ET.SubElement(response_root, "RETURN")
            ET.SubElement(ret, "RETURNCODE").text = "0"
            ET.SubElement(ret, "RETURNMESSAGE").text = ""

            return ET.tostring(response_root, encoding="utf-8", short_empty_elements=False)
        except Exception:
            self.logging.error(f"create_reply_w1001_xml failed\n{self.logger.get_slim_error_log()}")

    def create_request_w2002_xml(self, message_name, transaction_id, trx_id, sh_id):
        """Generates an xml request of w2002 command

        Args:
            message_name: Message name
            transaction_id: Transaction id
            sh_id: Store house id

        Returns:
            Xml request

        Example:
            >>> xml_socket_svr_obj = XMLSocketServer()
            >>> xml_socket_svr_obj.create_request_w2002_xml(message_name, transaction_id, sh_id)

        """
        try:
            # Create request xml
            response_root = ET.Element("MESSAGE")
            header = ET.SubElement(response_root, "HEADER")
            ET.SubElement(header, "MESSAGENAME").text = message_name
            ET.SubElement(header, "TRANSACTIONID").text = transaction_id
            ET.SubElement(header, "REPLYSUBJECTNAME").text = ""
            ET.SubElement(header, "INBOXNAME").text = ""
            ET.SubElement(header, "LISTENER").text = ""

            body = ET.SubElement(response_root, "BODY")
            ET.SubElement(body, "TRX_ID").text = trx_id
            ET.SubElement(body, "StoreHouseID").text = str(sh_id)
            ET.SubElement(body, "RecipeName").text = "P140-228-5"

            recipe_info = ET.SubElement(body, "RecipeInfo")
            recipe_step = ET.SubElement(recipe_info, "RecipeStep")
            ET.SubElement(recipe_step, "Step").text = "1"
            ET.SubElement(recipe_step, "Control_Mode").text = "START_CC"
            ET.SubElement(recipe_step, "Cell_Max_Voltage").text = "4.18"
            ET.SubElement(recipe_step, "Cell_Min_Voltage").text = "0"
            ET.SubElement(recipe_step, "Cell_Delta_Voltage").text = "300"
            ET.SubElement(recipe_step, "Cell_Delta_Voltage_Time").text = "5"
            ET.SubElement(recipe_step, "Cell_Max_Current").text = "25"
            ET.SubElement(recipe_step, "Cell_Min_Current").text = "-1.5"
            ET.SubElement(recipe_step, "Cell_Protect_Temp").text = "45"
            ET.SubElement(recipe_step, "Cell_Delta_Temp").text = "3.2"
            ET.SubElement(recipe_step, "Cell_Delta_Temp_Time").text = "4"
            ET.SubElement(recipe_step, "Cell_Delta_Wire_Voltage").text = "1500"
            ET.SubElement(recipe_step, "Cell_Delta_Wire_Voltage_Time").text = "4"
            ET.SubElement(recipe_step, "Cell_Protect_Delay_Time").text = "60"
            ET.SubElement(recipe_step, "Cell_Setting_Time").text = "210"
            ET.SubElement(recipe_step, "Wire_Max_Voltage").text = "1.5"
            ET.SubElement(recipe_step, "Cell_Delta_Wire_Delay").text = "1"
            ET.SubElement(recipe_step, "Cell_Delta_Temp_Delay").text = "1"
            ET.SubElement(recipe_step, "Cell_Protect_Temp_Water").text = "90.1"
            ET.SubElement(recipe_step, "Cell_Delta_Temp_Frame").text = "3"

            recipe_step = ET.SubElement(recipe_info, "RecipeStep")
            ET.SubElement(recipe_step, "Step").text = "2"
            ET.SubElement(recipe_step, "Control_Mode").text = "REST"
            ET.SubElement(recipe_step, "Cell_Max_Voltage").text = "4.18"
            ET.SubElement(recipe_step, "Cell_Min_Voltage").text = "0"
            ET.SubElement(recipe_step, "Cell_Delta_Voltage").text = "300"
            ET.SubElement(recipe_step, "Cell_Delta_Voltage_Time").text = "5"
            ET.SubElement(recipe_step, "Cell_Max_Current").text = "25"
            ET.SubElement(recipe_step, "Cell_Min_Current").text = "-1.5"
            ET.SubElement(recipe_step, "Cell_Protect_Temp").text = "45"
            ET.SubElement(recipe_step, "Cell_Delta_Temp").text = "3.2"
            ET.SubElement(recipe_step, "Cell_Delta_Temp_Time").text = "4"
            ET.SubElement(recipe_step, "Cell_Delta_Wire_Voltage").text = "1500"
            ET.SubElement(recipe_step, "Cell_Delta_Wire_Voltage_Time").text = "4"
            ET.SubElement(recipe_step, "Cell_Protect_Delay_Time").text = "60"
            ET.SubElement(recipe_step, "Cell_Setting_Time").text = "210"
            ET.SubElement(recipe_step, "Wire_Max_Voltage").text = "1.5"
            ET.SubElement(recipe_step, "Cell_Delta_Wire_Delay").text = "1"
            ET.SubElement(recipe_step, "Cell_Delta_Temp_Delay").text = "1"
            ET.SubElement(recipe_step, "Cell_Protect_Temp_Water").text = "90.1"
            ET.SubElement(recipe_step, "Cell_Delta_Temp_Frame").text = "3"

            recipe_step = ET.SubElement(recipe_info, "RecipeStep")
            ET.SubElement(recipe_step, "Step").text = "3"
            ET.SubElement(recipe_step, "Control_Mode").text = "END"
            ET.SubElement(recipe_step, "Cell_Max_Voltage").text = "4.18"
            ET.SubElement(recipe_step, "Cell_Min_Voltage").text = "0"
            ET.SubElement(recipe_step, "Cell_Delta_Voltage").text = "300"
            ET.SubElement(recipe_step, "Cell_Delta_Voltage_Time").text = "5"
            ET.SubElement(recipe_step, "Cell_Max_Current").text = "25"
            ET.SubElement(recipe_step, "Cell_Min_Current").text = "-1.5"
            ET.SubElement(recipe_step, "Cell_Protect_Temp").text = "45"
            ET.SubElement(recipe_step, "Cell_Delta_Temp").text = "3.2"
            ET.SubElement(recipe_step, "Cell_Delta_Temp_Time").text = "4"
            ET.SubElement(recipe_step, "Cell_Delta_Wire_Voltage").text = "1500"
            ET.SubElement(recipe_step, "Cell_Delta_Wire_Voltage_Time").text = "4"
            ET.SubElement(recipe_step, "Cell_Protect_Delay_Time").text = "60"
            ET.SubElement(recipe_step, "Cell_Setting_Time").text = "210"
            ET.SubElement(recipe_step, "Wire_Max_Voltage").text = "1.5"
            ET.SubElement(recipe_step, "Cell_Delta_Wire_Delay").text = "1"
            ET.SubElement(recipe_step, "Cell_Delta_Temp_Delay").text = "1"
            ET.SubElement(recipe_step, "Cell_Protect_Temp_Water").text = "90.1"
            ET.SubElement(recipe_step, "Cell_Delta_Temp_Frame").text = "3"

            pallet_info = ET.SubElement(body, "PalletInfo")
            pallet = ET.SubElement(pallet_info, "Pallet")
            ET.SubElement(pallet, "PalletID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "L")].iloc[0]["PalletID"]
            ET.SubElement(pallet, "SerialBoardID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "L")].iloc[0]["SerialBoardID"]
            ET.SubElement(pallet, "DRCID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "L")].iloc[0]["ProtectBoardID"]
            ET.SubElement(pallet, "PalletPosition").text = "L"
            qrcode_list = ET.SubElement(pallet, "QRCodeList")
            qrcode = ET.SubElement(qrcode_list, "QRCode")
            ET.SubElement(qrcode, "Position").text = "1"
            ET.SubElement(qrcode, "QRCODEID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "L") & (self.meas_map["Position"] == 1)].iloc[0][
                "QRCODEID"
            ]
            qrcode = ET.SubElement(qrcode_list, "QRCode")
            ET.SubElement(qrcode, "Position").text = "2"
            ET.SubElement(qrcode, "QRCODEID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "L") & (self.meas_map["Position"] == 2)].iloc[0][
                "QRCODEID"
            ]
            qrcode = ET.SubElement(qrcode_list, "QRCode")
            ET.SubElement(qrcode, "Position").text = "3"
            ET.SubElement(qrcode, "QRCODEID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "L") & (self.meas_map["Position"] == 3)].iloc[0][
                "QRCODEID"
            ]
            qrcode = ET.SubElement(qrcode_list, "QRCode")
            ET.SubElement(qrcode, "Position").text = "4"
            ET.SubElement(qrcode, "QRCODEID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "L") & (self.meas_map["Position"] == 4)].iloc[0][
                "QRCODEID"
            ]
            qrcode = ET.SubElement(qrcode_list, "QRCode")
            ET.SubElement(qrcode, "Position").text = "5"
            ET.SubElement(qrcode, "QRCODEID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "L") & (self.meas_map["Position"] == 5)].iloc[0][
                "QRCODEID"
            ]
            qrcode = ET.SubElement(qrcode_list, "QRCode")
            ET.SubElement(qrcode, "Position").text = "6"
            ET.SubElement(qrcode, "QRCODEID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "L") & (self.meas_map["Position"] == 6)].iloc[0][
                "QRCODEID"
            ]

            pallet = ET.SubElement(pallet_info, "Pallet")
            ET.SubElement(pallet, "PalletID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "R")].iloc[0]["PalletID"]
            ET.SubElement(pallet, "SerialBoardID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "R")].iloc[0]["SerialBoardID"]
            ET.SubElement(pallet, "DRCID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "R")].iloc[0]["ProtectBoardID"]
            ET.SubElement(pallet, "PalletPosition").text = "R"

            qrcode_list = ET.SubElement(pallet, "QRCodeList")
            qrcode = ET.SubElement(qrcode_list, "QRCode")
            ET.SubElement(qrcode, "Position").text = "1"
            ET.SubElement(qrcode, "QRCODEID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "R") & (self.meas_map["Position"] == 1)].iloc[0][
                "QRCODEID"
            ]
            qrcode = ET.SubElement(qrcode_list, "QRCode")
            ET.SubElement(qrcode, "Position").text = "2"
            ET.SubElement(qrcode, "QRCODEID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "R") & (self.meas_map["Position"] == 2)].iloc[0][
                "QRCODEID"
            ]
            qrcode = ET.SubElement(qrcode_list, "QRCode")
            ET.SubElement(qrcode, "Position").text = "3"
            ET.SubElement(qrcode, "QRCODEID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "R") & (self.meas_map["Position"] == 3)].iloc[0][
                "QRCODEID"
            ]
            qrcode = ET.SubElement(qrcode_list, "QRCode")
            ET.SubElement(qrcode, "Position").text = "4"
            ET.SubElement(qrcode, "QRCODEID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "R") & (self.meas_map["Position"] == 4)].iloc[0][
                "QRCODEID"
            ]
            qrcode = ET.SubElement(qrcode_list, "QRCode")
            ET.SubElement(qrcode, "Position").text = "5"
            ET.SubElement(qrcode, "QRCODEID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "R") & (self.meas_map["Position"] == 5)].iloc[0][
                "QRCODEID"
            ]
            qrcode = ET.SubElement(qrcode_list, "QRCode")
            ET.SubElement(qrcode, "Position").text = "6"
            ET.SubElement(qrcode, "QRCODEID").text = self.meas_map[(self.meas_map["StoreHouseID"] == sh_id) & (self.meas_map["PalletPosition"] == "R") & (self.meas_map["Position"] == 6)].iloc[0][
                "QRCODEID"
            ]

            ret = ET.SubElement(response_root, "RETURN")
            ET.SubElement(ret, "RETURNCODE").text = "0"
            ET.SubElement(ret, "RETURNMESSAGE").text = ""

            return ET.tostring(response_root, encoding="utf-8", short_empty_elements=False)
        except Exception:
            self.logging.error(f"create_reply_w1001_xml failed\n{self.logger.get_slim_error_log()}")

    def create_request_w2003_xml(self, message_name, transaction_id, trx_id, sh_id, step):
        """Generates an xml request of w2003 command

        Args:
            message_name: Message name
            transaction_id: Transaction id
            sh_id: Store house id
            step: Charging step

        Returns:
            Xml request

        Example:
            >>> xml_socket_svr_obj = XMLSocketServer()
            >>> xml_socket_svr_obj.create_request_w2003_xml(message_name, transaction_id, sh_id, step)

        """
        try:
            # Create request xml
            response_root = ET.Element("MESSAGE")
            header = ET.SubElement(response_root, "HEADER")
            ET.SubElement(header, "MESSAGENAME").text = message_name
            ET.SubElement(header, "TRANSACTIONID").text = transaction_id
            ET.SubElement(header, "REPLYSUBJECTNAME").text = ""
            ET.SubElement(header, "INBOXNAME").text = ""
            ET.SubElement(header, "LISTENER").text = ""

            body = ET.SubElement(response_root, "BODY")
            ET.SubElement(body, "TRX_ID").text = trx_id
            ET.SubElement(body, "StoreHouseID").text = str(sh_id)
            ET.SubElement(body, "STEP").text = str(step)

            ret = ET.SubElement(response_root, "RETURN")
            ET.SubElement(ret, "RETURNCODE").text = "0"
            ET.SubElement(ret, "RETURNMESSAGE").text = ""

            return ET.tostring(response_root, encoding="utf-8", short_empty_elements=False)
        except Exception:
            self.logging.error(f"create_request_w2003_xml failed\n{self.logger.get_slim_error_log()}")

    def create_request_w2004_xml(self, message_name, transaction_id, trx_id, sh_id):
        """Generates an xml request of w2004 command

        Args:
            message_name: Message name
            transaction_id: Transaction id
            sh_id: Store house id

        Returns:
            Xml request

        Example:
            >>> xml_socket_svr_obj = XMLSocketServer()
            >>> xml_socket_svr_obj.create_request_w2004_xml(message_name, transaction_id, sh_id)

        """
        try:
            # Create request xml
            response_root = ET.Element("MESSAGE")
            header = ET.SubElement(response_root, "HEADER")
            ET.SubElement(header, "MESSAGENAME").text = message_name
            ET.SubElement(header, "TRANSACTIONID").text = transaction_id
            ET.SubElement(header, "REPLYSUBJECTNAME").text = ""
            ET.SubElement(header, "INBOXNAME").text = ""
            ET.SubElement(header, "LISTENER").text = ""

            body = ET.SubElement(response_root, "BODY")
            ET.SubElement(body, "TRX_ID").text = trx_id
            ET.SubElement(body, "LINE_ID").text = "SECCHARGE0100"
            ET.SubElement(body, "StoreHouseID").text = str(sh_id)

            ret = ET.SubElement(response_root, "RETURN")
            ET.SubElement(ret, "RETURNCODE").text = "0"
            ET.SubElement(ret, "RETURNMESSAGE").text = ""

            return ET.tostring(response_root, encoding="utf-8", short_empty_elements=False)
        except Exception:
            self.logging.error(f"create_request_w2004_xml failed\n{self.logger.get_slim_error_log()}")

    def create_request_w2005_xml(self, message_name, transaction_id, trx_id, sh_id):
        """Generates an xml request of w2005 command

        Args:
            message_name: Message name
            transaction_id: Transaction id
            sh_id: Store house id

        Returns:
            Xml request

        Example:
            >>> xml_socket_svr_obj = XMLSocketServer()
            >>> xml_socket_svr_obj.create_request_w2005_xml(message_name, transaction_id, sh_id)

        """
        try:
            # Create request xml
            response_root = ET.Element("MESSAGE")
            header = ET.SubElement(response_root, "HEADER")
            ET.SubElement(header, "MESSAGENAME").text = message_name
            ET.SubElement(header, "TRANSACTIONID").text = transaction_id
            ET.SubElement(header, "REPLYSUBJECTNAME").text = ""
            ET.SubElement(header, "INBOXNAME").text = ""
            ET.SubElement(header, "LISTENER").text = ""

            body = ET.SubElement(response_root, "BODY")
            ET.SubElement(body, "TRX_ID").text = trx_id
            ET.SubElement(body, "LINE_ID").text = "SECCHARGE0100"
            ET.SubElement(body, "StoreHouseID").text = str(sh_id)

            ret = ET.SubElement(response_root, "RETURN")
            ET.SubElement(ret, "RETURNCODE").text = "0"
            ET.SubElement(ret, "RETURNMESSAGE").text = ""

            return ET.tostring(response_root, encoding="utf-8", short_empty_elements=False)
        except Exception:
            self.logging.error(f"create_request_w2005_xml failed\n{self.logger.get_slim_error_log()}")

    def parse_and_handle_messages(self, xml_string, notified_socket):
        """Processing received messages

        Args:
            xml_string: Parsing xml
            notified_socket: Socket object

        Returns:
            None

        Example:
            >>> xml_socket_svr_obj = XMLSocketServer()
            >>> xml_socket_svr_obj.parse_and_handle_messages(xml_string, notified_socket)

        """
        try:
            # self.logging.info(f"{minidom.parseString(xml_string).toprettyxml(indent="    ")}")
            root = ET.fromstring(xml_string)  # noqa: S314
            message_name = root.find("./HEADER/MESSAGENAME").text
            rsp_message = root.find("./HEADER/MESSAGENAME").text.replace("Request", "Reply")
            transaction_id = root.find("./HEADER/TRANSACTIONID").text
            trx_id = root.find("./BODY/TRX_ID").text
            # storehouse_id = root.findtext(".//StoreHouseID")
            # if storehouse_id:
            #     self.logging.info(f"{message_name} | shid: {storehouse_id}")

            # self.logging.debug(f"{minidom.parseString(xml_string).toprettyxml(indent='    ')}")  # noqa: S318
            storehouse_id = root.findtext("./BODY/StoreHouseID")
            return_code = root.findtext("./RETURN/RETURNCODE")
            return_msg = root.findtext("./RETURN/RETURNMESSAGE")
            code_map = {
                0: "OK",
                1: "NG",
                2: "Water",
                3: "NoResponse"
            }
            return_code = (
                code_map.get(int(return_code), "Unknown")
                if return_code and return_code.strip()
                else None
            )
            reply_map = {
                "SyncTimeRequest": "[Recv] W0001Req",
                "SyncTimeReply": "[Send] W0001Rsp",
                "AliveRequest": "[Recv] W0002Req",
                "AliveReply": "[Send] W0002Rsp",
                "AlarmStopReport": "[Recv] W1001Rsp",
                "StoreHouseStatusRequest": "[Send] W2002Req",
                "StoreHouseStatusReply": "[Recv] W2002Rsp",
                "StoreHouseStepCheckRequest": "[Send] W2003Req",
                "StoreHouseStepCheckReply": "[Recv] W2003Rsp",
                "StoreHouseNGCheckRequest": "[Send] W2004Req",
                "StoreHouseNGCheckReply": "[Recv] W2004Rsp",
                "JudgmentCompletionNotificationRequest": "[Send] W2005Req",
                "JudgmentCompletionNotificationReply": "[Recv] W2005Rsp"
            }
            msg_code = (
                reply_map.get(message_name, "Unknown")
                if message_name and message_name.strip()
                else None
            )
            self.logging.debug(f"{msg_code} | {return_code} | {storehouse_id} | {transaction_id[-6:]} | {trx_id[-6:]} | {return_msg}")
            
            request_xml = b""
            # Create and send reply
            if message_name == "SyncTimeRequest":
                request_xml = self.create_reply_w0001_xml(rsp_message, transaction_id)
            elif message_name == "AliveRequest":
                request_xml = self.create_reply_w0002_xml(rsp_message, transaction_id)
            if request_xml:
                notified_socket.sendall(struct.pack(">I", len(request_xml)) + request_xml)
        except ET.ParseError:
            self.logging.error(f"parse_and_handle_messages failed\n{self.logger.get_slim_error_log()}")

    def handle_power(self):
        """Keyboard input handler

        Args:
            None

        Returns:
            None

        Example:
            >>> xml_socket_svr_obj = XMLSocketServer()
            >>> xml_socket_svr_obj.handle_power()

        """
        try:
            while True:
                user_input = input()
                if user_input.lower() == "exit":
                    self.logging.info("Exiting command mode...")
                    break
                else:
                    if user_input == "con":
                        self.power_control(0, "Power ON Device")
                    if user_input == "on":
                        self.power_control(0, "Battery Entry")
                    elif user_input == "off":
                        self.power_control(0, "Battery Exit")
                    elif user_input == "cc":
                        self.power_control(30, "Battery CCC")
                    elif user_input == "rest":
                        self.power_control(30, "Battery REST")
        except ET.ParseError:
            self.logging.error(f"handle_power failed\n{self.logger.get_slim_error_log()}")

    def handle_input_cmd(self):
        """input command handler

        Args:
            None

        Returns:
            None

        Example:
            >>> xml_socket_svr_obj = XMLSocketServer()
            >>> xml_socket_svr_obj.handle_input_cmd()

        """
        try:
            while True:
                user_input = input().lower()
                if user_input.lower() == "exit":
                    break
                if not user_input:
                    continue
                else:
                    inputs = user_input.split()
                    if len(inputs) == 4:
                        self.cmd = inputs[0]
                        try:
                            self.cmd_start_idx = int(inputs[1])
                            self.cmd_end_idx = int(inputs[2])
                            self.cmd_step = int(inputs[3])
                        except ValueError:
                            self.logging.info(
                                """Usage:
                                Command [Start_StoreHouse_ID] [End_StoreHouse_ID] [Charging Step]
                                Examples:
                                > w2002 1 1 0...Set StoreHouseID 1 Config
                                > w2003 1 1 1...Change to Charging Step 1
                                > w2003 1 1 2...Change to Charging Step 2
                                > w2003 1 1 3...Change to Charging Step 3
                                > w2004 1 1 0...Get StoreHouseID 1 Status
                                > w2005 1 1 0...Stop StoreHouseID 1\n
                                > w2002 1 10 0...Set StoreHouseID 1-10 Config
                                > w2003 1 10 1...Change to Charging Step 1
                                > w2003 1 10 2...Change to Charging Step 2
                                > w2003 1 10 3...Change to Charging Step 3
                                > w2004 1 10 0...Get StoreHouseID 1-10 Status
                                > w2005 1 10 0...Stop StoreHouseID 1-10"""
                            )
                            continue
                    else:
                        if len(inputs) > 0:
                            if inputs[0] == "con":
                                self.power_control(0, "Power ON Device")
                            elif inputs[0] == "on":
                                self.power_control(0, "Battery Entry")
                            elif inputs[0] == "off":
                                self.power_control(0, "Battery Exit")
                            elif inputs[0] == "cc":
                                self.power_control(30, "Battery CCC")
                            elif inputs[0] == "rest":
                                self.power_control(30, "Battery REST")
        except ET.ParseError:
            self.logging.error(f"handle_keyboard_input failed\n{self.logger.get_slim_error_log()}")

    def handle_keyboard_input(self):
        """Keyboard input handler

        Args:
            None

        Returns:
            None

        Example:
            >>> xml_socket_svr_obj = XMLSocketServer()
            >>> xml_socket_svr_obj.handle_keyboard_input()

        """
        try:
            while True:
                user_input = input()
                if user_input.lower() == "exit":
                    self.logging.info("Exiting command mode...")
                    break
                else:
                    inputs = user_input.split()
                    if len(inputs) == 4:
                        self.cmd = inputs[0]
                        try:
                            self.cmd_start_idx = int(inputs[1])
                            self.cmd_end_idx = int(inputs[2])
                            self.cmd_step = int(inputs[3])
                        except ValueError:
                            self.logging.info(
                                """Usage:
                                Command [Start_StoreHouse_ID] [End_StoreHouse_ID] [Charging Step]
                                Examples:
                                > w2002 1 1 0...Set StoreHouseID 1 Config
                                > w2003 1 1 1...Change to Charging Step 1
                                > w2003 1 1 2...Change to Charging Step 2
                                > w2003 1 1 3...Change to Charging Step 3
                                > w2004 1 1 0...Get StoreHouseID 1 Status
                                > w2005 1 1 0...Stop StoreHouseID 1\n
                                > w2002 1 10 0...Set StoreHouseID 1-10 Config
                                > w2003 1 10 1...Change to Charging Step 1
                                > w2003 1 10 2...Change to Charging Step 2
                                > w2003 1 10 3...Change to Charging Step 3
                                > w2004 1 10 0...Get StoreHouseID 1-10 Status
                                > w2005 1 10 0...Stop StoreHouseID 1-10"""
                            )
                            continue
                    else:
                        self.logging.info(
                            """Usage:
                            Command [Start_StoreHouse_ID] [End_StoreHouse_ID] [Charging Step]
                            Examples:
                            > w2002 1 1 0...Set StoreHouseID 1 Config
                            > w2003 1 1 1...Change to Charging Step 1
                            > w2003 1 1 2...Change to Charging Step 2
                            > w2003 1 1 3...Change to Charging Step 3
                            > w2004 1 1 0...Get StoreHouseID 1 Status
                            > w2005 1 1 0...Stop StoreHouseID 1\n
                            > w2002 1 10 0...Set StoreHouseID 1-10 Config
                            > w2003 1 10 1...Change to Charging Step 1
                            > w2003 1 10 2...Change to Charging Step 2
                            > w2003 1 10 3...Change to Charging Step 3
                            > w2004 1 10 0...Get StoreHouseID 1-10 Status
                            > w2005 1 10 0...Stop StoreHouseID 1-10"""
                        )
        except ET.ParseError:
            self.logging.error(f"handle_keyboard_input failed\n{self.logger.get_slim_error_log()}")

    def format_duration(self, seconds):
        """Convert total seconds to days, hours, minutes, seconds format."""
        return str(timedelta(seconds=int(seconds)))

    def handle_autotest_script_large(self):
        """Autotest script handler

        Args:
            None

        Returns:
            None

        Example:
            >>> xml_socket_svr_obj = XMLSocketServer()
            >>> xml_socket_svr_obj.handle_autotest_script()

        """
        try:
            round = 0
            start_time = time.time()
            uri = "PYRO:battery_mode_api@192.168.127.99:8888"
            self.ccs_api = RemoteBatteryModeAPI(uri)
            res = self.ccs_api.connect_devices()
            self.influxdb_obj.write_log_influxdb("DEBUG", f"Device Connection {res}", "system")
            self.logging.debug(f"{res}")
            while True:
                try:
                    self.influxdb_obj.write_log_influxdb("DEBUG", "CCS Battery Entry", "system")
                    self.logging.info("Battery Entry")
                    self.ccs_api.mode_battery_entry()
                    time.sleep(20)
                    self.influxdb_obj.write_log_influxdb("DEBUG", "CCS w2002 2 2 0", "system")
                    self.logging.info("w2002 2 2 0")
                    self.cmd = "w2002"
                    self.cmd_start_idx = 2
                    self.cmd_end_idx = 2
                    self.cmd_step = 0
                    time.sleep(10)
                    self.influxdb_obj.write_log_influxdb("DEBUG", "CCS w2002 5 8 0", "system")
                    self.logging.info("w2002 5 8 0")
                    self.cmd = "w2002"
                    self.cmd_start_idx = 5
                    self.cmd_end_idx = 8
                    self.cmd_step = 0
                    time.sleep(15)

                    for idx in range(1, 15):
                        if idx % 2 == 1:
                            self.influxdb_obj.write_log_influxdb("DEBUG", f"CCS w2003 2 2 {idx}", "system")
                            self.logging.info(f"w2003 2 2 {idx}")
                            self.cmd = "w2003"
                            self.cmd_start_idx = 2
                            self.cmd_end_idx = 2
                            self.cmd_step = idx
                            time.sleep(5)

                            self.influxdb_obj.write_log_influxdb("DEBUG", f"CCS w2003 5 8 {idx}", "system")
                            self.logging.info(f"w2003 5 8 {idx}")
                            self.cmd = "w2003"
                            self.cmd_start_idx = 5
                            self.cmd_end_idx = 8
                            self.cmd_step = idx

                            self.influxdb_obj.write_log_influxdb("DEBUG", "CCS Battery CCC", "system")
                            self.logging.info("Battery CCC")
                            self.ccs_api.mode_cc_charge(7)
                            time.sleep(12)

                        else:
                            self.influxdb_obj.write_log_influxdb("DEBUG", f"CCS w2003 2 2 {idx}", "system")
                            self.logging.info(f"w2003 2 2 {idx}")
                            self.cmd = "w2003"
                            self.cmd_start_idx = 2
                            self.cmd_end_idx = 2
                            self.cmd_step = idx
                            time.sleep(5)

                            self.influxdb_obj.write_log_influxdb("DEBUG", f"CCS w2003 5 8 {idx}", "system")
                            self.logging.info(f"w2003 5 8 {idx}")
                            self.cmd = "w2003"
                            self.cmd_start_idx = 5
                            self.cmd_end_idx = 8
                            self.cmd_step = idx

                            self.influxdb_obj.write_log_influxdb("DEBUG", "CCS Battery REST", "system")
                            self.logging.info("Battery REST")
                            self.ccs_api.mode_idle(7)
                            time.sleep(12)

                    self.influxdb_obj.write_log_influxdb("DEBUG", "CCS w2003 2 2 3", "system")
                    self.logging.info("w2003 2 2 15")
                    self.cmd = "w2003"
                    self.cmd_start_idx = 2
                    self.cmd_end_idx = 2
                    self.cmd_step = 15
                    time.sleep(10)
                    self.influxdb_obj.write_log_influxdb("DEBUG", "CCS w2003 5 8 3", "system")
                    self.logging.info("w2003 5 8 15")
                    self.cmd = "w2003"
                    self.cmd_start_idx = 5
                    self.cmd_end_idx = 8
                    self.cmd_step = 15
                    time.sleep(10)
                    self.influxdb_obj.write_log_influxdb("DEBUG", "CCS Battery Exit", "system")
                    self.logging.info("Battery Exit")
                    self.ccs_api.mode_battery_exit()
                    time.sleep(30)
                    round += 1
                    formatted_time = self.format_duration(time.time() - start_time)
                    self.influxdb_obj.write_log_influxdb("DEBUG", f"CCS Round {round} - {formatted_time}", "system")
                    self.logging.info(f"Round {round} - {formatted_time}")
                except Exception:
                    self.logging.info(
                        """Usage:
                        Command [Start_StoreHouse_ID] [End_StoreHouse_ID] [Charging Step]
                        Examples:
                        > w2002 1 1 0...Set StoreHouseID 1 Config
                        > w2003 1 1 1...Change to Charging Step 1
                        > w2003 1 1 2...Change to Charging Step 2
                        > w2003 1 1 3...Change to Charging Step 3
                        > w2004 1 1 0...Get StoreHouseID 1 Status
                        > w2005 1 1 0...Stop StoreHouseID 1\n
                        > w2002 1 10 0...Set StoreHouseID 1-10 Config
                        > w2003 1 10 1...Change to Charging Step 1
                        > w2003 1 10 2...Change to Charging Step 2
                        > w2003 1 10 3...Change to Charging Step 3
                        > w2004 1 10 0...Get StoreHouseID 1-10 Status
                        > w2005 1 10 0...Stop StoreHouseID 1-10"""
                    )
        except Exception:
            self.logging.error(f"handle_autotest_script failed\n{self.logger.get_slim_error_log()}")

    def test_rack(self, start_idx, end_idx, step, cmd, sleep_time):
        self.influxdb_obj.write_log_influxdb("DEBUG", f"CCS {cmd} {start_idx} {end_idx} {step}", "system")
        self.logging.info(f"{cmd} {start_idx} {end_idx} {step}")
        self.cmd = cmd
        self.cmd_start_idx = start_idx
        self.cmd_end_idx = end_idx
        self.cmd_step = step
        time.sleep(sleep_time)

        self.influxdb_obj.write_log_influxdb("DEBUG", f"CCS {cmd}", "system")
        
    def check_power_on(self):
        if not self.ccs_api:
            self.power_control(0, "Power ON Device")

    def power_control(self, process_time, msg):
        self.logging.info(f"{msg}")

        if msg == "Power ON Device":
            uri = "PYRO:battery_mode_api@192.168.127.99:8888"
            self.ccs_api = RemoteBatteryModeAPI(uri)
            self.logging.info(f"Connect Device: {self.ccs_api.connect_devices()}")
        else:
            self.check_power_on()
            if msg == "Battery Entry":
                self.ccs_api.mode_battery_entry()
                time.sleep(process_time)
            elif msg == "Battery CCC":
                self.ccs_api.mode_cc_charge(math.ceil(process_time / 2.8))
                time.sleep(process_time)
            elif msg == "Battery REST":
                self.ccs_api.mode_idle(math.ceil(process_time / 2.8))
                time.sleep(process_time)
            elif msg == "Battery Exit":
                self.ccs_api.mode_battery_exit()
                time.sleep(process_time)

    def check_task_status(self, mssql_obj, task_names: list[str]):
        """
        Query Task table for a given task_name and check active/is_delete

        Args:
            mssql_obj: mssql object
            task_name: find task_name list
        """
        try:
            task_names_str = ",".join(f"'{t}'" for t in task_names)
            query = f"""
            WITH last_row AS (
                SELECT
                    [task_name],
                    [active],
                    [is_delete],
                    [datetime],
                    ROW_NUMBER() OVER (
                        PARTITION BY [task_name]
                        ORDER BY [datetime] DESC, [task_id] DESC
                    ) AS rn
                FROM [BALPS].[dbo].[Task]
                WHERE [task_name] IN ({task_names_str})
            )
            SELECT [task_name], [active], [is_delete], [datetime]
            FROM last_row
            WHERE rn = 1
            ORDER BY [task_name];
            """
            df = mssql_obj.query_db_pd(query)

            if df.empty:
                return True

            for _, row in df.iterrows():
                if not row["active"] and row["is_delete"]:
                    return True
                else:
                    return False

        except Exception:
            self.logging.error(f"check_task_status failed\n{self.logger.get_slim_error_log()}")


    def handle_autotest_script_demo(self):
        """Autotest script demo handler

        Args:
            None

        Returns:
            None

        Example:
            >>> xml_socket_svr_obj = XMLSocketServer()
            >>> xml_socket_svr_obj.handle_autotest_script_demo()

        """
        try:
            round = 0
            start_time = time.time()
            while True:
                try:
                    # if self.check_task_status(self.mssql_obj, ["A1312", "A0158"]):
                    if self.check_task_status(self.mssql_obj, ["A0005", "A0006"]):
                        self.influxdb_obj.write_log_influxdb("DEBUG", "CCS w2002 A1315 and A0155", "system")
                        self.logging.info("w2002 1 84 0")
                        self.cmd = "w2002"
                        self.cmd_start_idx = 3
                        self.cmd_end_idx = 3
                        self.cmd_step = 0
                        time.sleep(10)

                        round += 1
                        formatted_time = self.format_duration(time.time() - start_time)
                        msg_log = f"CCS Round {round} - {formatted_time}"
                        self.influxdb_obj.write_log_influxdb("DEBUG", msg_log, "system")
                        self.logging.info(msg_log)
                except Exception:
                    self.logging.error(
                        """Usage:
                        Command [Start_StoreHouse_ID] [End_StoreHouse_ID] [Charging Step]
                        Examples:
                        > w2002 1 1 0...Set StoreHouseID 1 Config
                        > w2003 1 1 1...Change to Charging Step 1
                        > w2003 1 1 2...Change to Charging Step 2
                        > w2003 1 1 3...Change to Charging Step 3
                        > w2004 1 1 0...Get StoreHouseID 1 Status
                        > w2005 1 1 0...Stop StoreHouseID 1\n
                        > w2002 1 10 0...Set StoreHouseID 1-10 Config
                        > w2003 1 10 1...Change to Charging Step 1
                        > w2003 1 10 2...Change to Charging Step 2
                        > w2003 1 10 3...Change to Charging Step 3
                        > w2004 1 10 0...Get StoreHouseID 1-10 Status
                        > w2005 1 10 0...Stop StoreHouseID 1-10"""
                    )
                time.sleep(1)
        except Exception:
            self.logging.error(f"handle_autotest_script failed\n{self.logger.get_slim_error_log()}")

    def handle_autotest_script(self):
        """Autotest script handler

        Args:
            None

        Returns:
            None

        Example:
            >>> xml_socket_svr_obj = XMLSocketServer()
            >>> xml_socket_svr_obj.handle_autotest_script()

        """
        try:
            round = 0
            start_time = time.time()
            self.power_control(0, "Power ON Device")
            self.power_control(10, "Battery Exit")
            while True:
                try:
                    self.power_control(30, "Battery Entry")
                    self.test_rack(4, 4, 0, "w2002", 10)
                    self.test_rack(7, 8, 0, "w2002", 10)
                    self.test_rack(4, 4, 1, "w2003", 10)
                    self.test_rack(7, 8, 1, "w2003", 10)
                    self.power_control(30, "Battery CCC")
                    # self.power_control(7200, "Battery CCC")

                    self.test_rack(4, 4, 2, "w2003", 10)
                    self.test_rack(7, 8, 2, "w2003", 10)
                    self.power_control(30, "Battery REST")
                    # self.power_control(600, "Battery REST")

                    self.test_rack(4, 4, 3, "w2003", 10)
                    self.test_rack(7, 8, 3, "w2003", 10)
                    self.power_control(10, "Battery Exit")
                    round += 1
                    formatted_time = self.format_duration(time.time() - start_time)
                    msg_log = f"CCS Round {round} - {formatted_time}"
                    self.influxdb_obj.write_log_influxdb("DEBUG", msg_log, "system")
                    self.logging.info(msg_log)
                except Exception:
                    self.logging.error(
                        """Usage:
                        Command [Start_StoreHouse_ID] [End_StoreHouse_ID] [Charging Step]
                        Examples:
                        > w2002 1 1 0...Set StoreHouseID 1 Config
                        > w2003 1 1 1...Change to Charging Step 1
                        > w2003 1 1 2...Change to Charging Step 2
                        > w2003 1 1 3...Change to Charging Step 3
                        > w2004 1 1 0...Get StoreHouseID 1 Status
                        > w2005 1 1 0...Stop StoreHouseID 1\n
                        > w2002 1 10 0...Set StoreHouseID 1-10 Config
                        > w2003 1 10 1...Change to Charging Step 1
                        > w2003 1 10 2...Change to Charging Step 2
                        > w2003 1 10 3...Change to Charging Step 3
                        > w2004 1 10 0...Get StoreHouseID 1-10 Status
                        > w2005 1 10 0...Stop StoreHouseID 1-10"""
                    )
        except Exception:
            self.logging.error(f"handle_autotest_script failed\n{self.logger.get_slim_error_log()}")

    def handle_autotest_script_simulation(self):
        """Autotest script handler

        Args:
            None

        Returns:
            None

        Example:
            >>> xml_socket_svr_obj = XMLSocketServer()
            >>> xml_socket_svr_obj.handle_autotest_script()

        """
        try:
            round = 0
            start_time = time.time()
            while True:
                try:
                    self.influxdb_obj.write_log_influxdb("DEBUG", "CCS w2002 1 84 0", "system")
                    self.logging.info("w2002 1 84 0")
                    self.cmd = "w2002"
                    self.cmd_start_idx = 1
                    self.cmd_end_idx = 1
                    self.cmd_step = 0
                    time.sleep(10)

                    self.influxdb_obj.write_log_influxdb("DEBUG", "CCS w2003 1 84 1", "system")
                    self.logging.info("w2003 1 84 1")
                    self.cmd = "w2003"
                    self.cmd_start_idx = 1
                    self.cmd_end_idx = 1
                    self.cmd_step = 1
                    time.sleep(300)

                    self.influxdb_obj.write_log_influxdb("DEBUG", "CCS w2003 1 84 2", "system")
                    self.logging.info("w2003 1 84 2")
                    self.cmd = "w2003"
                    self.cmd_start_idx = 1
                    self.cmd_end_idx = 1
                    self.cmd_step = 2
                    time.sleep(300)

                    self.influxdb_obj.write_log_influxdb("DEBUG", "CCS w2003 1 84 3", "system")
                    self.logging.info("w2003 1 84 3")
                    self.cmd = "w2003"
                    self.cmd_start_idx = 1
                    self.cmd_end_idx = 1
                    self.cmd_step = 3
                    time.sleep(120)
                    round += 1
                    formatted_time = self.format_duration(time.time() - start_time)
                    self.influxdb_obj.write_log_influxdb("DEBUG", f"CCS Round {round} - {formatted_time}", "system")
                    self.logging.info(f"Round {round} - {formatted_time}")
                except Exception:
                    self.logging.error(
                        """Usage:
                        Command [Start_StoreHouse_ID] [End_StoreHouse_ID] [Charging Step]
                        Examples:
                        > w2002 1 1 0...Set StoreHouseID 1 Config
                        > w2003 1 1 1...Change to Charging Step 1
                        > w2003 1 1 2...Change to Charging Step 2
                        > w2003 1 1 3...Change to Charging Step 3
                        > w2004 1 1 0...Get StoreHouseID 1 Status
                        > w2005 1 1 0...Stop StoreHouseID 1\n
                        > w2002 1 10 0...Set StoreHouseID 1-10 Config
                        > w2003 1 10 1...Change to Charging Step 1
                        > w2003 1 10 2...Change to Charging Step 2
                        > w2003 1 10 3...Change to Charging Step 3
                        > w2004 1 10 0...Get StoreHouseID 1-10 Status
                        > w2005 1 10 0...Stop StoreHouseID 1-10"""
                    )
        except Exception:
            self.logging.error(f"handle_autotest_script failed\n{self.logger.get_slim_error_log()}")

    def run(self):  # noqa: C901
        """Process xml server

        Args:
            None

        Returns:
            None

        Example:
            >>> xml_socket_svr_obj = XMLSocketServer()
            >>> xml_socket_svr_obj.run()

        """
        try:
            self.logging.info(f"Server started on {self.host}:{self.port}")

            self.last_store_house_status = self.last_store_house_step_check = time.time()
            while True:
                try:
                    timeout = 1
                    read_sockets, write_sockets, exception_sockets = select.select(self.sockets_list, self.sockets_list, self.sockets_list, timeout)

                    for notified_socket in read_sockets:
                        if notified_socket == self.server_socket:
                            client_socket, client_address = self.server_socket.accept()
                            client_socket.setblocking(False)
                            self.sockets_list.append(client_socket)
                            self.clients[client_socket] = {
                                "address": client_address,
                                "outbox": [],
                            }
                            self.logging.info(f"Accepted new connection from {client_address}")
                        else:
                            try:
                                # Receive socket xml message
                                data = b""
                                header = notified_socket.recv(4)
                                if header == b"":
                                    break
                                length = struct.unpack(">I", header)[0]
                                recv_data = notified_socket.recv(length)
                                length = length - len(recv_data)
                                data += recv_data
                                while length > 0:
                                    data = notified_socket.recv(length)
                                    data += recv_data
                                self.parse_and_handle_messages(data.decode(), notified_socket)
                            except BlockingIOError:
                                pass
                            except Exception:
                                self.logging.error(f"run Failed\n{self.logger.get_slim_error_log()}")
                                self.sockets_list.remove(notified_socket)
                                del self.clients[notified_socket]

                    for notified_socket in write_sockets:
                        if notified_socket in self.clients:
                            try:
                                # Send w2002 and w2003 xml message
                                for sh_id in range(self.cmd_start_idx, self.cmd_end_idx + 1):
                                    tid = self.generate_transaction_id()
                                    trxid = self.generate_transaction_id()
                                    if self.cmd == "w2002":
                                        request_xml = self.create_request_w2002_xml(
                                            "StoreHouseStatusRequest",
                                            tid,
                                            trxid,
                                            sh_id
                                        )
                                        self.logging.debug(f"[Send] W2002Req | None | {sh_id} | {tid[-6:]} | {trxid[-6:]}")
                                        notified_socket.sendall(struct.pack(">I", len(request_xml)) + request_xml)
                                    elif self.cmd == "w2003":
                                        request_xml = self.create_request_w2003_xml(
                                            "StoreHouseStepCheckRequest",
                                            tid,
                                            trxid,
                                            sh_id,
                                            self.cmd_step
                                        )
                                        self.logging.debug(f"[Send] W2003Req | None | {sh_id} |  {tid[-6:]} | {trxid[-6:]}")
                                        notified_socket.sendall(struct.pack(">I", len(request_xml)) + request_xml)
                                    elif self.cmd == "w2004":
                                        request_xml = self.create_request_w2004_xml(
                                            "StoreHouseNGCheckRequest",
                                            tid,
                                            trxid,
                                            sh_id
                                        )
                                        self.logging.debug(f"[Send] W2004Req | None | {sh_id} | {tid[-6:]} | {trxid[-6:]}")
                                        notified_socket.sendall(struct.pack(">I", len(request_xml)) + request_xml)
                                    elif self.cmd == "w2005":
                                        request_xml = self.create_request_w2005_xml("JudgmentCompletionNotificationRequest",
                                                                                    tid,
                                                                                    trxid,
                                                                                    sh_id)
                                        self.logging.debug(f"[Send] W2005Req | None | {sh_id} | {tid[-6:]} | {trxid[-6:]}")
                                        notified_socket.sendall(struct.pack(">I", len(request_xml)) + request_xml)
                                self.cmd = ""
                            except Exception:
                                self.logging.error(f"run failed\n{self.logger.get_slim_error_log()}")
                                del self.clients[notified_socket]
                    for exception_socket in exception_sockets:
                        self.sockets_list.remove(exception_socket)
                        del self.clients[exception_socket]
                except Exception:
                    self.logging.error(f"run failed\n{self.logger.get_slim_error_log()}")
                    time.sleep(3)
        except ET.ParseError:
            self.logging.error(f"run failed\n{self.logger.get_slim_error_log()}")
            time.sleep(3)


if __name__ == "__main__":
    try:
        with open("core\\main\\config.json") as json_file:
            config_data = json.load(json_file)
            # Create xml socket server thread
            # server = XMLSocketServer(config_data["ccs_ip"], config_data["ccs_port"])
            server = XMLSocketServer(config_data["ccs_ip"], config_data["ccs_port"])
            server_thread = threading.Thread(target=server.run, daemon=True)
            server_thread.start()

            # Create keyboard input handler thread
            keyboard_thread = threading.Thread(target=server.handle_keyboard_input, daemon=True)
            # keyboard_thread.start()
            # autotest_thread = threading.Thread(target=server.handle_autotest_script_simulation, daemon=True)
            # autotest_thread = threading.Thread(target=server.handle_autotest_script, daemon=True)
            # autotest_thread = threading.Thread(target=server.handle_autotest_script_demo, daemon=True)
            # autotest_thread.start()

            # keyboard_thread = threading.Thread(target=server.handle_power, daemon=True)
            # keyboard_thread.start()
            # keyboard_thread.join()

            keyboard_thread = threading.Thread(target=server.handle_input_cmd, daemon=True)
            keyboard_thread.start()
            keyboard_thread.join()

            server_thread.join()
            # keyboard_thread.join()
            # autotest_thread.join()

            logging.info("Program Terminated")
    except Exception:
        logging.error("Main function failed")
