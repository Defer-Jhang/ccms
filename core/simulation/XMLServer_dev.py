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


import warnings
from influxdb_client.client.warnings import MissingPivotFunction
warnings.simplefilter("ignore", MissingPivotFunction)


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import balps.influxdb_mgr
import balps.mssql_mgr
from remote_mode_api import RemoteBatteryModeAPI
from balps import log_mgr
import Pyro5.api
import re


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
            self.cmd_lock = threading.Lock()
            self.auto_state_lock = threading.Lock()

            self.auto_w2003_pending = {}
            self.auto_w2003_step2_pending = {}
            self.auto_w2003_step3_pending = {}

            # rack cooldown after step3
            # key: rack id, value: cooldown end monotonic timestamp
            self.rack_cooldown_until = {}

            # 預設 cooldown 秒數，可依需求改，例如 300 / 600 / 1800
            self.step3_to_w2002_cooldown_sec = 180

            self.remote_schedule_lock = threading.Lock()

            # 下一次已排程的 rexit 執行時間，monotonic timestamp
            self.remote_rexit_due_ts = None

            # rexit 後至少等幾秒才允許 rentry
            self.remote_reentry_after_exit_guard_sec = 10

            self.enable_test_servermap_hook = os.environ.get("CCMS_ENABLE_TEST_SERVERMAP_HOOK", "0") == "1"

            # Get charging cabinet information
            # The ServerMap table has StoreHouseID, PalletID, SerialBoardID, PalletPosition, Position and QRCODEID fields
            self.mssql_obj = balps.mssql_mgr.mssql_api()
            self.mssql_obj.create_db_connect_pd()

            if self.enable_test_servermap_hook:
                self.meas_map = self.mssql_obj.query_db_pd("SELECT * FROM CCSMap_peter")
            else:
                self.meas_map = self.mssql_obj.query_db_pd("SELECT * FROM CCSMap")
            
            self.influxdb_obj = balps.influxdb_mgr.influxdb_api()
            self.influxdb_obj.create_connect()
            self.ccs_api = None

            self.network_connected = True
            self.enable_artifact_hook = os.environ.get("CCMS_ENABLE_ARTIFACT_HOOK", "0") == "1"
            self.enable_artifact_alive_hook = os.environ.get("CCMS_ENABLE_ARTIFACT_ALIVE_HOOK", "0") == "1"
            self.enable_artifact_sync_hook = os.environ.get("CCMS_ENABLE_ARTIFACT_SYNC_HOOK", "0") == "1"

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
            
            if self.enable_artifact_sync_hook:
                ET.SubElement(body, "SYSTIME").text = (datetime.now() + timedelta(hours=-3)).strftime("%Y%m%d%H%M%S")
            else:
                ET.SubElement(body, "SYSTIME").text = datetime.now().strftime("%Y%m%d%H%M%S")

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
            # ET.SubElement(recipe_step, "Cell_Delta_Temp_Frame").text = "3"

            xml_hook = os.environ.get(
                "CCS_XML_HOOK"
            )
            if(xml_hook is None or xml_hook=="5"):
                ET.SubElement(recipe_step, "Cell_Delta_Temp_Frame").text = "3"  #normal case
            elif(xml_hook=="1"):
                ET.SubElement(recipe_step, "cell_Delta_Temp_Frame").text = "3"  # Key case mismatch
            elif(xml_hook=="2"):
                ET.SubElement(recipe_step, "Cell_Delta_Temp_Frame").text = "aa" #Invalid recipe parameter value
            elif(xml_hook=="3"):
                ET.SubElement(recipe_step, "").text = "3"   #Parsing XML error

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

            if(xml_hook=="5"):
                ET.SubElement(pallet, "SerialBoardID").text = "C00169"
            else:
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

            # ===== [HOOK] Save Reply XML to artifacts for automation =====
            try:
                if self.enable_artifact_hook and message_name:
                    os.makedirs("artifacts", exist_ok=True)

                    # 保留你原本的 Reply 行為
                    if message_name.endswith("Reply"):
                        with open(r"artifacts\last_reply.xml", "w", encoding="utf-8") as f:
                            f.write(xml_string)

                        with open(r"artifacts\reply_log.xml", "a", encoding="utf-8") as f:
                            f.write("\n<!-- REPLY_BEGIN -->\n")
                            f.write(xml_string)
                            f.write("\n<!-- REPLY_END -->\n")

                    # 新增：Alarm 開頭的訊息另外寫到 last_alarm.xml
                    if message_name.startswith("Alarm"):
                        with open(r"artifacts\last_alarm.xml", "w", encoding="utf-8") as f:
                            f.write(xml_string)

                    if self.enable_artifact_alive_hook and message_name.startswith("Alive"):
                        with open(r"artifacts\alive_request.xml", "a", encoding="utf-8") as f:
                            f.write("\n<!-- REPLY_BEGIN -->\n")
                            f.write(xml_string)
                            f.write("\n<!-- REPLY_END -->\n")

                    if self.enable_artifact_sync_hook and message_name.startswith("Sync"):
                        with open(r"artifacts\last_sync.xml", "w", encoding="utf-8") as f:
                            f.write(xml_string)

            except Exception:
                pass
            # ===== [HOOK END] =====

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
                            self.influxdb_obj.write_ccs_logs_influxdb("DEBUG", f"CCS {self.cmd} {self.cmd_start_idx} {self.cmd_end_idx} {self.cmd_step}", "system")
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
                            if inputs[0] == "dis":
                                self.logging.info(">>> [SIMULATION] Physical network wire UNPLUGGED (拔除網路線) <<<")
                                self.network_connected = False
                                self.simulate_disconnect()  # 立刻踢掉現有連線
                            
                            elif inputs[0] == "recon":
                                self.logging.info(">>> [SIMULATION] Physical network wire PLUGGED IN (插回網路線) <<<")
                                self.network_connected = True
                                self.logging.info("Server is now ready to accept new connections from CCMS.")
                            
                            elif inputs[0] == "con":
                                self.power_control(0, "Power ON Device")
                            elif inputs[0] == "on":
                                self.power_control(0, "Battery Entry")
                            elif inputs[0] == "off":
                                self.power_control(0, "Battery Exit")
                            elif inputs[0] == "cc":
                                self.power_control(30, "Battery CCC")
                            elif inputs[0] == "rest":
                                self.power_control(30, "Battery REST")
                            elif inputs[0] == "rcon":
                                self.remote_connect()
                            elif inputs[0] == "rentry":
                                self.remote_mode_battery_entry()
                            elif inputs[0] == "rcc":
                                timer = int(inputs[1]) if len(inputs) >= 2 else 30
                                self.remote_mode_cc_charge(timer)
                            elif inputs[0] == "ridle":
                                timer = int(inputs[1]) if len(inputs) >= 2 else 30
                                self.remote_mode_idle(timer)
                            elif inputs[0] == "rexit":
                                self.remote_mode_battery_exit()
                        
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

    def query_latest_board_status_flux(
        self,
        protectboard_id: str,
        field_name: str,
        measurement: str | None = None,
        lookback_sec: int = 30,
        exclude_qrcode_curr: bool = True,
    ):
        """
        正式流程用：
        直接用 raw Flux query 查指定 serialboard_id / field 的最新一筆資料。

        查不到回 None。
        查到回 latest dict。
        """
        try:
            import pandas as pd

            meas = measurement or self.get_current_measurement_name(prefix="data", use_utc=False,)

            qrcode_filter = (
                '  |> filter(fn: (r) => r["qrcode"] != "curr")\n'
                if exclude_qrcode_curr else ""
            )

            flux_query = (
                f'from(bucket: "{self.influxdb_obj.bucket}")\n'
                f'  |> range(start: -{int(lookback_sec)}s)\n'
                f'  |> filter(fn: (r) => r["_measurement"] == "{meas}")\n'
                f'  |> filter(fn: (r) => r["_field"] == "{field_name}")\n'
                f'{qrcode_filter}'
                f'  |> filter(fn: (r) => r["protectboard_id"] == "{protectboard_id}")\n'
                f'  |> group()\n'
                f'  |> sort(columns: ["_time"], desc: true)\n'
                f'  |> limit(n: 1)'
            )

            df = self.influxdb_obj.query_api.query_data_frame(
                query=flux_query,
                org=self.influxdb_obj.org,
            )

            if isinstance(df, list):
                df = pd.concat(
                    [x for x in df if isinstance(x, pd.DataFrame) and not x.empty],
                    ignore_index=True,
                ) if df else pd.DataFrame()

            if not isinstance(df, pd.DataFrame) or df.empty:
                return None

            drop_cols = [c for c in ["result", "table"] if c in df.columns]
            if drop_cols:
                df = df.drop(columns=drop_cols)

            if "_value" in df.columns and field_name not in df.columns:
                df = df.rename(columns={"_value": field_name})

            if "_time" in df.columns and "datetime" not in df.columns:
                df["datetime"] = pd.to_datetime(df["_time"], errors="coerce")
            elif "datetime" in df.columns:
                df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")

            if "protectboard_id" in df.columns:
                df["protectboard_id"] = df["protectboard_id"].astype(str)
                df = df[df["protectboard_id"] == str(protectboard_id)]

            if df.empty:
                return None

            sort_col = "datetime" if "datetime" in df.columns else "_time"
            df = df.sort_values(sort_col)

            latest = df.iloc[-1].to_dict()

            if "datetime" in latest and pd.notna(latest["datetime"]):
                latest["datetime"] = str(latest["datetime"])

            if "_time" in latest and pd.notna(latest["_time"]):
                latest["_time"] = str(latest["_time"])

            return latest

        except Exception:
            self.logging.error(
                f"query_latest_board_status_flux failed\n"
                f"{self.logger.get_slim_error_log()}"
            )
            return None
    
    def get_current_measurement_name(self, prefix: str = "data", use_utc: bool = False):
        """
        依目前日期產生 measurement 名稱，例如 data_20260616
        """
        # from datetime import datetime, timezone

        # now_dt = datetime.now(timezone.utc) if use_utc else datetime.now()
        return "meas_data"

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

                    # === 1. 處理讀取與新連線事件 ===
                    for notified_socket in read_sockets:
                        if notified_socket == self.server_socket:
                            client_socket, client_address = self.server_socket.accept()

                            # 【斷線模擬】如果目前是拔線狀態，直接拒絕並關閉連線
                            if not self.network_connected:
                                self.logging.info(f"Network offline. Refusing connection from {client_address}")
                                client_socket.close()
                                continue


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


                                # 【關鍵修正】當 CCMS 斷開連線（收到空資料）時，徹底清理乾淨並 continue
                                if header == b"":
                                    self.logging.info("Client connection lost (received empty header). Cleaning up.")
                                    if notified_socket in self.sockets_list:
                                        self.sockets_list.remove(notified_socket)
                                    if notified_socket in self.clients:
                                        del self.clients[notified_socket]
                                    notified_socket.close()
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
                                if notified_socket in self.sockets_list:
                                    self.sockets_list.remove(notified_socket)
                                if notified_socket in self.clients:
                                    del self.clients[notified_socket]
                                try:
                                    notified_socket.close()
                                except Exception:
                                    pass
                    
                    
                    for notified_socket in write_sockets:
                        if notified_socket in self.clients:
                            try:
                                # 用 lock 保護整組 command 取出
                                with self.cmd_lock:
                                    if not self.cmd:
                                        continue

                                    current_cmd = self.cmd
                                    current_start_idx = self.cmd_start_idx
                                    current_end_idx = self.cmd_end_idx
                                    current_step = self.cmd_step

                                    # 取走後立刻清空，避免重複送
                                    self.cmd = ""

                                # Send w2002 / w2003 / w2004 / w2005 xml message
                                for sh_id in range(current_start_idx, current_end_idx + 1):
                                    tid = self.generate_transaction_id()
                                    trxid = self.generate_transaction_id()
                                    if current_cmd == "w2002":
                                        request_xml = self.create_request_w2002_xml(
                                            "StoreHouseStatusRequest",
                                            tid,
                                            trxid,
                                            sh_id
                                        )
                                        self.logging.debug(f"[Send] W2002Req | None | {sh_id} | {tid[-6:]} | {trxid[-6:]}")
                                        notified_socket.sendall(struct.pack(">I", len(request_xml)) + request_xml)
                                    elif current_cmd == "w2003":
                                        request_xml = self.create_request_w2003_xml(
                                            "StoreHouseStepCheckRequest",
                                            tid,
                                            trxid,
                                            sh_id,
                                            current_step
                                        )
                                        self.logging.debug(f"[Send] W2003Req | None | {sh_id} |  {tid[-6:]} | {trxid[-6:]}")
                                        notified_socket.sendall(struct.pack(">I", len(request_xml)) + request_xml)
                                    elif current_cmd == "w2004":
                                        request_xml = self.create_request_w2004_xml(
                                            "StoreHouseNGCheckRequest",
                                            tid,
                                            trxid,
                                            sh_id
                                        )
                                        self.logging.debug(f"[Send] W2004Req | None | {sh_id} | {tid[-6:]} | {trxid[-6:]}")
                                        notified_socket.sendall(struct.pack(">I", len(request_xml)) + request_xml)

                                    elif current_cmd == "w2005":
                                        request_xml = self.create_request_w2005_xml("JudgmentCompletionNotificationRequest", tid, trxid, sh_id)
                                        self.logging.debug(f"[Send] W2005Req | None | {sh_id} | {tid[-6:]} | {trxid[-6:]}")
                                        notified_socket.sendall(struct.pack(">I", len(request_xml)) + request_xml)

                            except Exception:
                                self.logging.error(f"run failed\n{self.logger.get_slim_error_log()}")
                                if notified_socket in self.sockets_list:
                                    self.sockets_list.remove(notified_socket)
                                if notified_socket in self.clients:
                                    del self.clients[notified_socket]
                                try:
                                    notified_socket.close()
                                except Exception:
                                    pass



                    # === 3. 處理異常 Socket ===
                    for exception_socket in exception_sockets:
                        self.logging.info("Handling exception socket.")
                        if exception_socket in self.sockets_list:
                            self.sockets_list.remove(exception_socket)
                        if exception_socket in self.clients:
                            del self.clients[exception_socket]
                        try:
                            exception_socket.close()
                        except Exception:
                            pass
                except Exception:
                    self.logging.error(f"run failed\n{self.logger.get_slim_error_log()}")
                    time.sleep(3)
        except ET.ParseError:
            self.logging.error(f"run failed\n{self.logger.get_slim_error_log()}")
            time.sleep(3)

    def simulate_disconnect(self):
        """模擬網路斷線：主動強制切斷目前所有已連線的 CCMS 客戶端"""
        try:
            active_clients = list(self.clients.keys())
            for client_socket in active_clients:
                address = self.clients[client_socket]["address"]
                self.logging.info(f"Forcing disconnect on client: {address}")
                
                if client_socket in self.sockets_list:
                    self.sockets_list.remove(client_socket)
                if client_socket in self.clients:
                    del self.clients[client_socket]
                try:
                    client_socket.shutdown(socket.SHUT_RDWR)
                    client_socket.close()
                except Exception:
                    pass
            self.logging.info("All active clients have been cut off.")
        except Exception:
            self.logging.error(f"simulate_disconnect failed\n{self.logger.get_slim_error_log()}")


    ############### auto scan test ##########################
    def wait_until_ccms_connected(self, timeout_sec: int = 120, check_interval_sec: float = 1.0):
        """
        等待至少有一個 CCMS client 連上 XMLServer
        """
        deadline = time.time() + timeout_sec

        while time.time() < deadline:
            try:
                if len(self.clients) > 0:
                    self.logging.info(f"CCMS connected, client_count={len(self.clients)}")
                    return True
            except Exception:
                pass

            time.sleep(check_interval_sec)

        self.logging.error("wait_until_ccms_connected timeout")
        return False

    
    def issue_cmd(self, cmd: str, start_idx: int, end_idx: int, step: int = 0, sleep_sec: float = 1.0):
        """
        統一設定 XMLServer 指令排程（Lock 保護版）
        """
        try:
            self.influxdb_obj.write_log_influxdb(
                "DEBUG",
                f"CCS {cmd} {start_idx} {end_idx} {step}",
                "system"
            )
        except Exception:
            pass

        self.logging.debug(f"{cmd} {start_idx} {end_idx} {step}")

        with self.cmd_lock:
            self.cmd = cmd
            self.cmd_start_idx = start_idx
            self.cmd_end_idx = end_idx
            self.cmd_step = step

        time.sleep(sleep_sec)
    
    def issue_cmd_wait_slot_empty(
        self,
        cmd: str,
        start_idx: int,
        end_idx: int,
        step: int = 0,
        sleep_sec: float = 1.0,
        timeout_sec: float = 10.0,
        poll_sec: float = 0.05,
    ):
        """
        送新 command 前，先等待目前 cmd slot 被 run() 消化掉

        Args:
            cmd: 例如 "w2002" / "w2003"
            start_idx: Start StoreHouseID
            end_idx: End StoreHouseID
            step: 對應 w2003 / w2002 的 step，預設 0
            sleep_sec: issue_cmd 後額外等待秒數
            timeout_sec: 最多等多久讓 cmd slot 變空
            poll_sec: 每次檢查 slot 的間隔秒數

        Returns:
            {
                "ok": bool,
                "reason": str | None,
                "cmd": str,
                "start_idx": int,
                "end_idx": int,
                "step": int,
            }
        """
        try:
            deadline = time.time() + timeout_sec

            while time.time() < deadline:
                with self.cmd_lock:
                    slot_empty = (self.cmd == "")

                if slot_empty:
                    self.issue_cmd(
                        cmd=cmd,
                        start_idx=start_idx,
                        end_idx=end_idx,
                        step=step,
                        sleep_sec=sleep_sec,
                    )
                    return {
                        "ok": True,
                        "reason": None,
                        "cmd": cmd,
                        "start_idx": start_idx,
                        "end_idx": end_idx,
                        "step": step,
                    }

                time.sleep(poll_sec)

            self.logging.warning(
                f"issue_cmd_wait_slot_empty timeout | "
                f"cmd={cmd} {start_idx} {end_idx} {step}"
            )
            return {
                "ok": False,
                "reason": "timeout_wait_slot_empty",
                "cmd": cmd,
                "start_idx": start_idx,
                "end_idx": end_idx,
                "step": step,
            }

        except Exception:
            self.logging.error(f"issue_cmd_wait_slot_empty failed\n{self.logger.get_slim_error_log()}")
            return {
                "ok": False,
                "reason": "exception",
                "cmd": cmd,
                "start_idx": start_idx,
                "end_idx": end_idx,
                "step": step,
            }

    
    def read_esp_port_status_file(self, retries: int = 10, retry_delay: float = 0.2):
        """
        讀取 artifacts/esp_expected_ports.json 與 esp_active_ports.json
        """
        import json

        expected_path = os.path.join("artifacts", "esp_expected_ports.json")
        active_path = os.path.join("artifacts", "esp_active_ports.json")

        last_error = None

        for _ in range(retries):
            try:
                expected = {}
                active = {}

                if os.path.exists(expected_path):
                    with open(expected_path, "r", encoding="utf-8") as f:
                        txt = f.read().strip()
                        if txt:
                            expected = json.loads(txt)

                if os.path.exists(active_path):
                    with open(active_path, "r", encoding="utf-8") as f:
                        txt = f.read().strip()
                        if txt:
                            active = json.loads(txt)

                expected_ports = sorted(expected.get("expected_ports", []))
                active_ports = sorted(active.get("active_ports", []))

                return {
                    "racks": expected.get("racks") or active.get("racks"),
                    "expected_ports": expected_ports,
                    "active_ports": active_ports,
                    "matched": expected_ports == active_ports,
                    "missing_ports": sorted(set(expected_ports) - set(active_ports)),
                    "unexpected_ports": sorted(set(active_ports) - set(expected_ports)),
                }

            except Exception as e:
                last_error = e
                time.sleep(retry_delay)

        self.logging.error(f"read_esp_port_status_file failed: {last_error}")
        return {
            "racks": None,
            "expected_ports": [],
            "active_ports": [],
            "matched": False,
            "missing_ports": [],
            "unexpected_ports": [],
        }
    

    
    def port_to_rack(self, port: int) -> int:
        """
        20001,20002 -> rack 1
        20003,20004 -> rack 2
        """
        return ((int(port) - 20001) // 2) + 1
    

    
    def get_target_ports(self, port_count: int = 168, port_start: int = 20001):
        """
        取得這次要掃描的 target ports
        預設 168 個 port = 20001 ~ 20168
        """
        return list(range(port_start, port_start + port_count))
    

    def merge_to_ranges(self, nums: list[int]):
        """
        [1,2,3,7,8,10] -> [(1,3), (7,8), (10,10)]
        """
        if not nums:
            return []

        nums = sorted(set(nums))
        ranges = []
        start = prev = nums[0]

        for n in nums[1:]:
            if n == prev + 1:
                prev = n
            else:
                ranges.append((start, prev))
                start = prev = n

        ranges.append((start, prev))
        return ranges
    

    # def rack_to_serialboard_ids(self, rack: int) -> list[str]:
    #     """
    #     rack 與 serialboard_id 的對應規則：
    #     rack 1 -> A0001, A0002
    #     rack 2 -> A0003, A0004
    #     rack 3 -> A0005, A0006
    #     ...
    #     """
    #     first_num = (rack - 1) * 2 + 1
    #     return [f"A{first_num:04d}", f"A{first_num + 1:04d}"]

    def rack_to_task_names(self, rack: int) -> list[str]:
        """
        Rack 與 task name 的對應關係：

        rack 1  -> ["1", "2"]
        rack 2  -> ["3", "4"]
        ...
        rack 84 -> ["167", "168"]
        """
        first_num = (rack - 1) * 2 + 1
        return [str(first_num), str(first_num + 1)]


    def task_name_to_rack(self, task_name: str) -> int | None:
        """
        Task name 與 rack 的對應關係：

        1, 2     -> rack 1
        3, 4     -> rack 2
        ...
        167, 168 -> rack 84

        也支援：

        A0001  -> rack 1
        C00001 -> rack 1
        00002  -> rack 1
        """
        try:
            if not task_name:
                return None

            task_name = str(task_name).strip().upper()

            # 只取最後一段連續數字，前面的英文字母與前導 0 不影響。
            match = re.search(r"\d+$", task_name)
            if not match:
                return None

            serial_num = int(match.group())

            # 每兩個 task name 對應一個 rack。
            rack = ((serial_num - 1) // 2) + 1

            if rack < 1 or rack > 84:
                return None

            return rack

        except (TypeError, ValueError):
            return None
        
    def add_racks_to_cooldown(self, racks: list[int], cooldown_sec: int):
        """
        將 rack 加入 cooldown。
        cooldown 期間 handle_poll_expected_task_ready_by_rack_count 會跳過這些 rack。
        """
        try:
            if not racks:
                return

            now_ts = time.monotonic()
            cooldown_until = now_ts + cooldown_sec

            with self.auto_state_lock:
                for rack in racks:
                    self.rack_cooldown_until[rack] = cooldown_until

        except Exception:
            self.logging.error(
                f"add_racks_to_cooldown failed\n"
                f"{self.logger.get_slim_error_log()}"
            )

    def get_active_cooldown_racks(self) -> set:
        """
        回傳目前仍在 cooldown 的 rack set。
        同時清掉已過期的 cooldown。
        """
        try:
            now_ts = time.monotonic()

            with self.auto_state_lock:
                expired_racks = [
                    rack for rack, until_ts in self.rack_cooldown_until.items()
                    if now_ts >= until_ts
                ]

                for rack in expired_racks:
                    self.rack_cooldown_until.pop(rack, None)

                active_cooldown_racks = set(self.rack_cooldown_until.keys())

            return active_cooldown_racks

        except Exception:
            self.logging.error(
                f"get_active_cooldown_racks failed\n"
                f"{self.logger.get_slim_error_log()}"
            )
            return set()
        

    def schedule_remote_call_after_delay(
        self,
        delay_sec: int,
        action_name: str,
        *args,
        **kwargs,
    ):
        """
        背景延遲呼叫 remote mode API。

        特別保護：
        - 如果 action_name == "rexit"，記錄 rexit due time。
        - 如果 action_name == "rentry"，且已有 rexit 尚未執行，
        則保證 rentry 不會早於 rexit_due_ts + guard_sec。
        """
        try:
            now_ts = time.monotonic()
            due_ts = now_ts + delay_sec

            with self.remote_schedule_lock:
                if action_name == "rexit":
                    self.remote_rexit_due_ts = due_ts

                elif action_name == "rentry":
                    if self.remote_rexit_due_ts is not None:
                        min_rentry_ts = (
                            self.remote_rexit_due_ts
                            + self.remote_reentry_after_exit_guard_sec
                        )

                        if due_ts < min_rentry_ts:
                            due_ts = min_rentry_ts

            actual_delay_sec = max(0, due_ts - time.monotonic())

            def worker():
                try:
                    self.logging.info(
                        f"[REMOTE-SCHEDULE] scheduled action={action_name}, "
                        f"delay_sec={actual_delay_sec:.1f}, args={args}, kwargs={kwargs}"
                    )

                    time.sleep(actual_delay_sec)

                    if action_name == "rentry":
                        self.remote_mode_battery_entry()

                    elif action_name == "rexit":
                        self.remote_mode_battery_exit()

                        with self.remote_schedule_lock:
                            # 只有目前這次 rexit 執行完，才清空
                            self.remote_rexit_due_ts = None

                    elif action_name == "rcc":
                        timer = args[0] if args else kwargs.get("timer", 0)
                        self.remote_mode_cc_charge(timer)

                    elif action_name == "ridle":
                        timer = args[0] if args else kwargs.get("timer", 0)
                        self.remote_mode_idle(timer)

                    else:
                        self.logging.warning(
                            f"[REMOTE-SCHEDULE] unknown action_name={action_name}"
                        )
                        return

                    self.logging.info(
                        f"[REMOTE-SCHEDULE] executed action={action_name}"
                    )

                except Exception:
                    self.logging.error(
                        f"schedule_remote_call_after_delay worker failed | "
                        f"action={action_name}\n"
                        f"{self.logger.get_slim_error_log()}"
                    )

            t = threading.Thread(
                target=worker,
                name=f"remote-delay-{action_name}",
                daemon=True,
            )
            t.start()

        except Exception:
            self.logging.error(
                f"schedule_remote_call_after_delay failed | action={action_name}\n"
                f"{self.logger.get_slim_error_log()}"
            )


    def query_protectboard_ids_by_task_numbers(self, task_numbers: list[int]) -> dict[int, str]:
        """
        每次都重新查 DB，不使用 cache。

        用 BoardMapping.SerialBoardID 最後面的數字比對 task_no。

        例如：
            SerialBoardID = A0001    -> task_no = 1
            SerialBoardID = C0001    -> task_no = 1
            SerialBoardID = C000001  -> task_no = 1
            SerialBoardID = C0124    -> task_no = 124

        假設：
            同一個 task_no 在 BoardMapping 裡永遠只有一筆。

        回傳：
            {
                1: "PCB5001001",
                2: "PCB5002001",
            }
        """
        try:
            if not task_numbers:
                return {}

            task_numbers = sorted(set(int(x) for x in task_numbers))

            values_sql = ",\n".join(
                f"({task_no})"
                for task_no in task_numbers
            )

            query = f"""
            WITH expected_tasks AS (
                SELECT v.[task_no]
                FROM (VALUES
                    {values_sql}
                ) AS v([task_no])
            ),
            mapping_with_no AS (
                SELECT
                    bm.[SerialBoardID],
                    bm.[ProtectBoardID],
                    TRY_CONVERT(
                        int,
                        RIGHT(
                            bm.[SerialBoardID],
                            PATINDEX('%[^0-9]%', REVERSE(bm.[SerialBoardID]) + 'X') - 1
                        )
                    ) AS [task_no]
                FROM [BALPS].[dbo].[BoardMapping] AS bm
                WHERE bm.[SerialBoardID] IS NOT NULL
                AND bm.[ProtectBoardID] IS NOT NULL
            )
            SELECT
                m.[task_no],
                m.[SerialBoardID],
                m.[ProtectBoardID]
            FROM mapping_with_no AS m
            INNER JOIN expected_tasks AS e
                ON e.[task_no] = m.[task_no]
            """

            df = self.mssql_obj.query_db_pd(query)

            if df is None or df.empty:
                return {}

            result = {}

            for _, row in df.iterrows():
                task_no = row.get("task_no")
                protectboard_id = row.get("ProtectBoardID")

                if task_no is None or protectboard_id is None:
                    continue

                result[int(task_no)] = str(protectboard_id)

            return result

        except Exception:
            self.logging.error(
                f"query_protectboard_ids_by_task_numbers failed\n"
                f"{self.logger.get_slim_error_log()}"
            )
            return {}


    def rack_to_protectboard_ids(self, rack: int) -> list:
        """
        rack -> 對應兩個 ProtectBoardID。

        每次都重新查 BoardMapping，不使用 cache。

        rack 1 -> task_no 1, 2
        rack 2 -> task_no 3, 4
        """
        try:
            first_task_no = (int(rack) - 1) * 2 + 1
            task_numbers = [first_task_no, first_task_no + 1]

            mapping = self.query_protectboard_ids_by_task_numbers(task_numbers)

            protectboard_ids = []

            for task_no in task_numbers:
                protectboard_id = mapping.get(task_no)

                if protectboard_id:
                    protectboard_ids.append(protectboard_id)
                else:
                    self.logging.warning(
                        f"[BOARD-MAPPING] missing ProtectBoardID | "
                        f"rack={rack}, task_no={task_no}"
                    )

            return protectboard_ids

        except Exception:
            self.logging.error(
                f"rack_to_protectboard_ids failed | rack={rack}\n"
                f"{self.logger.get_slim_error_log()}"
            )
            return []

    def handle_poll_expected_task_ready_by_rack_count(
        self,
        rack_count: int = 84,
        poll_interval_sec: int = 300,
        issue_sleep_sec: int = 2,
        slot_timeout_sec: float = 10.0,
        slot_poll_sec: float = 0.05,
    ):
        """
        定期檢查 task 狀態。

        若 rack_count=10，則檢查 rack 1~10。
        rack 1~10 會轉成 task number:
            1 ~ 20

        DB 裡的 task_name 可能是：
            A001, C0001, C000001, A002 ...
        這版會抓 task_name 最後面的數字做比對：
            A001     -> 1
            C0001    -> 1
            C000001  -> 1
            A002     -> 2

        match 條件：
        1. task_no 沒有任何資料列 -> match
        2. task_no 有資料列，且所有資料列都是 active=0 且 is_delete=1 -> match

        not match 條件：
        - 只要有任一資料列不是 active=0 且 is_delete=1 -> not match

        對 matched task number 對應的 rack 下：
            w2002 rack rack 0
        """
        try:

            if rack_count < 1 or rack_count > 84:
                raise ValueError(f"rack_count must be between 1 and 84, got {rack_count}")

            selected_racks = list(range(1, rack_count + 1))

            # rack 1 -> task_no 1, 2
            # rack 2 -> task_no 3, 4
            # ...
            expected_task_numbers = []
            for rack in selected_racks:
                first_task_no = (rack - 1) * 2 + 1
                expected_task_numbers.extend([first_task_no, first_task_no + 1])

            self.logging.info(
                f"[TASK-POLL] start | rack_count={rack_count}, "
                f"selected_racks={selected_racks}, "
                f"expected_task_numbers={expected_task_numbers}, "
                f"poll_interval_sec={poll_interval_sec}"
            )

            # round_no = 0

            values_sql = ",\n".join(
                f"({task_no})"
                for task_no in expected_task_numbers
            )

            while(len(self.clients) <= 0):
                self.logging.error("[AUTO-W2002] CCMS not connected, autotest handler stop")
                time.sleep(10)

            while True:
                try:

                    if len(self.clients) <= 0:
                        self.logging.error("[AUTO-W2002] CCMS not connected, autotest handler stop")
                        continue

                    # start_time = time.monotonic()

                    query = f"""
                    WITH expected_tasks AS (
                        SELECT v.[task_no]
                        FROM (VALUES
                            {values_sql}
                        ) AS v([task_no])
                    ),
                    task_with_no AS (
                        SELECT
                            t.[task_name],
                            t.[active],
                            t.[is_delete],
                            TRY_CONVERT(
                                int,
                                RIGHT(
                                    t.[task_name],
                                    PATINDEX('%[^0-9]%', REVERSE(t.[task_name]) + 'X') - 1
                                )
                            ) AS [task_no]
                        FROM [BALPS].[dbo].[Task] AS t
                    )
                    SELECT e.[task_no]
                    FROM expected_tasks AS e
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM task_with_no AS bad
                        WHERE bad.[task_no] = e.[task_no]
                        AND (
                                bad.[active] <> 0
                            OR bad.[is_delete] <> 1
                        )
                    )
                    """

                    task_df = self.mssql_obj.query_db_pd(query)

                    if task_df is None or task_df.empty:
                        matched_task_numbers = []
                        matched_racks = []
                        self.logging.info("[TASK-POLL] no matched task")
                    else:
                        matched_set = set(task_df["task_no"].astype(int).tolist())

                        # 保留 expected_task_numbers 原本順序
                        matched_task_numbers = [
                            task_no for task_no in expected_task_numbers
                            if task_no in matched_set
                        ]

                        # task_no -> rack
                        matched_racks = []
                        for task_no in matched_task_numbers:
                            rack = ((int(task_no) - 1) // 2) + 1
                            matched_racks.append(rack)

                        matched_racks = sorted(set(matched_racks))

                        # 過濾仍在 cooldown 的 rack
                        # cooldown_racks = self.get_active_cooldown_racks()

                        # if cooldown_racks:
                        #     matched_racks = [
                        #         rack for rack in matched_racks
                        #         if rack not in cooldown_racks
                        #     ]

                        # self.logging.info(
                        #     f"[TASK-POLL] matched_task_numbers={matched_task_numbers}, "
                        #     f"matched_racks_after_cooldown_filter={matched_racks}"
                        # )

                    # 對 matched racks 下 w2002
                    if matched_racks:
                        rack_ranges = self.merge_to_ranges(matched_racks)

                        self.logging.info(
                            f"[TASK-POLL] issuing w2002 for matched racks | "
                            f"matched_racks={matched_racks}, rack_ranges={rack_ranges}"
                        )

                        for start_idx, end_idx in rack_ranges:
                            issue_result = self.issue_cmd_wait_slot_empty(
                                cmd="w2002",
                                start_idx=start_idx,
                                end_idx=end_idx,
                                step=0,
                                sleep_sec=issue_sleep_sec,
                                timeout_sec=slot_timeout_sec,
                                poll_sec=slot_poll_sec,
                            )

                            if issue_result["ok"]:
                                now_ts = time.monotonic()
                                self.influxdb_obj.write_ccs_logs_influxdb("DEBUG", f"CCS w2002 {start_idx} {end_idx} 0", "system")

                                # 建立 step1 pending，讓後續 w2003 step1 thread 接著處理
                                with self.auto_state_lock:
                                    for rack in range(start_idx, end_idx + 1):
                                        self.auto_w2003_pending[rack] = {
                                            "init_ts": now_ts,
                                            "w2003_issued": False,
                                        }

                                        # 新 w2002 代表重新 init，清掉舊的後續 step 狀態
                                        if hasattr(self, "auto_w2003_step2_pending"):
                                            self.auto_w2003_step2_pending.pop(rack, None)

                                        if hasattr(self, "auto_w2003_step3_pending"):
                                            self.auto_w2003_step3_pending.pop(rack, None)

                            else:
                                self.logging.warning(
                                    f"[TASK-POLL] issue skipped | "
                                    f"w2002 {start_idx} {end_idx} 0 | "
                                    f"reason={issue_result['reason']}"
                                )

                except Exception:
                    self.logging.error(
                        f"handle_poll_expected_task_ready_by_rack_count inner failed\n"
                        f"{self.logger.get_slim_error_log()}"
                    )

                time.sleep(poll_interval_sec)

        except Exception:
            self.logging.error(
                f"handle_poll_expected_task_ready_by_rack_count failed\n"
                f"{self.logger.get_slim_error_log()}"
            )

    def handle_autotest_w2003_after_init_and_active(
        self,
        measurement: str | None = None,
        voltage_field: str = "voltage",
        lookback_sec: int = 30,
        scan_interval_sec: int = 60,
        init_to_step1_delay_sec: int = 30,
        issue_sleep_sec: float = 0.5,
        slot_timeout_sec: float = 10.0,
        slot_poll_sec: float = 0.05,
        pending_expire_sec: int | None = None,
        require_all_boards_ready: bool = False,
    ):
        """
        step1 自動流程（latest 判斷版 / measurement 自動切天版）：

        1. rack 曾成功送過 w2002
        2. 等待 init_to_step1_delay_sec 秒（可為 0）
        3. 查 InfluxDB 中該 rack 對應 serialboard 的 latest 資料
        4. 若符合條件，則送出：
            w2003 rack rack 1

        條件說明：
        - require_all_boards_ready=False:
            只要任一塊 board query 到 latest（latest is not None），就切 step1
        - require_all_boards_ready=True:
            兩塊 board 都 query 到 latest（latest is not None），才切 step1

        measurement 說明：
        - 若 measurement is None：
            每輪自動使用 self.get_current_measurement_name(prefix="data", use_utc=True)
        - 若 measurement 有指定：
            則固定使用傳入的 measurement

        注意：
        - 這版不再依賴 espdevice.py 的 active port JSON
        - 只依賴：
            a. self.auto_w2003_pending（由 w2002 成功後建立）
            b. 經過時間
            c. query_latest_board_status() 查到的 latest 是否存在
        """
        try:
            while(len(self.clients) <= 0):
                self.logging.error("[AUTO-W2003-S1] CCMS not connected, autotest handler stop")
                time.sleep(10)

            # round_no = 0

            while True:
                try:
                    now_ts = time.monotonic()
                    # start_time = time.monotonic()

                    # measurement 若未指定，則每輪依日期自動產生
                    current_measurement = measurement or self.get_current_measurement_name(prefix="data", use_utc=False,)

                    due_racks = []
                    expired_racks = []

                    # 先在 lock 內複製 snapshot，避免 dictionary changed size during iteration
                    with self.auto_state_lock:
                        pending_items = list(self.auto_w2003_pending.items())

                    for rack, state in pending_items:
                        init_ts = state.get("init_ts")
                        w2003_issued = state.get("w2003_issued", False)

                        if init_ts is None or w2003_issued:
                            continue

                        elapsed = now_ts - init_ts

                        # optional: 避免 pending 永遠卡住
                        if pending_expire_sec is not None and elapsed >= pending_expire_sec:
                            expired_racks.append(rack)
                            continue

                        # 還沒到 delay
                        if elapsed < init_to_step1_delay_sec:
                            continue

                        protectboard_ids = self.rack_to_protectboard_ids(rack)
                        if not protectboard_ids:
                            self.logging.warning(
                                f"[AUTO-W2003-S1] rack={rack} has no protectboard_id mapping, skip"
                            )
                            continue

                        board_ready_results = []
                        board_debug_info = []

                        for protectboard_id in protectboard_ids:    
                            latest = self.query_latest_board_status_flux(
                                measurement=current_measurement,
                                protectboard_id=protectboard_id,
                                field_name=voltage_field,
                                lookback_sec=lookback_sec,
                                exclude_qrcode_curr=True,
                            )

                            # 只要 query 到 latest，就算 ready
                            ready = (latest is not None)

                            board_ready_results.append(ready)
                            board_debug_info.append(
                                f"{protectboard_id}: latest_exists={ready}, latest={latest}"
                            )

                        if require_all_boards_ready:
                            rack_ready = all(board_ready_results) if board_ready_results else False
                        else:
                            rack_ready = any(board_ready_results) if board_ready_results else False

                        if rack_ready:
                            due_racks.append(rack)

                    # 清掉過期 pending
                    if expired_racks:
                        with self.auto_state_lock:
                            for rack in expired_racks:
                                self.auto_w2003_pending.pop(rack, None)

                    due_racks = sorted(due_racks)
                    due_ranges = self.merge_to_ranges(due_racks)

                    if due_racks:

                        issued_racks = []

                        for start_idx, end_idx in due_ranges:
                            issue_result = self.issue_cmd_wait_slot_empty(
                                cmd="w2003",
                                start_idx=start_idx,
                                end_idx=end_idx,
                                step=1,
                                sleep_sec=issue_sleep_sec,
                                timeout_sec=slot_timeout_sec,
                                poll_sec=slot_poll_sec,
                            )

                            if issue_result["ok"]:
                                self.influxdb_obj.write_ccs_logs_influxdb("DEBUG", f"CCS w2003 {start_idx} {end_idx} 1","system")
                                racks = list(range(start_idx, end_idx + 1))
                                issued_racks.extend(racks)

                        
                        # 標記 step1 完成，並原子地建立 step2 pending
                        if issued_racks:
                            now_ts_after_step1 = time.monotonic()

                            with self.auto_state_lock:
                                for rack in issued_racks:
                                    # 1. 標記 step1 已送出
                                    if rack in self.auto_w2003_pending:
                                        self.auto_w2003_pending[rack]["w2003_issued"] = True

                                    # 2. 建立 step2 pending
                                    self.auto_w2003_step2_pending[rack] = {
                                        "step1_ts": now_ts_after_step1,
                                        "step2_issued": False,
                                    }

                    else:
                        with self.auto_state_lock:
                            pending_count = len(self.auto_w2003_pending)


                    # 清理已完成的項目
                    with self.auto_state_lock:
                        done_racks = [
                            rack for rack, state in self.auto_w2003_pending.items()
                            if state.get("w2003_issued", False)
                        ]
                        for rack in done_racks:
                            self.auto_w2003_pending.pop(rack, None)

                        # pending_count_after_cleanup = len(self.auto_w2003_pending)

                except Exception:
                    self.logging.error(
                        f"handle_autotest_w2003_after_init_and_active inner failed\n"
                        f"{self.logger.get_slim_error_log()}"
                    )

                time.sleep(scan_interval_sec)

        except Exception:
            self.logging.error(
                f"handle_autotest_w2003_after_init_and_active failed\n"
                f"{self.logger.get_slim_error_log()}"
            )

    def handle_autotest_w2003_step2_after_step1_current_below_threshold(
        self,
        measurement: str | None = None,
        current_field: str = "current",
        current_threshold: float = 3.0,
        lookback_sec: int = 15,
        scan_interval_sec: int = 60,
        step1_to_step2_delay_sec: int = 30,
        issue_sleep_sec: float = 0.5,
        slot_timeout_sec: float = 10.0,
        slot_poll_sec: float = 0.05,
        pending_expire_sec: int | None = None,
        require_all_boards_zero: bool = False,
    ):
        """
        step2 自動流程（current == 0 判斷版 / measurement 自動切天版）：

        1. rack 已成功送過 step1
        2. 等待 step1_to_step2_delay_sec 秒（可為 0）
        3. 查 InfluxDB 中該 rack 對應 serialboard 的 latest current
        4. 若符合條件，則送出：
            w2003 rack rack 2

        條件說明：
        - require_all_boards_zero=False:
            只要任一塊 board 的 current == 0，就切 step2
        - require_all_boards_zero=True:
            兩塊 board 的 current 都 == 0，才切 step2

        measurement 說明：
        - 若 measurement is None：
            每輪自動使用 self.get_current_measurement_name(prefix="data", use_utc=True)
        - 若 measurement 有指定：
            則固定使用傳入的 measurement
        """
        try:
            while(len(self.clients) <= 0):
                self.logging.error("[AUTO-W2003-S2] CCMS not connected, autotest handler stop")
                time.sleep(10)

            # round_no = 0

            while True:
                try:
                    now_ts = time.monotonic()
                    # start_time = time.monotonic()

                    current_measurement = measurement or self.get_current_measurement_name(prefix="data", use_utc=False,)

                    due_racks = []
                    expired_racks = []

                    # 先 copy snapshot，避免 iterate 時被改 dict
                    with self.auto_state_lock:
                        pending_items = list(self.auto_w2003_step2_pending.items())

                    for rack, state in pending_items:
                        step1_ts = state.get("step1_ts")
                        step2_issued = state.get("step2_issued", False)

                        if step1_ts is None or step2_issued:
                            continue

                        elapsed = now_ts - step1_ts

                        # optional: 避免 pending 永遠卡住
                        if pending_expire_sec is not None and elapsed >= pending_expire_sec:
                            expired_racks.append(rack)
                            continue

                        # 還沒到 delay
                        if elapsed < step1_to_step2_delay_sec:
                            continue

                        protectboard_ids = self.rack_to_protectboard_ids(rack)
                        if not protectboard_ids:
                            self.logging.warning(
                                f"[AUTO-W2003-S2] rack={rack} has no protectboard_id mapping, skip"
                            )
                            continue

                        board_zero_results = []
                        board_debug_info = []

                        for protectboard_id in protectboard_ids:    
                            latest = self.query_latest_board_status_flux(
                                measurement=current_measurement,
                                protectboard_id=protectboard_id,
                                field_name=current_field,
                                lookback_sec=lookback_sec,
                                exclude_qrcode_curr=True,
                            )

                            ready = False
                            current_value = None

                            if latest is not None:
                                current_value = latest.get(current_field)
                                ready = (current_value is not None and current_value < current_threshold)

                            board_zero_results.append(ready)
                            board_debug_info.append(
                                f"{protectboard_id}: {current_field}={current_value}, zero_ready={ready}, latest={latest}"
                            )

                        if require_all_boards_zero:
                            rack_ready = all(board_zero_results) if board_zero_results else False
                        else:
                            rack_ready = any(board_zero_results) if board_zero_results else False

                        if rack_ready:
                            due_racks.append(rack)

                    # 清掉過期 pending
                    if expired_racks:
                        with self.auto_state_lock:
                            for rack in expired_racks:
                                self.auto_w2003_step2_pending.pop(rack, None)

                    due_racks = sorted(due_racks)
                    due_ranges = self.merge_to_ranges(due_racks)

                    if due_racks:

                        issued_racks = []

                        for start_idx, end_idx in due_ranges:
                            issue_result = self.issue_cmd_wait_slot_empty(
                                cmd="w2003",
                                start_idx=start_idx,
                                end_idx=end_idx,
                                step=2,
                                sleep_sec=issue_sleep_sec,
                                timeout_sec=slot_timeout_sec,
                                poll_sec=slot_poll_sec,
                            )

                            if issue_result["ok"]:
                                self.influxdb_obj.write_ccs_logs_influxdb("DEBUG", f"CCS w2003 {start_idx} {end_idx} 2", "system")
                                racks = list(range(start_idx, end_idx + 1))
                                issued_racks.extend(racks)

                        # 標記完成
                        if issued_racks:
                            now_ts_after_step2 = time.monotonic()

                            with self.auto_state_lock:
                                for rack in issued_racks:
                                    if rack in self.auto_w2003_step2_pending:
                                        self.auto_w2003_step2_pending[rack]["step2_issued"] = True

                                    
                                    self.auto_w2003_step3_pending[rack] = {
                                        "step2_ts": now_ts_after_step2,
                                        "step3_issued": False,
                                    }

                    else:
                        with self.auto_state_lock:
                            pending_count = len(self.auto_w2003_step2_pending)

                    # 清理已完成的項目
                    with self.auto_state_lock:
                        done_racks = [
                            rack for rack, state in self.auto_w2003_step2_pending.items()
                            if state.get("step2_issued", False)
                        ]
                        for rack in done_racks:
                            self.auto_w2003_step2_pending.pop(rack, None)

                        # pending_count_after_cleanup = len(self.auto_w2003_step2_pending)

                except Exception:
                    self.logging.error(
                        f"handle_autotest_w2003_step2_after_step1_current_below_threshold inner failed\n"
                        f"{self.logger.get_slim_error_log()}"
                    )

                time.sleep(scan_interval_sec)

        except Exception:
            self.logging.error(
                f"handle_autotest_w2003_step2_after_step1_current_below_threshold failed\n"
                f"{self.logger.get_slim_error_log()}"
            )


    def handle_autotest_w2003_step3_after_step2_delay(
        self,
        scan_interval_sec: int = 60,
        step2_to_step3_delay_sec: int = 120,
        issue_sleep_sec: float = 0.5,
        slot_timeout_sec: float = 10.0,
        slot_poll_sec: float = 0.05,
        pending_expire_sec: int | None = None,
    ):
        """
        step3 自動流程（純時間版）：

        1. rack 已成功送過 step2
        2. 等待 step2_to_step3_delay_sec 秒（可為 0）
        3. 時間到就送出：
            w2003 rack rack 3
        """
        try:
            while(len(self.clients) <= 0):
                self.logging.error("[AUTO-W2003-S3] CCMS not connected, autotest handler stop")
                time.sleep(10)

            # round_no = 0

            while True:
                try:
                    now_ts = time.monotonic()
                    # start_time = time.monotonic()

                    due_racks = []
                    expired_racks = []

                    with self.auto_state_lock:
                        pending_items = list(self.auto_w2003_step3_pending.items())

                    for rack, state in pending_items:
                        step2_ts = state.get("step2_ts")
                        step3_issued = state.get("step3_issued", False)

                        if step2_ts is None or step3_issued:
                            continue

                        elapsed = now_ts - step2_ts

                        if pending_expire_sec is not None and elapsed >= pending_expire_sec:
                            expired_racks.append(rack)
                            continue

                        if elapsed < step2_to_step3_delay_sec:
                            continue

                        due_racks.append(rack)

                    if expired_racks:
                        with self.auto_state_lock:
                            for rack in expired_racks:
                                self.auto_w2003_step3_pending.pop(rack, None)

                    due_racks = sorted(due_racks)
                    due_ranges = self.merge_to_ranges(due_racks)

                    if due_racks:

                        # step3 下完後，該 rack 進 cooldown，cooldown 期間 task poll 掃到也不會下 w2002。
                        # self.add_racks_to_cooldown(racks=due_racks, cooldown_sec=self.step3_to_w2002_cooldown_sec,)

                        issued_racks = []

                        for start_idx, end_idx in due_ranges:
                            issue_result = self.issue_cmd_wait_slot_empty(
                                cmd="w2003",
                                start_idx=start_idx,
                                end_idx=end_idx,
                                step=3,
                                sleep_sec=issue_sleep_sec,
                                timeout_sec=slot_timeout_sec,
                                poll_sec=slot_poll_sec,
                            )

                            if issue_result["ok"]:
                                self.influxdb_obj.write_ccs_logs_influxdb("DEBUG", f"CCS w2003 {start_idx} {end_idx} 3", "system")
                                racks = list(range(start_idx, end_idx + 1))
                                issued_racks.extend(racks)

                        if issued_racks:
                            with self.auto_state_lock:
                                for rack in issued_racks:
                                    if rack in self.auto_w2003_step3_pending:
                                        self.auto_w2003_step3_pending[rack]["step3_issued"] = True
                    else:
                        with self.auto_state_lock:
                            pending_count = len(self.auto_w2003_step3_pending)

                    with self.auto_state_lock:
                        done_racks = [
                            rack for rack, state in self.auto_w2003_step3_pending.items()
                            if state.get("step3_issued", False)
                        ]
                        for rack in done_racks:
                            self.auto_w2003_step3_pending.pop(rack, None)

                        pending_count_after_cleanup = len(self.auto_w2003_step3_pending)

                except Exception:
                    self.logging.error(
                        f"handle_autotest_w2003_step3_after_step2_delay inner failed\n"
                        f"{self.logger.get_slim_error_log()}"
                    )

                time.sleep(scan_interval_sec)

        except Exception:
            self.logging.error(
                f"handle_autotest_w2003_step3_after_step2_delay failed\n"
                f"{self.logger.get_slim_error_log()}"
            )
        
    
    def remote_mode_uri(self):
        SERVER_IP = "192.168.127.99"
        PORT = "8888"
        OBJECT_ID = "battery_mode_api"
        return f"PYRO:{OBJECT_ID}@{SERVER_IP}:{PORT}"
    
    def remote_connect(self):
        try:
            with Pyro5.api.Proxy(self.remote_mode_uri()) as api:
                result = api.connect()
                self.logging.info(f"[REMOTE] connect result={result}")
                return result
        except Exception as e:
            self.logging.error(
                f"[REMOTE] connect failed | {type(e).__name__}: {e}\n"
                f"{self.logger.get_slim_error_log()}"
            )
            return None


    def remote_mode_battery_entry(self):
        try:
            with Pyro5.api.Proxy(self.remote_mode_uri()) as api:
                result = api.mode_battery_entry()
                self.logging.info(f"[REMOTE] mode_battery_entry result={result}")
                return result
        except Exception as e:
            self.logging.error(
                f"[REMOTE] mode_battery_entry failed | {type(e).__name__}: {e}\n"
                f"{self.logger.get_slim_error_log()}"
            )
            return None


    def remote_mode_cc_charge(self, timer: int = 0):
        try:
            with Pyro5.api.Proxy(self.remote_mode_uri()) as api:
                result = api.mode_cc_charge(timer)
                self.logging.info(f"[REMOTE] mode_cc_charge timer={timer}, result={result}")
                return result
        except Exception as e:
            self.logging.error(
                f"[REMOTE] mode_cc_charge failed | timer={timer} | {type(e).__name__}: {e}\n"
                f"{self.logger.get_slim_error_log()}"
            )
            return None


    def remote_mode_idle(self, timer: int = 0):
        try:
            with Pyro5.api.Proxy(self.remote_mode_uri()) as api:
                result = api.mode_idle(timer)
                self.logging.info(f"[REMOTE] mode_idle timer={timer}, result={result}")
                return result
        except Exception as e:
            self.logging.error(
                f"[REMOTE] mode_idle failed | timer={timer} | {type(e).__name__}: {e}\n"
                f"{self.logger.get_slim_error_log()}"
            )
            return None


    def remote_mode_battery_exit(self):
        try:
            with Pyro5.api.Proxy(self.remote_mode_uri()) as api:
                result = api.mode_battery_exit()
                self.logging.info(f"[REMOTE] mode_battery_exit result={result}")
                return result
        except Exception as e:
            self.logging.error(
                f"[REMOTE] mode_battery_exit failed | {type(e).__name__}: {e}\n"
                f"{self.logger.get_slim_error_log()}"
            )
            return None


if __name__ == "__main__":
    server = None
    try:
        with open("core\\main\\config.json") as json_file:
            config_data = json.load(json_file)
            # Create xml socket server thread
            server = XMLSocketServer(config_data["ccs_ip"], config_data["ccs_port"])

            # 1. 啟動 XML socket server
            server_thread = threading.Thread(target=server.run, daemon=True)
            server_thread.start()

            # if os.environ.get("CCMS_AUTO_TASK_POLL", "0") == "1":
            #     task_poll_thread = threading.Thread(
            #         target=server.handle_poll_expected_task_ready_by_rack_count,
            #         kwargs={
            #             "rack_count": int(os.environ.get("CCMS_AUTO_TASK_POLL_RACK_COUNT", "84")),
            #             "poll_interval_sec": int(os.environ.get("CCMS_AUTO_TASK_POLL_INTERVAL_SEC", "300")),
            #             "issue_sleep_sec": float(os.environ.get("CCMS_AUTO_TASK_POLL_ISSUE_SLEEP_SEC", "0.5")),
            #         },
            #         daemon=True,
            #     )
            #     task_poll_thread.start()

            # if os.environ.get("CCMS_AUTO_W2003_STEP1", "0") == "1":
            #     auto_w2003_step1_thread = threading.Thread(
            #         target=server.handle_autotest_w2003_after_init_and_active,
            #         kwargs={
            #             "voltage_field": os.environ.get("CCMS_AUTO_W2003_STEP1_VOLTAGE_FIELD", "voltage"),
            #             "lookback_sec": int(os.environ.get("CCMS_AUTO_W2003_STEP1_LOOKBACK_SEC", "15")),
            #             "scan_interval_sec": int(os.environ.get("CCMS_AUTO_W2003_STEP1_SCAN_INTERVAL_SEC", "60")),
            #             "init_to_step1_delay_sec": int(os.environ.get("CCMS_AUTO_W2003_STEP1_DELAY_SEC", "30")),
            #             "require_all_boards_ready": os.environ.get("CCMS_AUTO_W2003_STEP1_REQUIRE_ALL_BOARDS_READY", "0",) == "1",
            #         },
            #         daemon=True,
            #     )
            #     auto_w2003_step1_thread.start()

            # if os.environ.get("CCMS_AUTO_W2003_STEP2", "0") == "1":
            #     auto_w2003_step2_thread = threading.Thread(
            #         target=server.handle_autotest_w2003_step2_after_step1_current_below_threshold,
            #         kwargs={
            #             "current_field": os.environ.get("CCMS_AUTO_W2003_STEP2_CURRENT_FIELD", "current"),
            #             "current_threshold": float(os.environ.get("CCMS_AUTO_W2003_STEP2_CURRENT_THRESHOLD", "3.0")),
            #             "lookback_sec": int(os.environ.get("CCMS_AUTO_W2003_STEP2_LOOKBACK_SEC", "15")),
            #             "scan_interval_sec": int(os.environ.get("CCMS_AUTO_W2003_STEP2_SCAN_INTERVAL_SEC", "60")),
            #             "step1_to_step2_delay_sec": int(os.environ.get("CCMS_AUTO_W2003_STEP2_DELAY_SEC", "60")),
            #             "require_all_boards_zero": os.environ.get("CCMS_AUTO_W2003_STEP2_REQUIRE_ALL_BOARDS_ZERO", "0",) == "1",
            #         },
            #         daemon=True,
            #     )
            #     auto_w2003_step2_thread.start()

            # if os.environ.get("CCMS_AUTO_W2003_STEP3", "0") == "1":
            #     auto_w2003_step3_thread = threading.Thread(
            #         target=server.handle_autotest_w2003_step3_after_step2_delay,
            #         kwargs={
            #             "scan_interval_sec": int(os.environ.get("CCMS_AUTO_W2003_STEP3_SCAN_INTERVAL_SEC", "60")),
            #             "step2_to_step3_delay_sec": int(os.environ.get("CCMS_AUTO_W2003_STEP3_DELAY_SEC", "60",)),
            #         },
            #         daemon=True,
            #     )
            #     auto_w2003_step3_thread.start()

            task_poll_thread = threading.Thread(
                target=server.handle_poll_expected_task_ready_by_rack_count,
                kwargs={
                    "rack_count": 20,
                    "poll_interval_sec": 300,
                    "issue_sleep_sec": 0.5,
                },
                daemon=True,
            )
            task_poll_thread.start()

            # 3. auto w2003 step1：w2002 後等 N 秒，query latest，有資料就切 step1
            auto_w2003 = threading.Thread(
                target=server.handle_autotest_w2003_after_init_and_active,
                kwargs={
                    "measurement": None,              # 自動用當天 data_YYYYMMDD
                    "voltage_field": "voltage",
                    "lookback_sec": 15,
                    "scan_interval_sec": 60,
                    "init_to_step1_delay_sec": 30,
                    "issue_sleep_sec": 0.5,
                    "slot_timeout_sec": 10.0,
                    "slot_poll_sec": 0.05,
                    # "pending_expire_sec": 600,
                    "pending_expire_sec": None,
                    "require_all_boards_ready": False,
                },
                daemon=True,
            )
            auto_w2003.start()
            
           # 4. auto w2003 step2：step1 後等 N 秒，query current 低於 threshold，再切 step2
            auto_w2003_s2 = threading.Thread(
                target=server.handle_autotest_w2003_step2_after_step1_current_below_threshold,
                kwargs={
                    "measurement": None,                 # 自動用當天 data_YYYYMMDD
                    "current_field": "current",
                    "current_threshold": 5.0,
                    "lookback_sec": 15,
                    "scan_interval_sec": 60,
                    "step1_to_step2_delay_sec": 600,      # <- 可參數化
                    "issue_sleep_sec": 0.5,
                    "slot_timeout_sec": 10.0,
                    "slot_poll_sec": 0.05,
                    # "pending_expire_sec": 600,
                    "pending_expire_sec": None,
                    "require_all_boards_zero": False,    # 任一塊 current < threshold 就切；若要更嚴格改 True
                },
                daemon=True,
            )
            auto_w2003_s2.start()
            
            auto_w2003_s3 = threading.Thread(
                target=server.handle_autotest_w2003_step3_after_step2_delay,
                kwargs={
                    "scan_interval_sec": 60,
                    "step2_to_step3_delay_sec": 600,   # <- 可參數化
                    "issue_sleep_sec": 0.5,
                    "slot_timeout_sec": 10.0,
                    "slot_poll_sec": 0.05,
                    # "pending_expire_sec": 600,
                    "pending_expire_sec": None,
                },
                daemon=True,
            )
            auto_w2003_s3.start()

            # 5. 保留手動輸入
            keyboard_thread = threading.Thread(target=server.handle_input_cmd, daemon=True)
            keyboard_thread.start()
            keyboard_thread.join()

    except Exception:
        logging.error("Main function failed")

    finally:
        if server is not None:
            try:
                server.influxdb_obj.close_connect()
            except Exception:
                pass