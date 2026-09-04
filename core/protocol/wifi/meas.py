import pandas as pd
import json
import pytz

# User define code
import random
from datetime import datetime
import time
import struct
import os
import sys
import traceback

"""
Measurement Method
  Sensing device data
"""

tz = pytz.timezone("Asia/Taipei")
voltage = 0
temperature = 0
ChInSeries = 6


class meas_api:
    def __init__(self, vars, db_out, influxdb_obj, mssql_obj, sbid, shid):
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from balps.log_mgr import logging_api

        """
        Setting configuration and db connection

        Args:
            vars: Constant variable (FlowControl.json)
            db_out: Table name and column info (FlowControl.json)
            meas_map_obj: Measurement mapping table object
            influxdb_obj: Influx db object
            sbid: Serial board id

        Returns:
            None

        Example:
            >>> meas_obj = meas_api()
        """
        try:
            self.logger = logging_api(filename=os.path.join("logs", f"{os.path.splitext(os.path.basename(sys.argv[0]))[0]}"), backup_count=7)
            self.logging = self.logger.get_logger()  # Create influxdb and mssql object
            self.influxdb_obj = influxdb_obj
            self.mssql_obj = mssql_obj
            self.sbid = sbid
            self.shid = shid
            if not vars:
                with open("config.json") as json_file:
                    self.config_data = json.load(json_file)
                self.vars = self.config_data["meas"][0]["variable"]
                self.db_out = self.config_data["meas"][0]["db_out"]
            else:
                self.vars = vars
                self.db_out = db_out
            self.meas_map_obj = ""
            self.config_map = {}
            self.mode = "Init"
            self.step = 0
            self.last_heartbeat_timeout_time = time.time()
            self.last_heartbeat_sent_time = time.time()
            self.backup_data = ""
            self.serialboard_id = ""
            self.storehouse_id = ""
            self.mpd_heartbeat_timeout = self.vars["mpd_heartbeat_timeout"]
            self.last_time = time.time()
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"Measurement method initialization failed {self.sbid}\n{self.logger.get_slim_error_log()}", self.sbid)

    # def set_disconnect_times(self):
    #     self.disconnect_times = 20

    def set_configuration(self, meas_map_obj, mode):
        self.meas_map_obj = meas_map_obj
        self.mode = mode

    def set_heartbeat_timeout(self):
        self.last_heartbeat_timeout_time = time.time()
        self.influxdb_obj.write_log_influxdb("DEBUG", f"Set heartbeat timeout {time.time()}", self.sbid)

    def run_meas(self, conn_obj):  # noqa: C901
        """
        Running measurement method

        Args:
            conn_obj: Measurement device socket connection object

        Returns:
            res_result: Send back ack message
            meas_out_pd: Write measurement data back to the database

        Example:
            >>> meas_obj = meas_api()
            >>> meas_obj.run_meas(conn_obj)
        """
        try:
            current_time = time.time()
            if current_time - self.last_heartbeat_sent_time >= 5:
                tid = datetime.now().strftime("%Y%m%d%H%M%S%f")[:17]
                hb_data = {"Msg": {"0": "HeartbeatRequest"}, "Tid": {"0": tid}}
                hb_data = json.dumps(hb_data).encode("utf-8")
                conn_obj.settimeout(1)
                conn_obj.sendall(struct.pack(">I", len(hb_data)) + hb_data)
                # self.logging.info(f"CCMS heartbeat request {self.sbid} {tid}")
                self.influxdb_obj.write_log_influxdb("DEBUG", f"CCMS2MPD heartbeat request [connected state] {self.sbid}", self.sbid)
                self.last_heartbeat_sent_time = current_time
            if current_time - self.last_heartbeat_timeout_time >= self.mpd_heartbeat_timeout:
                self.influxdb_obj.write_log_influxdb("ERROR", f"CCMS2MPD heartbeat timeout [connected state] {self.sbid} {current_time} {self.last_heartbeat_timeout_time}", self.sbid)
                self.logging.error(f"CCMS2MPD heartbeat timeout [connected state] {self.sbid}")
                self.mssql_obj.clear_task_table(is_all_clear=False, sb_id=self.sbid)
                time.sleep(3)
                conn_obj.close()
                raise RuntimeError("Connection Closed")

            # if self.meas_map_obj:
            # Nothing to do
            # if self.mode == "Init":
            #     return None, None

            data = []
            T2Timer = time.time()
            T2Timer_s = round(T2Timer - 0.5)
            T2Timer_ms = int((round(T2Timer, 3) - T2Timer_s) * 1000)

            request = ""
            # Send config data to measurement device
            if self.mode == "Config":
                self.config_map = json.loads(self.meas_map_obj.get_config_map().to_json(orient="columns"))
                self.serialboard_id = self.config_map.get("SerialBoardID")["0"]
                self.protectboard_id = self.config_map.get("ProtectBoardID")["0"]
                self.storehouse_id = self.config_map.get("StoreHouseID")["0"]
                self.config_map.pop("StoreHouseID", None)
                self.config_map.pop("SerialBoardID", None)
                request = self.config_map
                request["StoreHouseID"] = {"0": self.storehouse_id}
                request["SerialBoardID"] = {"0": self.serialboard_id}
                request["ProtectBoardID"] = {"0": self.protectboard_id}
                request["TimerT2ms"] = {"0": T2Timer_ms}
                request["TimerT2s"] = {"0": T2Timer_s}
                # request["SerialBoardID"] = {"0": f"{self.serialboard_id}"}
                request["Msg"] = {"0": "Config"}
                request["Step"] = {"0": 0}
            # Send charging state check to measurement device
            elif self.mode == "Check":
                task_info = self.meas_map_obj.get_task_info()
                self.step = int(task_info.get("Step"))
                request = {
                    "TimerT2ms": {"0": T2Timer_ms},
                    "TimerT2s": {"0": T2Timer_s},
                    "SerialBoardID": {"0": f"{self.serialboard_id}"},
                    "Msg": {"0": "Check"},
                    "Step": {"0": self.step},
                }
            # Send socket message
            if request != "":
                self.influxdb_obj.write_log_influxdb("INFO", f"[SendMPD]\n {request}", self.sbid)
                self.influxdb_obj.write_json_influxdb("INFO", "[SendMPD]\n" + self.influxdb_obj.reformat_json_horizontal(request), "request", self.sbid, f"SendMPD{self.mode}")
                send_data = json.dumps(request).encode("utf-8")
                conn_obj.settimeout(1)
                conn_obj.sendall(struct.pack(">I", len(send_data)) + send_data)
                if self.mode == "Config":
                    self.mode = "Wait"
                elif self.mode == "Check":
                    self.mode = "Polling"
                # self.logging.error(f"{send_data}")

            data = b""
            try:
                conn_obj.settimeout(0.01)
                header = conn_obj.recv(4)
                if header != b"":
                    length = struct.unpack(">I", header)[0]
                    recv_data = conn_obj.recv(length)
                    length = length - len(recv_data)
                    data += recv_data
                    while length > 0:
                        recv_data = conn_obj.recv(length)
                        if not recv_data:
                            raise ConnectionError("Connection Failed")
                        length = length - len(recv_data)
                        data += recv_data
                    # print(f"Recv Data: {data}")
                if data:
                    response = {
                        "TimerT2ms": {"0": T2Timer_ms},
                        "TimerT2s": {"0": T2Timer_s},
                        "SerialBoardID": {"0": f"{self.serialboard_id}"},
                        "Msg": {"0": "Polling"},
                        "Step": {"0": self.step},
                    }
                    self.influxdb_obj.write_log_influxdb("INFO", f"[RecvMPD]\n {data}", self.sbid)
                    self.backup_data = data
                    data = json.loads(data.decode())
                    self.influxdb_obj.write_json_influxdb("INFO", "[RecvMPD]\n" + self.influxdb_obj.reformat_json_horizontal(data), "request", self.sbid, "RecvMPD")
                    rsp = {
                        "DataType": {},
                        "ReturnCode": {"0": data.get("ReturnCode", {}).get("0", "")},
                        "PalletID": {},
                        "ProtectBoardID": {"0": ""},
                        "Position": {},
                        "ErrorCode": {},
                        "QRCodeID": {},
                        "Status": {"0": data.get("Status", {}).get("0", "")},
                        "RSSI": {"0": data.get("RSSI", {}).get("0", "")},
                        "WarCnt": {"0": data.get("WarCnt", {}).get("0", "")},
                    }
                    # self.logging.info(f"{json.dumps(data, indent=4)}")
                    if data.get("Msg")["0"] == "HeartbeatRequest":
                        hb_ack = {"Msg": {"0": "HeartbeatAck"}}
                        hb_ack = json.dumps(hb_ack).encode("utf-8")
                        conn_obj.settimeout(1)
                        conn_obj.sendall(struct.pack(">I", len(hb_ack)) + hb_ack)
                        # self.logging.info(f"ESP heartbeat request {self.sbid}") #{data.get("Tid")["0"]}
                        self.influxdb_obj.write_log_influxdb("DEBUG", f"ESP2CCMS heartbeat request [connected state] {self.sbid}", self.sbid)
                    elif data.get("Msg")["0"] == "HeartbeatAck":
                        # self.logging.debug(f"ESP heartbeat ack {self.sbid} {time.time() - self.last_heartbeat_sent_time:.02f}")
                        self.influxdb_obj.write_perf_influxdb("HeartbeatRspTime", float(f"{time.time() - self.last_heartbeat_sent_time:.02f}"), self.sbid, self.shid)
                        self.last_heartbeat_timeout_time = time.time()
                    # Return measurement data
                    elif data.get("Msg")["0"] == "Data":
                        monitor_data = []
                        position_value_to_key = {v: k for k, v in data.get("Position", {}).items()}
                        sorted_position_values = sorted(position_value_to_key.keys())
                        for idx in range(1, ChInSeries + 1):
                            # Get cell QRCode
                            meas_map_res = self.meas_map_obj.query_meas_map_by_serialboard(data.get("SerialBoardID")["0"], idx)
                            # Merge measurement data
                            monitor_data.append(self.data_reorganization(meas_map_res, data, idx, position_value_to_key, sorted_position_values))

                        # curr OK status
                        # current_position = 7
                        # meas_map_res = self.meas_map_obj.query_meas_map_by_serialboard(data.get("SerialBoardID", {}).get("0"), 1)
                        # monitor_data.append(self.data_reorganization(meas_map_res, data, current_position, [], []))

                        self.influxdb_obj.write_json_influxdb("INFO", "[DataAckMPD]\n" + self.influxdb_obj.reformat_json_horizontal(response), "request", self.sbid, "DataAckMPD")
                        self.influxdb_obj.write_json_influxdb("INFO", "[NormalData]\n" + self.influxdb_obj.format_df_mixed(pd.DataFrame(monitor_data)), "request", self.sbid, "NormalData")
                        # self.influxdb_obj.write_json_influxdb("INFO",json.dumps(response, indent=4))
                        send_data = json.dumps(response).encode("utf-8")
                        conn_obj.settimeout(1)
                        conn_obj.sendall(struct.pack(">I", len(send_data)) + send_data)
                        rsp["DataType"]["0"] = "DATA"
                        meas_map_res = self.meas_map_obj.query_meas_map_by_serialboard(data.get("SerialBoardID", {}).get("0"), 1)
                        rsp["PalletID"]["0"] = "" if not meas_map_res else meas_map_res["PalletID"]
                        rsp["ProtectBoardID"]["0"] = data.get("ProtectBoardID", {}).get("0")
                        return rsp, pd.DataFrame(monitor_data)
                    # Return ACK request
                    elif data.get("Msg")["0"] == "ACK":
                        rsp["DataType"]["0"] = "ACK"
                        for pos_key, pos_value in data.get("Position", {}).items():
                            meas_map_res = self.meas_map_obj.query_meas_map_by_serialboard(data.get("SerialBoardID", {}).get("0"), int(pos_value))
                            rsp["Position"][pos_key] = 0  # pos_value
                            rsp["QRCodeID"][pos_key] = ""  # None if not meas_map_res else meas_map_res['QRCodeID']
                            rsp["PalletID"]["0"] = "" if not meas_map_res else meas_map_res["PalletID"]
                        rsp["ProtectBoardID"]["0"] = data.get("ProtectBoardID", {}).get("0")
                        return rsp, None
                    # Return alarm request
                    elif data.get("Msg")["0"] == "ALARM":
                        rsp["DataType"]["0"] = "ALARM"
                        monitor_data = []
                        for pos_key, pos_value in data.get("Position", {}).items():
                            meas_map_res = self.meas_map_obj.query_meas_map_by_serialboard(data.get("SerialBoardID")["0"], int(pos_value))
                            rsp["Position"][pos_key] = pos_value
                            rsp["ErrorCode"][pos_key] = data.get("ErrorCode", {}).get(pos_key, None)
                            rsp["QRCodeID"][pos_key] = None if not meas_map_res else meas_map_res["QRCodeID"]
                            rsp["PalletID"]["0"] = None if not meas_map_res else meas_map_res["PalletID"]
                        rsp["ProtectBoardID"]["0"] = data.get("ProtectBoardID", {}).get("0")
                        position_value_to_key = {v: k for k, v in data.get("Position", {}).items()}
                        sorted_position_values = sorted(position_value_to_key.keys())
                        for idx in range(1, ChInSeries + 1):
                            # Get cell QRCode
                            meas_map_res = self.meas_map_obj.query_meas_map_by_serialboard(data.get("SerialBoardID")["0"], idx)
                            # Merge measurement data
                            monitor_data.append(self.data_reorganization(meas_map_res, data, idx, position_value_to_key, sorted_position_values))

                        # current_position = 7
                        # if current_position in position_value_to_key:
                        #     if data.get("ErrorCode", {}).get(position_value_to_key[current_position]) in ("OC", "UC"):
                        #         meas_map_res = self.meas_map_obj.query_meas_map_by_serialboard(data.get("SerialBoardID")["0"], 1)
                        #         monitor_data.append(self.data_reorganization(meas_map_res, data, current_position, position_value_to_key, sorted_position_values))

                        self.influxdb_obj.write_json_influxdb("INFO", "[AlarmAckESP]\n" + self.influxdb_obj.reformat_json_horizontal(response), "request", self.sbid, "AlarmAckESP")
                        self.influxdb_obj.write_json_influxdb("INFO", "[AlarmData]\n" + self.influxdb_obj.format_df_mixed(pd.DataFrame(monitor_data)), "request", self.sbid, "AlarmData")
                        # self.influxdb_obj.write_json_influxdb("INFO",json.dumps(response, indent=4))
                        # send_data = json.dumps(response).encode("utf-8")
                        # conn_obj.settimeout(1)
                        # conn_obj.sendall(struct.pack(">I", len(send_data)) + send_data)
                        return rsp, pd.DataFrame(monitor_data)
                return None, None
            except TimeoutError:
                return None, None
            except ConnectionResetError:
                raise
            except Exception:
                self.influxdb_obj.write_log_influxdb("ERROR", f"run_meas failed {self.sbid}\n{self.logger.get_slim_error_log()}", self.sbid)
                raise
        except (OSError, ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            self.influxdb_obj.write_perf_influxdb("SocketDisconnect", 1.5, self.sbid, self.shid)
            self.influxdb_obj.write_log_influxdb("ERROR", f"run_meas socket failed {self.sbid}\n{self.logger.get_slim_error_log()}", self.sbid)
            conn_obj.close()
            raise
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"run_meas failed {self.sbid}\n{self.logger.get_slim_error_log()}", self.sbid)
            if hasattr(self, "backup_data"):
                self.influxdb_obj.write_log_influxdb("ERROR", f"{self.backup_data}", self.sbid)
            self.influxdb_obj.write_log_influxdb("ERROR", f"{traceback.format_exc()}")
            # self.logging.info(data)
            raise

    def data_reorganization(self, meas_map_res, data, idx, position_value_to_key, sorted_position_values):
        """
        Reorganize measurement data for influx db

        Args:
            meas_map_res: Measurement mapping table result
            data: Measurement device data
            idx: Channel index
            position_value_to_key: Mapping of position values to keys
            sorted_position_values: Sorted list of position values

        Returns:
            None

        Example:
            >>> meas_obj = meas_api()
            >>> meas_obj.data_reorganization(meas_map_res, data, idx)
        """
        try:
            result = {}
            result[self.db_out[0]["field"][0]] = datetime.fromtimestamp(data.get("TimerT1s", {}).get("0", 0))
            result[self.db_out[0]["field"][1]] = meas_map_res["StoreHouseID"]
            result[self.db_out[0]["field"][2]] = self.config_map.get("PalletPosition")["0"]
            result[self.db_out[0]["field"][3]] = data.get("SerialBoardID", {}).get("0", 0)
            result[self.db_out[0]["field"][4]] = int(idx)
            result[self.db_out[0]["field"][40]] = data.get("Status", {}).get("0", 0)
            result[self.db_out[0]["field"][5]] = meas_map_res["QRCodeID"]
            result[self.db_out[0]["field"][6]] = {
                0: "OK",
                1: "NG",
                2: "Water"
            }.get(int(data.get("ReturnCode", {}).get("0", 0)), "Undefine")
            result[self.db_out[0]["field"][7]] = float(data.get("Volt", {}).get(str(idx - 1), 0) / 1000)
            result[self.db_out[0]["field"][8]] = float((data.get("Temp", {}).get(str(idx - 1), 0)) / 10)
            result[self.db_out[0]["field"][9]] = data.get("Curr", {}).get("0", 0) / 1000
            result[self.db_out[0]["field"][11]] = data.get("ProtectBoardID", {}).get("0", 0)
            result[self.db_out[0]["field"][13]] = data.get("FETState", {}).get("0", 0)
            result[self.db_out[0]["field"][14]] = data.get("Fuse", {}).get("0", 0)
            result[self.db_out[0]["field"][15]] = data.get("AFE", {}).get("0", 0)
            result[self.db_out[0]["field"][16]] = data.get("WifiSTDisConn", {}).get("0", 0)
            result[self.db_out[0]["field"][17]] = data.get("WifiLTDisConn", {}).get("0", 0)
            result[self.db_out[0]["field"][18]] = data.get("SocketSTDisConn", {}).get("0", 0)
            result[self.db_out[0]["field"][19]] = data.get("SocketLTDisConn", {}).get("0", 0)
            result[self.db_out[0]["field"][20]] = data.get("HeartbeatTxCount", {}).get("0", 0)
            result[self.db_out[0]["field"][21]] = data.get("HeartbeatLossCount", {}).get("0", 0)
            result[self.db_out[0]["field"][22]] = data.get("HeartbeatLastRttMs", {}).get("0", 0)
            result[self.db_out[0]["field"][23]] = data.get("HeartbeatRttMaxMs", {}).get("0", 0)
            result[self.db_out[0]["field"][24]] = data.get("HeatbeatTimeoutFlag", {}).get("0", 0)
            result[self.db_out[0]["field"][25]] = data.get("WifiReconnLastMs", {}).get("0", 0)
            result[self.db_out[0]["field"][26]] = data.get("WifiReconnAvgMs", {}).get("0", 0)
            result[self.db_out[0]["field"][27]] = data.get("WifiReconnTimes", {}).get("0", 0)
            result[self.db_out[0]["field"][28]] = data.get("SocketReconnLastMs", {}).get("0", 0)
            result[self.db_out[0]["field"][29]] = data.get("SocketReconnAvgMs", {}).get("0", 0)
            result[self.db_out[0]["field"][30]] = data.get("SocketReconnMaxMs", {}).get("0", 0)
            result[self.db_out[0]["field"][31]] = data.get("SocketReconnTimes", {}).get("0", 0)
            result[self.db_out[0]["field"][38]] = data.get("ErrorValue", {}).get("0", -9999)
            wire_volt = data.get("WireVolt", {})
            result[self.db_out[0]["field"][33]] = wire_volt.get("0", 0)
            result[self.db_out[0]["field"][34]] = wire_volt.get("1", 0)
            result[self.db_out[0]["field"][35]] = wire_volt.get("2", 0)
            result[self.db_out[0]["field"][36]] = wire_volt.get("3", 0)
            result[self.db_out[0]["field"][37]] = wire_volt.get("4", 0)
            values = [
                wire_volt.get("0", 0),
                wire_volt.get("1", 0),
                wire_volt.get("2", 0),
                wire_volt.get("3", 0),
                wire_volt.get("4", 0)
            ]
            abnormal = [(v >= int(self.config_map.get("Cell_Delta_Wire_Voltage")["0"])) for v in values]
            error_pairs = []

            for i in range(len(abnormal)):
                if abnormal[i]:
                    error_pairs.append(f"[CH{i+1},CH{i+2} NG] ")

            result[self.db_out[0]["field"][12]] = " ".join(error_pairs) if error_pairs else "OK"

            if 7 in sorted_position_values:
                error_code_key = position_value_to_key.get(7)
                result[self.db_out[0]["field"][32]] = data.get("ErrorCode", {}).get(error_code_key, 0) if error_code_key is not None else 0
                result[self.db_out[0]["field"][39]] = data.get("ErrorValue", {}).get(error_code_key, 0)  if error_code_key is not None else -9999
            else:
                result[self.db_out[0]["field"][32]] = "OK"
                result[self.db_out[0]["field"][39]] = -9999

            if idx in sorted_position_values:
                error_code_key = position_value_to_key.get(idx)
                result[self.db_out[0]["field"][10]] = data.get("ErrorCode", {}).get(error_code_key, 0) if error_code_key is not None else 0
            else:
                result[self.db_out[0]["field"][10]] = "OK"

            # result[self.db_out[0]["field"][11]] = data.get("ProtectBoardID")["0"]

            return result

        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"data_reorganization failed {self.sbid}\n{self.logger.get_slim_error_log()}", self.sbid)
            raise
