import ctypes
import socket
import time
import json
import sys
import os
import random
import struct
import traceback
from datetime import datetime
import threading
from concurrent.futures import ThreadPoolExecutor
import logging


class ESP_simulation:
    def __init__(self):
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from balps.log_mgr import logging_api

        """
        ESP Simulation

        Args:
            None

        Returns:
            None

        Example:
            >>> esp_simulation_obj = ESP_simulation()
        """
        try:
            self.logger = logging_api(
                filename=os.path.join("logs", f"{os.path.splitext(os.path.basename(sys.argv[0]))[0]}"),
                level=logging.DEBUG,
                backup_count=7,
            )
            self.logging = self.logger.get_logger()  # Create influxdb and mssql object
            with open("core\\main\\config.json") as json_file:
                config_data = json.load(json_file)
                self.host = config_data["ccms_ip"]
                self.port_start = 20001
                self.port_end = 20168
                self.inactive_ports = list(range(self.port_start, self.port_end + 1))
                self.active_ports = []
                self.sockets = {}
                self.voltage = {}
                self.temperature = {}
                self.water_count = {}
                # self.disconn_val = {}
                self.mode = {}
                self.step = {}
                self.active_serial_board_list = {}
                self.active_protect_board_list = {}
                self.Perf_flag = False
                self.protect_config = {}
                self.last_heartbeat_timeout_time = {}
                self.last_heartbeat_sent_time = {}
                self.last_sent_data_time = {}
                self.heartbeat_current = {}
                self.heartbeat_ack = {}
                self.last_time = {}
                self.last_send_time = {}
                self.connect_threads = {}
                self.connect_stop_event = threading.Event()
        except Exception:
            self.logging.error(f"Esp initialization failed\n{self.logger.get_slim_error_log()}")

    def try_connect(self, port):
        """
        Connect ccms measurement flow control

        Args:
            port: Connect port

        Returns:
            None

        Example:
            >>> esp_simulation_obj = ESP_simulation()
            >>> esp_simulation_obj.try_connect(port)
        """
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.settimeout(0.01)
            sock.connect((self.host, port))
            self.sockets[port] = sock
            self.voltage[port] = 3.1
            self.temperature[port] = 23
            # Trigger w1001 Times for Water
            self.water_count[port] = 10
            # self.disconn_val[port] = 1000
            if port not in self.mode:
                self.mode[port] = "Init"
                self.step[port] = 0
                self.last_send_time[port] = time.time()
            self.last_heartbeat_timeout_time[port] = time.time()
            self.last_heartbeat_sent_time[port] = time.time()
            self.active_protect_board_list[port] = f"PCB{port}"
            self.last_sent_data_time[port] = time.time()
            self.last_time[port] = time.time()
            # self.logging.info(f"Port {port} connected successfully")
            return port
        except BlockingIOError:
            if sock is not None:
                sock.close()
            return None
        except (socket.timeout, TimeoutError):
            if sock is not None:
                sock.close()
                time.sleep(2)
            return None
        except Exception:
            self.logging.error(
                f"send_and_receive failed | Port: {port}\n"
                f"{self.logger.get_slim_error_log()}"
            )
            sock = self.sockets.pop(port, None)

            if sock is not None:
                try:
                    sock.close()
                except Exception:
                    pass
            return

    def poll_ports(self):
        """
        Connect ccms measurement flow control

        Args:
            port: Connect port

        Returns:
            None

        Example:
            >>> esp_simulation_obj = ESP_simulation()
            >>> esp_simulation_obj.try_connect(port)
        """
        try:
            if self.Perf_flag:
                start_time = time.time()
            # Try to Connect CCMS Measurement Using 30 Threads
            with ThreadPoolExecutor(max_workers=100) as executor:
                results = list(executor.map(self.try_connect, self.inactive_ports))
            # Set Active Port
            for port in results:
                if port is not None:
                    self.active_ports.append(port)
                    self.inactive_ports.remove(port)
            if self.Perf_flag:
                self.logging.debug(f"POLL TIME: {time.time() - start_time:.6f} s")
        except Exception:
            self.logging.error(f"poll_ports failed\n{self.logger.get_slim_error_log()}")

    def send_data(self, data, port, is_delay):
        try:
            current_time = time.time()
            elapsed = current_time - self.last_sent_data_time[port]

            if is_delay:
                if elapsed <= 1:
                    return
                self.last_sent_data_time[port] = current_time

            payload = json.dumps(data).encode("utf-8")
            self.sockets[port].settimeout(1)
            self.sockets[port].sendall(
                struct.pack(">I", len(payload)) + payload
            )

        except Exception:
            self.logging.error(
                f"send_data failed | Port: {port}\n"
                f"{self.logger.get_slim_error_log()}"
            )

    def send_and_receive(self, port):  # noqa: C901
        """
        Send and receive measurement configuration alert and ACK data

        Args:
            port: Connect port

        Returns:
            None

        Example:
            >>> esp_simulation_obj = ESP_simulation()
            >>> esp_simulation_obj.send_and_receive()
        """
        while True:
            try:
                current_time = time.time()
                if current_time - self.last_heartbeat_sent_time[port] >= 5:
                    hb_req = {"Msg": {"0": "HeartbeatRequest"}}
                    hb_req = json.dumps(hb_req).encode("utf-8")
                    self.sockets[port].settimeout(1)
                    self.sockets[port].sendall(struct.pack(">I", len(hb_req)) + hb_req)
                    # self.logging.debug(f"ESP2CCMS heartbeat request [connected state] {port}")
                    self.last_heartbeat_sent_time[port] = current_time
                if current_time - self.last_heartbeat_timeout_time[port] >= 60:
                    self.logging.error(f"ESP2CCMS heartbeat timeout [connected state] {port}")
                    sock = self.sockets.pop(port, None)
                    if sock is not None:
                        try:
                            sock.close()
                        except Exception:
                            pass
                    break

                T1Timer = time.time()
                T1Timer_s = round(T1Timer - 0.5)
                T1Timer_ms = int((round(T1Timer, 3) - T1Timer_s) * 1000)
                # Send w1001 Water Alarm
                if self.water_count[port] <= 0 and port in {
                    20099,
                    20100,
                    20101,
                    20102,
                }:
                    self.mode[port] = "Stop"
                    if time.time() - self.last_send_time[port] > 3:
                        voltage_change = round(random.uniform(0.01, 0.03), 3)  # noqa: S311
                        self.voltage[port] = min(self.voltage[port] + voltage_change, 4.085)
                        temperature_change = round(random.uniform(0.03, 0.05), 2)  # noqa: S311
                        self.temperature[port] = min(self.temperature[port] + temperature_change, 24.5)
                        self.last_send_time[port] = time.time()
                    if port == 20099:
                        response_data = {
                            "Msg": {"0": "ALARM"},
                            "ReturnCode": {"0": 2},
                            "SerialBoardID": {"0": f"{self.active_serial_board_list[port]}"},
                            "ProtectBoardID": {"0": f"{self.active_protect_board_list[port]}"},
                            "TimerT1s": {"0": T1Timer_s},
                            "TimerT1ms": {"0": T1Timer_ms},
                            "Status": {"0": 134184960},
                            "Position": {
                                "0": 1,
                                "1": 2,
                                "2": 3,
                                "3": 4,
                                "4": 5,
                                "5": 6,
                                "6": 7
                            },
                            "ErrorCode": {
                                "0": "OV",
                                "1": "UV",
                                "2": "OVS",
                                "3": "OT",
                                "4": "OTS",
                                "5": "OTW",
                                "6": "OC"
                            },
                            "ErrorValue": {
                                "0": "4210",
                                "1": "0",
                                "2": "412",
                                "3": "462",
                                "4": "62",
                                "5": "912",
                                "6": "2620"
                            },
                            "Volt": {
                                "0": int(round(random.uniform(4.18, 4.2), 2) * 1000),
                                "1": int(0 * 1000),
                                "2": int(self.voltage[port] * 1.004 * 1000),
                                "3": int(self.voltage[port] * 1000),  # noqa: S311
                                "4": int(self.voltage[port] * 1000),
                                "5": int(self.voltage[port] * 1000),  # noqa: S311
                            },
                            "Temp": {
                                "0": int(self.temperature[port] * 10),
                                "1": int(self.temperature[port] * 10),  # noqa: S311
                                "2": int(self.temperature[port] * 10),
                                "3": int(round(random.uniform(45, 45.5), 2) * 10),
                                "4": int(self.temperature[port] * 1.004 * 10),
                                "5": int(round(random.uniform(90.1, 90.5), 2) * 10),  # noqa: S311
                            },
                            "Curr": {"0": int(round(random.uniform(25, 27), 1) * 1000)},  # noqa: S311
                            "WireVolt":{
                                "0":int(round(random.uniform(100, 300), 0)),
                                "1":int(round(random.uniform(100, 300), 0)),
                                "2":int(round(random.uniform(100, 300), 0)),
                                "3":int(round(random.uniform(100, 300), 0)),
                                "4":int(round(random.uniform(100, 300), 0))
                            },
                            "Step":{"0":self.step[port]},
                            "FETState":{"0":1},
                            "Fuse":{"0":0},
                            "AFE":{"0":"0x12"},
                            "WifiSTDisConn":{"0":0},
                            "WifiLTDisConn":{"0":0},
                            "SocketSTDisConn":{"0":0},
                            "SocketLTDisConn":{"0":0},
                            "HeartbeatTxCount":{"0":10},
                            "HeartbeatLossCount":{"0":0},
                            "HeartbeatLastRttMs":{"0":32},
                            "HeartbeatRttMaxMs":{"0":40},
                            "HeatbeatTimeoutFlag":{"0":0},
                            "WifiReconnLastMs":{"0":0},
                            "WifiReconnAvgMs":{"0":0},
                            "WifiReconnMaxMs":{"0":0},
                            "WifiReconnTimes":{"0":0},
                            "SocketReconnLastMs":{"0":0},
                            "SocketReconnAvgMs":{"0":0},
                            "SocketReconnMaxMs":{"0":0},
                            "SocketReconnTimes":{"0":0},
                            "RSSI": {"0": 0},
                            "WarCnt": {"0": 0},
                        }
                    elif port == 20100:
                        response_data = {
                            "Msg": {"0": "ALARM"},
                            "ReturnCode": {"0": 1},
                            "SerialBoardID": {"0": f"{self.active_serial_board_list[port]}"},
                            "ProtectBoardID": {"0": f"{self.active_protect_board_list[port]}"},
                            "TimerT1s": {"0": T1Timer_s},
                            "TimerT1ms": {"0": T1Timer_ms},
                            "Status": {"0": 134152192},
                            "Position": {
                                "0": 1,
                                "1": 2,
                                "2": 3,
                                "3": 4,
                                "4": 5,
                                "5": 7
                            },
                            "ErrorCode": {
                                "0": "OWVP",
                                "1": "OWVP",
                                "2": "OWVS",
                                "3": "OWVS",
                                "4": "OTF",
                                "5": "OC"
                            },
                            "ErrorValue": {
                                "0": "1505",
                                "1": "1510",
                                "2": "1515",
                                "3": "1520",
                                "4": "50",
                                "5": "2620"
                            },
                            "Volt": {
                                "0": int(self.voltage[port] * 1000),
                                "1": int(self.voltage[port] * 1000),
                                "2": int(self.voltage[port] * 1000),
                                "3": int(self.voltage[port] * 1000),  # noqa: S311
                                "4": int(self.voltage[port] * 1000),
                                "5": int(self.voltage[port] * 1000),  # noqa: S311
                            },
                            "Temp": {
                                "0": int(self.temperature[port] * 10),
                                "1": int(self.temperature[port] * 10),  # noqa: S311
                                "2": int(self.temperature[port] * 10),
                                "3": int(self.temperature[port] * 10),
                                "4": int(self.temperature[port] * 1.01 * 10),
                                "5": int(self.temperature[port] * 10),  # noqa: S311
                            },
                            "Curr": {"0": int(round(random.uniform(25, 27), 1) * 1000)},  # noqa: S311
                            "WireVolt":{
                                "0":int(round(random.uniform(1500, 1600), 0)),
                                "1":int(round(random.uniform(1500, 1600), 0)),
                                "2":int(round(random.uniform(1500, 1600), 0)),
                                "3":int(round(random.uniform(100, 300), 0)),
                                "4":int(round(random.uniform(100, 300), 0))
                            },
                            "Step":{"0":self.step[port]},
                            "FETState":{"0":1},
                            "Fuse":{"0":0},
                            "AFE":{"0":"0x12"},
                            "WifiSTDisConn":{"0":0},
                            "WifiLTDisConn":{"0":0},
                            "SocketSTDisConn":{"0":0},
                            "SocketLTDisConn":{"0":0},
                            "HeartbeatTxCount":{"0":10},
                            "HeartbeatLossCount":{"0":0},
                            "HeartbeatLastRttMs":{"0":32},
                            "HeartbeatRttMaxMs":{"0":40},
                            "HeatbeatTimeoutFlag":{"0":0},
                            "WifiReconnLastMs":{"0":0},
                            "WifiReconnAvgMs":{"0":0},
                            "WifiReconnMaxMs":{"0":0},
                            "WifiReconnTimes":{"0":0},
                            "SocketReconnLastMs":{"0":0},
                            "SocketReconnAvgMs":{"0":0},
                            "SocketReconnMaxMs":{"0":0},
                            "SocketReconnTimes":{"0":0},
                            "RSSI": {"0": 0},
                            "WarCnt": {"0": 0},
                        }
                    elif port == 20101:
                        response_data = {
                            "Msg": {"0": "ALARM"},
                            "ReturnCode": {"0": 1},
                            "SerialBoardID": {"0": f"{self.active_serial_board_list[port]}"},
                            "ProtectBoardID": {"0": f"{self.active_protect_board_list[port]}"},
                            "TimerT1s": {"0": T1Timer_s},
                            "TimerT1ms": {"0": T1Timer_ms},
                            "Status": {"0": 134119424},
                            "Position": {
                                "0": 1,
                                "1": 2,
                                "2": 3,
                                "3": 4,
                                "4": 5,
                                "5": 6,
                                "6": 7
                            },
                            "ErrorCode": {
                                "0": "CTO",
                                "1": "CTO",
                                "2": "CTO",
                                "3": "CTO",
                                "4": "CTO",
                                "5": "CTO",
                                "6": "UC"
                            },
                            "ErrorValue": {
                                "0": "11400000",
                                "1": "11400000",
                                "2": "11400000",
                                "3": "11400000",
                                "4": "11400000",
                                "5": "11400000",
                                "6": "-1550"
                            },
                            "Volt": {
                                "0": int(self.voltage[port] * 1000),
                                "1": int(self.voltage[port] * 1000),
                                "2": int(self.voltage[port] * 1000),
                                "3": int(self.voltage[port] * 1000),
                                "4": int(self.voltage[port] * 1000),
                                "5": int(self.voltage[port] * 1000),
                            },
                            "Temp": {
                                "0": int(self.temperature[port] * 10),
                                "1": int(self.temperature[port] * 10),
                                "2": int(self.temperature[port] * 10),
                                "3": int(self.temperature[port] * 10),
                                "4": int(self.temperature[port] * 10),  # noqa: S311
                                "5": int(self.temperature[port] * 10),
                            },
                            "Curr": {"0": int(round(random.uniform(-1.5, -1.6), 1) * 1000)},  # noqa: S311
                            "WireVolt":{
                                "0":int(round(random.uniform(100, 300), 0)),
                                "1":int(round(random.uniform(100, 300), 0)),
                                "2":int(round(random.uniform(100, 300), 0)),
                                "3":int(round(random.uniform(100, 300), 0)),
                                "4":int(round(random.uniform(100, 300), 0))
                            },
                            "Step":{"0":self.step[port]},
                            "FETState":{"0":1},
                            "Fuse":{"0":0},
                            "AFE":{"0":"0x12"},
                            "WifiSTDisConn":{"0":0},
                            "WifiLTDisConn":{"0":0},
                            "SocketSTDisConn":{"0":0},
                            "SocketLTDisConn":{"0":0},
                            "HeartbeatTxCount":{"0":10},
                            "HeartbeatLossCount":{"0":0},
                            "HeartbeatLastRttMs":{"0":32},
                            "HeartbeatRttMaxMs":{"0":40},
                            "HeatbeatTimeoutFlag":{"0":0},
                            "WifiReconnLastMs":{"0":0},
                            "WifiReconnAvgMs":{"0":0},
                            "WifiReconnMaxMs":{"0":0},
                            "WifiReconnTimes":{"0":0},
                            "SocketReconnLastMs":{"0":0},
                            "SocketReconnAvgMs":{"0":0},
                            "SocketReconnMaxMs":{"0":0},
                            "SocketReconnTimes":{"0":0},
                            "RSSI": {"0": 0},
                            "WarCnt": {"0": 0},
                        }
                    elif port == 20102:
                        response_data = {
                            "Msg": {"0": "ALARM"},
                            "ReturnCode": {"0": 1},
                            "SerialBoardID": {"0": f"{self.active_serial_board_list[port]}"},
                            "ProtectBoardID": {"0": f"{self.active_protect_board_list[port]}"},
                            "TimerT1s": {"0": T1Timer_s},
                            "TimerT1ms": {"0": T1Timer_ms},
                            "Status": {"0": 134053888},
                            "Position": {
                                "0": 1,
                                "1": 2,
                                "2": 3,
                                "3": 4,
                                "4": 5,
                                "5": 6,
                                "6": 7
                            },
                            "ErrorCode": {
                                "0": "CTO",
                                "1": "CTO",
                                "2": "CTO",
                                "3": "CTO",
                                "4": "CTO",
                                "5": "CTO",
                                "6": "UC"
                            },
                            "ErrorValue": {
                                "0": "11400000",
                                "1": "11400000",
                                "2": "11400000",
                                "3": "11400000",
                                "4": "11400000",
                                "5": "11400000",
                                "6": "-1550"
                            },
                            "Volt": {
                                "0": int(self.voltage[port] * 1000),
                                "1": int(self.voltage[port] * 1000),
                                "2": int(self.voltage[port] * 1000),
                                "3": int(self.voltage[port] * 1000),
                                "4": int(self.voltage[port] * 1000),
                                "5": int(self.voltage[port] * 1000),
                            },
                            "Temp": {
                                "0": int(self.temperature[port] * 10),
                                "1": int(self.temperature[port] * 10),
                                "2": int(self.temperature[port] * 10),
                                "3": int(self.temperature[port] * 10),
                                "4": int(self.temperature[port] * 10),  # noqa: S311
                                "5": int(self.temperature[port] * 10),
                            },
                            "Curr": {"0": int(round(random.uniform(-1.5, -1.6), 1) * 1000)},  # noqa: S311
                            "WireVolt":{
                                "0":int(round(random.uniform(100, 300), 0)),
                                "1":int(round(random.uniform(100, 300), 0)),
                                "2":int(round(random.uniform(100, 300), 0)),
                                "3":int(round(random.uniform(100, 300), 0)),
                                "4":int(round(random.uniform(100, 300), 0))
                            },
                            "Step":{"0":self.step[port]},
                            "FETState":{"0":1},
                            "Fuse":{"0":0},
                            "AFE":{"0":"0x12"},
                            "WifiSTDisConn":{"0":0},
                            "WifiLTDisConn":{"0":0},
                            "SocketSTDisConn":{"0":0},
                            "SocketLTDisConn":{"0":0},
                            "HeartbeatTxCount":{"0":10},
                            "HeartbeatLossCount":{"0":0},
                            "HeartbeatLastRttMs":{"0":32},
                            "HeartbeatRttMaxMs":{"0":40},
                            "HeatbeatTimeoutFlag":{"0":0},
                            "WifiReconnLastMs":{"0":0},
                            "WifiReconnAvgMs":{"0":0},
                            "WifiReconnMaxMs":{"0":0},
                            "WifiReconnTimes":{"0":0},
                            "SocketReconnLastMs":{"0":0},
                            "SocketReconnAvgMs":{"0":0},
                            "SocketReconnMaxMs":{"0":0},
                            "SocketReconnTimes":{"0":0},
                            "RSSI": {"0": 0},
                            "WarCnt": {"0": 0},
                        }
                    self.send_data(response_data, port, True)
                # Send Measurement Data
                if self.mode[port] == "Polling":
                    # print(port)
                    if self.step[port] != 0:
                        self.water_count[port] -= 1
                    # self.disconn_val[port] -= 1
                    if self.step[port] == 0:
                        response_data = {
                            "Msg": {"0": "Data"},
                            "ReturnCode": {"0": 0},
                            "SerialBoardID": {"0": f"{self.active_serial_board_list[port]}"},
                            "ProtectBoardID": {"0": f"{self.active_protect_board_list[port]}"},
                            "TimerT1s": {"0": T1Timer_s},
                            "TimerT1ms": {"0": T1Timer_ms},
                            "Status": {"0": 134184960},
                            "Volt": {
                                "0": int(self.voltage[port] * 1000 * random.uniform(1.00400, 1.00700)),
                                "1": int(self.voltage[port] * 1000 * random.uniform(1.00400, 1.00700)),
                                "2": int(self.voltage[port] * 1000 * random.uniform(1.00400, 1.00700)),
                                "3": int(self.voltage[port] * 1000 * random.uniform(1.00400, 1.00700)),
                                "4": int(self.voltage[port] * 1000 * random.uniform(1.00400, 1.00700)),
                                "5": int(self.voltage[port] * 1000 * random.uniform(1.00400, 1.00700)),
                            },
                            "Temp": {
                                "0": int(self.temperature[port] * 10 * random.uniform(1.00400, 1.00700)),
                                "1": int(self.temperature[port] * 10 * random.uniform(1.00400, 1.00700)),
                                "2": int(self.temperature[port] * 10 * random.uniform(1.00400, 1.00700)),
                                "3": int(self.temperature[port] * 10 * random.uniform(1.00400, 1.00700)),
                                "4": int(self.temperature[port] * 10 * random.uniform(1.00400, 1.00700)),
                                "5": int(self.temperature[port] * 10 * random.uniform(1.00400, 1.00700)),
                            },
                            "Curr": {"0": int(round(random.uniform(17.1, 17.4), 1) * 1000)},  # noqa: S311
                            "WireVolt":{
                                "0":int(round(random.uniform(100, 300), 0)),
                                "1":int(round(random.uniform(100, 300), 0)),
                                "2":int(round(random.uniform(100, 300), 0)),
                                "3":int(round(random.uniform(100, 300), 0)),
                                "4":int(round(random.uniform(100, 300), 0))
                            },
                            "Step":{"0":self.step[port]},
                            "FETState":{"0":1},
                            "Fuse":{"0":0},
                            "AFE":{"0":"0x12"},
                            "WifiSTDisConn":{"0":0},
                            "WifiLTDisConn":{"0":0},
                            "SocketSTDisConn":{"0":0},
                            "SocketLTDisConn":{"0":0},
                            "HeartbeatTxCount":{"0":10},
                            "HeartbeatLossCount":{"0":0},
                            "HeartbeatLastRttMs":{"0":32},
                            "HeartbeatRttMaxMs":{"0":40},
                            "HeatbeatTimeoutFlag":{"0":0},
                            "WifiReconnLastMs":{"0":0},
                            "WifiReconnAvgMs":{"0":0},
                            "WifiReconnMaxMs":{"0":0},
                            "WifiReconnTimes":{"0":0},
                            "SocketReconnLastMs":{"0":0},
                            "SocketReconnAvgMs":{"0":0},
                            "SocketReconnMaxMs":{"0":0},
                            "SocketReconnTimes":{"0":0},
                            "RSSI": {"0": 0},
                            "WarCnt": {"0": 0},
                        }
                        self.send_data(response_data, port, True)
                    # CC Charging Simulation
                    if self.step[port] == 1:
                        if time.time() - self.last_send_time[port] > 3:
                            voltage_change = round(random.uniform(0.01, 0.03), 3)  # noqa: S311
                            self.voltage[port] = min(self.voltage[port] + voltage_change, 4.085)
                            temperature_change = round(random.uniform(0.03, 0.05), 2)  # noqa: S311
                            self.temperature[port] = min(self.temperature[port] + temperature_change, 24.5)
                            self.last_send_time[port] = time.time()
                        response_data = {
                            "Msg": {"0": "Data"},
                            "ReturnCode": {"0": 0},
                            "SerialBoardID": {"0": f"{self.active_serial_board_list[port]}"},
                            "ProtectBoardID": {"0": f"{self.active_protect_board_list[port]}"},
                            "TimerT1s": {"0": T1Timer_s},
                            "TimerT1ms": {"0": T1Timer_ms},
                            "Status": {"0": 134184960},
                            "Volt": {
                                "0": int(self.voltage[port] * 1000),
                                "1": int(self.voltage[port] * 1000),
                                "2": int(self.voltage[port] * 1000),
                                "3": int(self.voltage[port] * 1000),
                                "4": int(self.voltage[port] * 1000),
                                "5": int(self.voltage[port] * 1000),
                            },
                            "Temp": {
                                "0": int(self.temperature[port] * 10),
                                "1": int(self.temperature[port] * 10),
                                "2": int(self.temperature[port] * 10),
                                "3": int(self.temperature[port] * 10),
                                "4": int(self.temperature[port] * 10),
                                "5": int(self.temperature[port] * 10),
                            },
                            "Curr": {"0": int(round(random.uniform(17.1, 17.4), 1) * 1000)},  # noqa: S311
                            # "Curr": {"0": 0},  # noqa: S311
                            "WireVolt":{
                                "0":int(round(random.uniform(100, 300), 0)),
                                "1":int(round(random.uniform(100, 300), 0)),
                                "2":int(round(random.uniform(100, 300), 0)),
                                "3":int(round(random.uniform(100, 300), 0)),
                                "4":int(round(random.uniform(100, 300), 0))
                            },
                            "Step":{"0":self.step[port]},
                            "FETState":{"0":1},
                            "Fuse":{"0":0},
                            "AFE":{"0":"0x12"},
                            "WifiSTDisConn":{"0":0},
                            "WifiLTDisConn":{"0":0},
                            "SocketSTDisConn":{"0":0},
                            "SocketLTDisConn":{"0":0},
                            "HeartbeatTxCount":{"0":10},
                            "HeartbeatLossCount":{"0":0},
                            "HeartbeatLastRttMs":{"0":32},
                            "HeartbeatRttMaxMs":{"0":40},
                            "HeatbeatTimeoutFlag":{"0":0},
                            "WifiReconnLastMs":{"0":0},
                            "WifiReconnAvgMs":{"0":0},
                            "WifiReconnMaxMs":{"0":0},
                            "WifiReconnTimes":{"0":0},
                            "SocketReconnLastMs":{"0":0},
                            "SocketReconnAvgMs":{"0":0},
                            "SocketReconnMaxMs":{"0":0},
                            "SocketReconnTimes":{"0":0},
                            "RSSI": {"0": 0},
                            "WarCnt": {"0": 0},
                        }
                        self.send_data(response_data, port, True)
                    # Rest Simulation
                    elif self.step[port] == 2:
                        if time.time() - self.last_send_time[port] > 3:
                            voltage_change = round(random.uniform(0.01, 0.02), 3)  # noqa: S311
                            self.voltage[port] = max(self.voltage[port] - voltage_change, 3.875)
                            temperature_change = round(random.uniform(0.01, 0.02), 2)  # noqa: S311
                            self.temperature[port] = max(self.temperature[port] - temperature_change, 23.5)
                            self.last_send_time[port] = time.time()
                        response_data = {
                            "Msg": {"0": "Data"},
                            "ReturnCode": {"0": 0},
                            "SerialBoardID": {"0": f"{self.active_serial_board_list[port]}"},
                            "ProtectBoardID": {"0": f"{self.active_protect_board_list[port]}"},
                            "TimerT1s": {"0": T1Timer_s},
                            "TimerT1ms": {"0": T1Timer_ms},
                            "Status": {"0": 134184960},
                            "Volt": {
                                "0": int(self.voltage[port] * 1000),
                                "1": int(self.voltage[port] * 1000),
                                "2": int(self.voltage[port] * 1000),
                                "3": int(self.voltage[port] * 1000),
                                "4": int(self.voltage[port] * 1000),
                                "5": int(self.voltage[port] * 1000),
                                "6": int(self.voltage[port] * 1000),
                            },
                            "Temp": {
                                "0": int(self.temperature[port] * 10),
                                "1": int(self.temperature[port] * 10),
                                "2": int(self.temperature[port] * 10),
                                "3": int(self.temperature[port] * 10),
                                "4": int(self.temperature[port] * 10),
                                "5": int(self.temperature[port] * 10),
                                "6": int(self.temperature[port] * 10),
                            },
                            "Curr": {"0": 0},
                            "WireVolt":{
                                "0":int(round(random.uniform(100, 300), 0)),
                                "1":int(round(random.uniform(100, 300), 0)),
                                "2":int(round(random.uniform(100, 300), 0)),
                                "3":int(round(random.uniform(100, 300), 0)),
                                "4":int(round(random.uniform(100, 300), 0))
                            },
                            "Step":{"0":self.step[port]},
                            "FETState":{"0":1},
                            "Fuse":{"0":0},
                            "AFE":{"0":"0x12"},
                            "WifiSTDisConn":{"0":0},
                            "WifiLTDisConn":{"0":0},
                            "SocketSTDisConn":{"0":0},
                            "SocketLTDisConn":{"0":0},
                            "HeartbeatTxCount":{"0":10},
                            "HeartbeatLossCount":{"0":0},
                            "HeartbeatLastRttMs":{"0":32},
                            "HeartbeatRttMaxMs":{"0":40},
                            "HeatbeatTimeoutFlag":{"0":0},
                            "WifiReconnLastMs":{"0":0},
                            "WifiReconnAvgMs":{"0":0},
                            "WifiReconnMaxMs":{"0":0},
                            "WifiReconnTimes":{"0":0},
                            "SocketReconnLastMs":{"0":0},
                            "SocketReconnAvgMs":{"0":0},
                            "SocketReconnMaxMs":{"0":0},
                            "SocketReconnTimes":{"0":0},
                            "RSSI": {"0": 0},
                            "WarCnt": {"0": 0},
                        }
                        self.send_data(response_data, port, True)
                # Receive Message from CCMS Flow Control
                data = b""
                FIONREAD = 0x4004667F
                winsock = ctypes.windll.ws2_32

                pending = ctypes.c_ulong(0)

                ret = winsock.ioctlsocket(
                    self.sockets[port].fileno(),
                    FIONREAD,
                    ctypes.byref(pending),
                )

                if pending.value > 200:
                    print(f"{port}: pending={pending.value} bytes")
                self.sockets[port].settimeout(0.01)
                header = self.sockets[port].recv(4)
                if header != b"":
                    length = struct.unpack(">I", header)[0]
                    recv_data = self.sockets[port].recv(length)
                    length = length - len(recv_data)
                    data += recv_data
                    while length > 0:
                        recv_data = self.sockets[port].recv(length)
                        length = length - len(recv_data)
                        data += recv_data
                if data != b"":
                    data = json.loads(data)
                    if data.get("Msg")["0"] == "HeartbeatRequest":
                        hb_ack = {"Msg": {"0": "HeartbeatAck"}, "Tid": {"0": data.get("Tid")["0"]}}
                        self.send_data(hb_ack, port, False)
                        # self.logging.debug(f"CCMS2MPD heartbeat request [connected state] {port}")
                    elif data.get("Msg")["0"] == "HeartbeatAck":
                        self.last_heartbeat_timeout_time[port] = time.time()
                        # self.logging.debug(f"CCMS2MPD heartbeat ack [connected state] {port}")
                    # Send Config Reply
                    elif data.get("Msg")["0"] == "Config":
                        # self.sockets[port].sendall(struct.pack(">I", len("heartbeat".encode())) + "heartbeat".encode())
                        # print(json.dumps(data, indent=4))
                        self.protect_config = data
                        if data.get("SerialBoardID", {}).get("0") not in self.active_serial_board_list:
                            self.active_serial_board_list[port] = data.get("SerialBoardID")["0"]
                        if data.get("ProtectBoardID", {}).get("0") not in self.active_protect_board_list:
                            self.active_protect_board_list[port] = data.get("ProtectBoardID")["0"]
                        response_data = {
                            "Msg": {"0": "ACK"},
                            "ReturnCode": {"0": 0},
                            "SerialBoardID": {"0": f"{self.active_serial_board_list[port]}"},
                            "ProtectBoardID": {"0": f"{self.active_protect_board_list[port]}"},
                            "TimerT1s": {"0": T1Timer_s},
                            "TimerT1ms": {"0": T1Timer_ms},
                            "Status": {"0": 134184960},
                            "Position": {"0": 0},
                            "RSSI": {"0": 0},
                            "WarCnt": {"0": 0},
                        }
                        self.send_data(response_data, port, False)
                        self.mode[port] = "Polling"
                        self.logging.info(f"{data.get('Msg')['0']} | {data.get('SerialBoardID')['0']} | Step_0")
                    # Send Back the Check Results of Charging Step 1 for CC State
                    elif data.get("Msg")["0"] == "Check" and data.get("Step")["0"] == 1:
                        # self.logging.debug(f"{json.dumps(data, indent=4)}")
                        response_data = {
                            "Msg": {"0": "ACK"},
                            "ReturnCode": {"0": 0},
                            "SerialBoardID": {"0": f"{self.active_serial_board_list[port]}"},
                            "ProtectBoardID": {"0": f"{self.active_protect_board_list[port]}"},
                            "TimerT1s": {"0": T1Timer_s},
                            "TimerT1ms": {"0": T1Timer_ms},
                            "Status": {"0": 134184960},
                            "Position": {"0": 1},
                            "RSSI": {"0": 0},
                            "WarCnt": {"0": 0},
                        }
                        self.send_data(response_data, port, False)
                        self.mode[port] = "Polling"
                        self.step[port] = data.get("Step")["0"]
                        self.logging.info(f"{data.get('Msg')['0']} | {data.get('SerialBoardID')['0']} | Step_1")
                    # Send Back the Check Results of Charging Step 2 for Rest State
                    elif data.get("Msg")["0"] == "Check" and data.get("Step")["0"] == 2:
                        # self.logging.debug(f"{json.dumps(data, indent=4)}")
                        response_data = {
                            "Msg": {"0": "ACK"},
                            "ReturnCode": {"0": 0},
                            "SerialBoardID": {"0": f"{self.active_serial_board_list[port]}"},
                            "ProtectBoardID": {"0": f"{self.active_protect_board_list[port]}"},
                            "TimerT1s": {"0": T1Timer_s},
                            "TimerT1ms": {"0": T1Timer_ms},
                            "Status": {"0": 134184960},
                            "Position": {"0": 1},
                            "RSSI": {"0": 0},
                            "WarCnt": {"0": 0},
                        }
                        self.send_data(response_data, port, False)
                        self.mode[port] = "Polling"
                        self.step[port] = data.get("Step")["0"]
                        self.logging.info(f"{data.get('Msg')['0']} | {data.get('SerialBoardID')['0']} | Step_2")
                    # Send Back the Check Results of Charging Step 3 for End State
                    elif data.get("Msg")["0"] == "Check" and data.get("Step")["0"] == 3:
                        response_data = {
                            "Msg": {"0": "ACK"},
                            "ReturnCode": {"0": 0},
                            "SerialBoardID": {"0": f"{self.active_serial_board_list[port]}"},
                            "ProtectBoardID": {"0": f"{self.active_protect_board_list[port]}"},
                            "TimerT1s": {"0": T1Timer_s},
                            "TimerT1ms": {"0": T1Timer_ms},
                            "Status": {"0": 134184960},
                            "Position": {"0": 0},
                            "RSSI": {"0": 0},
                            "WarCnt": {"0": 0},
                        }
                        self.send_data(response_data, port, False)
                        self.logging.info(f"{data.get('Msg')['0']} | {data.get('SerialBoardID')['0']} | Step_3")
            except TimeoutError:
                pass
            except Exception:
                self.logging.error(f"send_and_receive failed\n{self.logger.get_slim_error_log()}")
                sock = self.sockets.pop(port, None)
                if sock is not None:
                    try:
                        sock.close()
                    except Exception:
                        pass
                break
            

    def create_port_context(self, port):
        """
        Create an isolated single-port simulation context.

        The returned context owns all mutable socket and simulation state used
        by its worker. Only immutable configuration and the thread-safe logger
        are shared with the coordinator, so worker locks are not required.

        Args:
            port: TCP port assigned to the context.

        Returns:
            An isolated ESP_simulation instance for one worker thread.
        """
        context = object.__new__(ESP_simulation)
        context.logger = self.logger
        context.logging = self.logging
        context.host = self.host
        context.port_start = port
        context.port_end = port
        context.inactive_ports = [port]
        context.active_ports = []
        context.sockets = {}
        context.voltage = {}
        context.temperature = {}
        context.water_count = {}
        context.mode = {}
        context.step = {}
        context.active_serial_board_list = {}
        context.active_protect_board_list = {}
        context.Perf_flag = self.Perf_flag
        context.protect_config = {}
        context.last_heartbeat_timeout_time = {}
        context.last_heartbeat_sent_time = {}
        context.last_sent_data_time = {}
        context.heartbeat_current = {}
        context.heartbeat_ack = {}
        context.last_time = {}
        context.last_send_time = {}
        context.connect_threads = {}
        context.connect_stop_event = self.connect_stop_event
        return context

    def port_worker(self, port):
        """
        Maintain one socket connection for one dedicated port.

        Args:
            port: Worker-owned TCP port.

        Returns:
            None
        """
        context = self.create_port_context(port)

        while not self.connect_stop_event.is_set():
            try:
                if port not in context.sockets:
                    connected_port = context.try_connect(port)

                    if connected_port is None:
                        self.connect_stop_event.wait(0.5)
                        continue

                # This function keeps running until the socket disconnects.
                context.send_and_receive(port)

            except Exception:
                self.logging.error(
                    f"Port worker failed | Port: {port}\n"
                    f"{self.logger.get_slim_error_log()}"
                )

                sock = context.sockets.pop(port, None)

                if sock is not None:
                    try:
                        sock.close()
                    except Exception:
                        pass

            self.connect_stop_event.wait(0.5)

    def run(self):
        """
        Start one persistent connect/send/receive worker for every port.

        Args:
            None

        Returns:
            None

        Example:
            >>> esp_simulation_obj = ESP_simulation()
            >>> esp_simulation_obj.run()
        """
        try:
            ports = range(self.port_start, self.port_end + 1)

            for port in ports:
                thread = threading.Thread(
                    target=self.port_worker,
                    args=(port,),
                    daemon=False,
                    name=f"ESP-Port-{port}",
                )
                self.connect_threads[port] = thread
                thread.start()

            self.logging.info(
                f"Started {len(self.connect_threads)} persistent port workers"
            )

            for thread in self.connect_threads.values():
                thread.join()

        except KeyboardInterrupt:
            self.connect_stop_event.set()
            self.logging.info("Stopping ESP port workers")
        except Exception:
            self.logging.error(f"Run failed\n{self.logger.get_slim_error_log()}")


if __name__ == "__main__":
    try:
        esp_simulation_obj = ESP_simulation()
        esp_simulation_obj.run()
    except Exception:
        exc_type, exc_value, exc_tb = sys.exc_info()
        tb_summary = traceback.extract_tb(exc_tb)
        last_call = tb_summary[-1]

        err_msg = f"{exc_type.__name__}: {exc_value}"
        logging.error(f"Main function failed\nFile {last_call.filename}, line {last_call.lineno}, in {last_call.name}: {err_msg}")
