import socket
import time
import json
import sys
import os
import random
import struct
import traceback
from concurrent.futures import ThreadPoolExecutor
import logging


def rack_to_ports(rack_id):
    """
    rack 1 -> [20001, 20002]
    rack 2 -> [20003, 20004]
    """
    base_port = 20001 + (int(rack_id) - 1) * 2
    return [base_port, base_port + 1]



class ESP_simulation:
    
    def __init__(
        self,
        rack_count=None,
        max_active_racks: int = 20,
        random_enable_interval_sec: int = 60,
        rack_cooldown_sec: int = 60,
    ):
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from balps.log_mgr import logging_api

        try:
            self.logger = logging_api(
                filename=os.path.join("logs", f"{os.path.splitext(os.path.basename(sys.argv[0]))[0]}"),
                backup_count=7,
            )
            self.logging = self.logger.get_logger()

            with open("core\\main\\config.json") as json_file:
                config_data = json.load(json_file)

            self.host = config_data["ccms_ip"]
            self.artifacts_dir = "artifacts"

            # 新架構：ESP 啟動時從 0 rack 開始，不一開始 init rack。
            # rack 會由 schedule_one_random_rack_from_pool() 定期從 pool 中挑選。
            self.rack_count = 0 if rack_count is None else int(rack_count)
            self.racks = []

            self.port_start = 20001
            self.port_end = 20168
            self.expected_ports = []
            self.inactive_ports = []
            self.active_ports = []

            # rack pool / cooldown lifecycle
            self.rack_pool_all = set(range(1, 31))
            self.available_rack_pool = set(range(1, 31))
            self.cooldown_racks = {}  # rack -> monotonic timestamp when rack can return to pool

            self.random_enable_interval_sec = int(random_enable_interval_sec)
            self.rack_cooldown_sec = int(rack_cooldown_sec)
            self.max_active_racks = int(max_active_racks)
            if self.max_active_racks < 1 or self.max_active_racks > 84:
                raise ValueError(f"max_active_racks must be between 1 and 84, got {self.max_active_racks}")
            self.max_active_ports = self.max_active_racks * 2
            self.last_random_enable_time = time.monotonic()

            self.sockets = {}
            self.voltage = {}
            self.temperature = {}
            self.water_count = {}
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
            self.last_time = time.time()
            self.last_send_time = {}
            self.step1_curr_cnt = {}   # 每個 port 在 step1 已實際送出幾次 Data

            # 需要時可以打開，輸出目前 active/inactive 狀態給 runner 看
            # self._write_port_status()

            self.logging.info(
                f"ESP pool mode init | start_from_zero=True, "
                f"max_active_racks={self.max_active_racks}, max_active_ports={self.max_active_ports}, "
                f"random_enable_interval_sec={self.random_enable_interval_sec}, "
                f"rack_cooldown_sec={self.rack_cooldown_sec}, "
                f"available_pool_size={len(self.available_rack_pool)}"
            )

        except Exception:
            self.logging.error(f"Esp initialization failed\n{self.logger.get_slim_error_log()}")
            raise

    def _write_port_status(self):
        try:
            os.makedirs(self.artifacts_dir, exist_ok=True)

            expected_data = {
                "racks": self.racks,
                "expected_ports": self.expected_ports,
            }

            active_data = {
                "racks": self.racks,
                "active_ports": sorted(self.active_ports),
                "inactive_ports": sorted(self.inactive_ports),
                "updated_at": time.time(),
            }

            expected_path = os.path.join(self.artifacts_dir, "esp_expected_ports.json")
            active_path = os.path.join(self.artifacts_dir, "esp_active_ports.json")

            expected_tmp = expected_path + ".tmp"
            active_tmp = active_path + ".tmp"

            with open(expected_tmp, "w", encoding="utf-8") as f:
                json.dump(expected_data, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())

            with open(active_tmp, "w", encoding="utf-8") as f:
                json.dump(active_data, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())

            # 原子替換，避免 runner 讀到半寫入檔案
            os.replace(expected_tmp, expected_path)
            os.replace(active_tmp, active_path)

        except Exception:
            pass

    #######################################


    def port_to_rack(self, port: int) -> int:
        """
        port 20001, 20002 -> rack 1
        port 20003, 20004 -> rack 2
        """
        return ((int(port) - 20001) // 2) + 1

    def refresh_cooldown_racks(self):
        """
        cooldown 到期的 rack 放回 available_rack_pool。
        """
        try:
            now_ts = time.monotonic()
            ready_racks = [
                rack for rack, ready_ts in list(self.cooldown_racks.items())
                if now_ts >= ready_ts
            ]

            released_racks = []
            for rack in ready_racks:
                self.cooldown_racks.pop(rack, None)
                ports = rack_to_ports(rack)

                # 保險：如果還 active 或 inactive，就不要放回 pool
                if any(port in self.active_ports for port in ports):
                    continue
                if any(port in self.inactive_ports for port in ports):
                    continue

                self.available_rack_pool.add(rack)
                released_racks.append(rack)

            if released_racks:
                self.logging.info(
                    f"[ESP-RACK-POOL] cooldown released racks={sorted(released_racks)}, "
                    f"available_pool_size={len(self.available_rack_pool)}"
                )
        except Exception:
            self.logging.error(
                f"refresh_cooldown_racks failed\n{self.logger.get_slim_error_log()}"
            )

    def schedule_one_random_rack_from_pool(self, interval_sec: int = 60):
        """
        每 interval_sec 秒，從 available_rack_pool 隨機挑一個 rack，
        放入 inactive_ports，讓 poll_ports 去 connect。

        active_ports + inactive_ports 達 max_active_ports 時，不再選新 rack。
        """
        try:
            now_ts = time.monotonic()
            if now_ts - self.last_random_enable_time < interval_sec:
                return

            self.last_random_enable_time = now_ts

            # 先把 cooldown 到期的 rack 放回 pool
            self.refresh_cooldown_racks()

            occupied_port_count = len(self.active_ports) + len(self.inactive_ports)
            if occupied_port_count >= self.max_active_ports:
                self.logging.info(
                    f"[ESP-RACK-POOL] capacity full"
                    # f"active_ports={len(self.active_ports)}, "
                    # f"inactive_ports={len(self.inactive_ports)}, "
                    # f"max_active_ports={self.max_active_ports}"
                )
                return

            # 一個 rack 會新增 2 個 port，所以至少要有 2 個空位
            if self.max_active_ports - occupied_port_count < 2:
                self.logging.info(
                    f"[ESP-RACK-POOL] not enough capacity for new rack "
                    # f"occupied_port_count={occupied_port_count}, "
                    # f"max_active_ports={self.max_active_ports}"
                )
                return

            if not self.available_rack_pool:
                self.logging.info("[ESP-RACK-POOL] no available rack in pool")
                return

            active_set = set(self.active_ports)
            inactive_set = set(self.inactive_ports)

            candidate_racks = []
            for rack in sorted(self.available_rack_pool):
                ports = rack_to_ports(rack)
                if any(port in active_set for port in ports):
                    continue
                if any(port in inactive_set for port in ports):
                    continue
                if rack in self.cooldown_racks:
                    continue
                candidate_racks.append(rack)

            if not candidate_racks:
                self.logging.info("[ESP-RACK-POOL] no candidate rack available")
                return

            selected_rack = random.choice(candidate_racks)
            selected_ports = rack_to_ports(selected_rack)

            self.available_rack_pool.discard(selected_rack)

            for port in selected_ports:
                if port not in self.inactive_ports and port not in self.active_ports:
                    self.inactive_ports.append(port)
                if port not in self.expected_ports:
                    self.expected_ports.append(port)

            self.expected_ports = sorted(set(self.expected_ports))

            if selected_rack not in self.racks:
                self.racks.append(selected_rack)
                self.racks = sorted(set(self.racks))

            self.logging.info(
                f"[ESP-RACK-POOL] selected rack={selected_rack}, "
            #     f"ports={selected_ports}, "
            #     f"active_ports={len(self.active_ports)}, "
            #     f"inactive_ports={len(self.inactive_ports)}, "
            #     f"available_pool_size={len(self.available_rack_pool)}"
            )
        except Exception:
            self.logging.error(
                f"schedule_one_random_rack_from_pool failed\n{self.logger.get_slim_error_log()}"
            )

    
    def move_port_to_disconnected_and_maybe_cooldown(self, port: int, cooldown_sec: int = 60):
        """
        只處理當前斷線 port。
        等同一個 rack 的兩個 port 都不在 active_ports / inactive_ports 後，
        才把 rack 放進 cooldown。
        """
        try:
            rack = self.port_to_rack(port)
            rack_ports = rack_to_ports(rack)

            # 1. 只移除當前 port
            if port in self.active_ports:
                self.active_ports.remove(port)

            if port in self.inactive_ports:
                self.inactive_ports.remove(port)

            # 2. 只關閉當前 port socket
            sock = self.sockets.pop(port, None)
            if sock is not None:
                try:
                    sock.close()
                except Exception:
                    pass

            # 3. 只清當前 port 的狀態，不能清同 rack 另一個 port
            self.voltage.pop(port, None)
            self.temperature.pop(port, None)
            self.water_count.pop(port, None)
            self.mode.pop(port, None)
            self.step.pop(port, None)
            self.active_serial_board_list.pop(port, None)
            self.active_protect_board_list.pop(port, None)
            self.last_heartbeat_timeout_time.pop(port, None)
            self.last_heartbeat_sent_time.pop(port, None)
            self.last_sent_data_time.pop(port, None)
            self.last_send_time.pop(port, None)
            self.step1_curr_cnt.pop(port, None)

            # 4. 檢查同 rack 兩個 port 是否都已經不在 active/inactive
            rack_still_active_or_waiting = any(
                p in self.active_ports or p in self.inactive_ports
                for p in rack_ports
            )

            if rack_still_active_or_waiting:
                # self.logging.info(
                #     f"[ESP-RACK-POOL] port={port} disconnected, "
                #     f"rack={rack} still has another port active/waiting | "
                #     f"rack_ports={rack_ports}"
                # )
                return

            # 5. 兩個 port 都處理完，rack 才進 cooldown
            if rack in self.racks:
                self.racks.remove(rack)

            self.cooldown_racks[rack] = time.monotonic() + cooldown_sec

            self.logging.info(
                f"[ESP-RACK-POOL] rack={rack} moved to cooldown after both ports disconnected"
            )

        except Exception:
            self.logging.error(
                f"move_port_to_disconnected_and_maybe_cooldown failed\n"
                f"{self.logger.get_slim_error_log()}"
            )


    def move_rack_to_cooldown(self, rack: int, cooldown_sec: int = 60):
        """
        將 rack 從 active/inactive 狀態移除，放入 cooldown。
        cooldown 到期後才會回 available_rack_pool。
        """
        try:
            ports = rack_to_ports(rack)

            for port in ports:
                if port in self.active_ports:
                    self.active_ports.remove(port)
                if port in self.inactive_ports:
                    self.inactive_ports.remove(port)

                sock = self.sockets.pop(port, None)
                if sock is not None:
                    try:
                        sock.close()
                    except Exception:
                        pass

                self.voltage.pop(port, None)
                self.temperature.pop(port, None)
                self.water_count.pop(port, None)
                self.mode.pop(port, None)
                self.step.pop(port, None)
                self.active_serial_board_list.pop(port, None)
                self.active_protect_board_list.pop(port, None)
                self.last_heartbeat_timeout_time.pop(port, None)
                self.last_heartbeat_sent_time.pop(port, None)
                self.last_sent_data_time.pop(port, None)
                self.last_send_time.pop(port, None)
                self.step1_curr_cnt.pop(port, None)

            if rack in self.racks:
                self.racks.remove(rack)

            self.cooldown_racks[rack] = time.monotonic() + cooldown_sec

            self.logging.info(
                f"[ESP-RACK-POOL] rack={rack} moved to cooldown "
                # f"ports={ports}, cooldown_sec={cooldown_sec}, "
                # f"active_ports={len(self.active_ports)}, "
                # f"inactive_ports={len(self.inactive_ports)}"
            )
        except Exception:
            self.logging.error(
                f"move_rack_to_cooldown failed\n{self.logger.get_slim_error_log()}"
            )

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
                self.step1_curr_cnt[port] = 0
            self.last_heartbeat_timeout_time[port] = time.time()
            self.last_heartbeat_sent_time[port] = time.time()
            self.active_protect_board_list[port] = f"PCB{port}"
            self.last_sent_data_time[port] = time.time()

            return port
        except BlockingIOError:
            return None
        except TimeoutError:
            return None
        except Exception:
            return None

    def poll_ports(self):
        """
        Connect ccms measurement flow control.
        Pool mode: only ports currently in inactive_ports are candidates for connect,
        and active port count is capped by self.max_active_ports.
        """
        try:
            if self.Perf_flag:
                start_time = time.time()

            available_slots = self.max_active_ports - len(self.active_ports)
            if available_slots <= 0:
                return

            connect_ports = list(self.inactive_ports)[:available_slots]
            if not connect_ports:
                return

            with ThreadPoolExecutor(max_workers=30) as executor:
                results = list(executor.map(self.try_connect, connect_ports))

            for port in results:
                if port is not None:
                    if port not in self.active_ports:
                        self.active_ports.append(port)
                    if port in self.inactive_ports:
                        self.inactive_ports.remove(port)

            ############## MPD test ##############
            # self._write_port_status()
            ######################################

            if self.Perf_flag:
                self.logging.info(f"POLL TIME: {time.time() - start_time:.6f} s")
        except Exception:
            self.logging.error(f"poll_ports failed\n{self.logger.get_slim_error_log()}")


    def send_data(self, data, port, is_delay):
        try:
            if not is_delay or time.time() - self.last_sent_data_time[port] > 5:
                data = json.dumps(data).encode("utf-8")
                self.sockets[port].settimeout(0.1)
                self.sockets[port].sendall(struct.pack(">I", len(data)) + data)
                if is_delay:
                    self.last_sent_data_time[port] = time.time()
        except Exception:
            self.logging.error(f"send_data failed \n{self.logger.get_slim_error_log()}")

    def send_and_receive(self):  # noqa: C901
        """
        Send and receive measurement configuration alert and ACK data

        Args:
            None

        Returns:
            None

        Example:
            >>> esp_simulation_obj = ESP_simulation()
            >>> esp_simulation_obj.send_and_receive()
        """
        try:
            # if self.Perf_flag:
            start_time = time.time()
            # SCAN ALL Active Port
            for port in self.active_ports[:]:
                try:
                    current_time = time.time()
                    if current_time - self.last_heartbeat_sent_time[port] >= 5:
                        hb_req = {"Msg": {"0": "HeartbeatRequest"}}
                        hb_req = json.dumps(hb_req).encode("utf-8")
                        self.sockets[port].settimeout(1)
                        self.sockets[port].sendall(struct.pack(">I", len(hb_req)) + hb_req)
                        # self.logging.error(f"ESP2CCMS heartbeat request [connected state] {port}")
                        self.last_heartbeat_sent_time[port] = current_time
                    if current_time - self.last_heartbeat_timeout_time[port] >= 20:
                        self.logging.error(f"ESP2CCMS heartbeat timeout [connected state] {port}")
                        self.sockets[port].close()
                        continue

                    T1Timer = time.time()
                    T1Timer_s = round(T1Timer - 0.5)
                    T1Timer_ms = int((round(T1Timer, 3) - T1Timer_s) * 1000)
                    # Send w1001 Water Alarm
                    if self.water_count[port] <= 0 and port in {
                        # 20001,
                        # 20010,
                        # 20101,
                        # 20102,
                    }:
                        self.mode[port] = "Stop"
                        if time.time() - self.last_send_time[port] > 3:
                            voltage_change = round(random.uniform(0.01, 0.03), 3)  # noqa: S311
                            self.voltage[port] = min(self.voltage[port] + voltage_change, 4.085)
                            temperature_change = round(random.uniform(0.03, 0.05), 2)  # noqa: S311
                            self.temperature[port] = min(self.temperature[port] + temperature_change, 24.5)
                            self.last_send_time[port] = time.time()
                        if port == 20001:
                            response_data = {
                                "Msg": {"0": "ALARM"},
                                "ReturnCode": {"0": 2},
                                "SerialBoardID": {"0": f"{self.active_serial_board_list[port]}"},
                                "ProtectBoardID": {"0": f"{self.active_protect_board_list[port]}"},
                                "TimerT1s": {"0": T1Timer_s},
                                "TimerT1ms": {"0": T1Timer_ms},
                                "Status": {"0": 0},
                                "Position": {"0": 2, "1": 4, "2": 6, "3": 7},
                                "ErrorCode": {
                                    "0": "OT",
                                    "1": "OV",
                                    "2": "OT",
                                    "3": "OC",
                                },
                                "Volt": {
                                    "0": int(self.voltage[port] * 1000),
                                    "1": int(self.voltage[port] * 1000),
                                    "2": int(self.voltage[port] * 1000),
                                    "3": int(round(random.uniform(4.2, 4.25), 2) * 1000),  # noqa: S311
                                    "4": int(self.voltage[port] * 1000),
                                    "5": int(round(random.uniform(4.2, 4.25), 2) * 1000),  # noqa: S311
                                },
                                "Temp": {
                                    "0": int(self.temperature[port] * 10 + 2731),
                                    "1": int(round(random.uniform(60, 70), 1) * 10 + 2731),  # noqa: S311
                                    "2": int(self.temperature[port] * 10 + 2731),
                                    "3": int(self.temperature[port] * 10 + 2731),
                                    "4": int(self.temperature[port] * 10 + 2731),
                                    "5": int(round(random.uniform(60, 70), 1) * 10 + 2731),  # noqa: S311
                                },
                                "Curr": {"0": int(round(random.uniform(75, 85), 1) * 1000)},  # noqa: S311
                                "WireVolt":{"0":120,"1":118,"2":121,"3":119,"4":122},
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
                        elif port == 20010:
                            response_data = {
                                "Msg": {"0": "ALARM"},
                                "ReturnCode": {"0": 2},
                                "SerialBoardID": {"0": f"{self.active_serial_board_list[port]}"},
                                "ProtectBoardID": {"0": f"{self.active_protect_board_list[port]}"},
                                "TimerT1s": {"0": T1Timer_s},
                                "TimerT1ms": {"0": T1Timer_ms},
                                "Status": {"0": 0},
                                "Position": {"0": 4},
                                "ErrorCode": {"0": "OV"},
                                "Volt": {
                                    "0": int(self.voltage[port] * 1000),
                                    "1": int(self.voltage[port] * 1000),
                                    "2": int(self.voltage[port] * 1000),
                                    "3": int(round(random.uniform(4.2, 4.25), 2) * 1000),  # noqa: S311
                                    "4": int(self.voltage[port] * 1000),
                                    "5": int(self.voltage[port] * 1000),
                                },
                                "Temp": {
                                    "0": int(self.temperature[port] * 10 + 2731),
                                    "1": int(self.temperature[port] * 10 + 2731),
                                    "2": int(self.temperature[port] * 10 + 2731),
                                    "3": int(self.temperature[port] * 10 + 2731),
                                    "4": int(self.temperature[port] * 10 + 2731),
                                    "5": int(self.temperature[port] * 10 + 2731),
                                },
                                "Curr": {"0": int(round(random.uniform(29.5, 30), 1) * 1000)},  # noqa: S311
                                "WireVolt":{"0":120,"1":118,"2":121,"3":119,"4":122},
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
                                "ReturnCode": {"0": 2},
                                "SerialBoardID": {"0": f"{self.active_serial_board_list[port]}"},
                                "ProtectBoardID": {"0": f"{self.active_protect_board_list[port]}"},
                                "TimerT1s": {"0": T1Timer_s},
                                "TimerT1ms": {"0": T1Timer_ms},
                                "Status": {"0": 0},
                                "Position": {"0": 5},
                                "ErrorCode": {"0": "OT"},
                                "Volt": {
                                    "0": int(self.voltage[port] * 1000),
                                    "1": int(self.voltage[port] * 1000),
                                    "2": int(self.voltage[port] * 1000),
                                    "3": int(self.voltage[port] * 1000),
                                    "4": int(self.voltage[port] * 1000),
                                    "5": int(self.voltage[port] * 1000),
                                },
                                "Temp": {
                                    "0": int(self.temperature[port] * 10 + 2731),
                                    "1": int(self.temperature[port] * 10 + 2731),
                                    "2": int(self.temperature[port] * 10 + 2731),
                                    "3": int(self.temperature[port] * 10 + 2731),
                                    "4": int(round(random.uniform(60, 70), 1) * 10 + 2731),  # noqa: S311
                                    "5": int(self.temperature[port] * 10 + 2731),
                                },
                                "Curr": {"0": int(round(random.uniform(29.5, 30), 1) * 1000)},  # noqa: S311
                                "WireVolt":{"0":120,"1":118,"2":121,"3":119,"4":122},
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
                                "ReturnCode": {"0": 2},
                                "SerialBoardID": {"0": f"{self.active_serial_board_list[port]}"},
                                "ProtectBoardID": {"0": f"{self.active_protect_board_list[port]}"},
                                "TimerT1s": {"0": T1Timer_s},
                                "TimerT1ms": {"0": T1Timer_ms},
                                "Status": {"0": 0},
                                "Position": {"0": 1},
                                "ErrorCode": {"0": "OV"},
                                "Volt": {
                                    "0": int(round(random.uniform(4.2, 4.25), 2) * 1000),  # noqa: S311
                                    "1": int(self.voltage[port] * 1000),
                                    "2": int(self.voltage[port] * 1000),
                                    "3": int(self.voltage[port] * 1000),
                                    "4": int(self.voltage[port] * 1000),
                                    "5": int(self.voltage[port] * 1000),
                                },
                                "Temp": {
                                    "0": int(self.temperature[port] * 10 + 2731),
                                    "1": int(self.temperature[port] * 10 + 2731),
                                    "2": int(self.temperature[port] * 10 + 2731),
                                    "3": int(self.temperature[port] * 10 + 2731),
                                    "4": int(self.temperature[port] * 10 + 2731),
                                    "5": int(self.temperature[port] * 10 + 2731),
                                },
                                "Curr": {"0": int(round(random.uniform(29.5, 30), 1) * 1000)},  # noqa: S311
                                "WireVolt":{"0":120,"1":118,"2":121,"3":119,"4":122},
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


                    if self.step[port] != 0 and port in {
                        # 20003,
                        # 20005,
                        # 20021,
                        # 20023,
                        # 20024,
                    }:
                        self.mode[port] = "Stop"
                        if time.time() - self.last_send_time[port] > 3:
                            voltage_change = round(random.uniform(0.01, 0.03), 3)  # noqa: S311
                            self.voltage[port] = min(self.voltage[port] + voltage_change, 4.085)
                            temperature_change = round(random.uniform(0.03, 0.05), 2)  # noqa: S311
                            self.temperature[port] = min(self.temperature[port] + temperature_change, 24.5)
                            self.last_send_time[port] = time.time()
                        if port == 20003:
                            response_data = {
                                "Msg": {"0": "ALARM"},
                                "ReturnCode": {"0": 1},
                                "SerialBoardID": {"0": f"{self.active_serial_board_list[port]}"},
                                "ProtectBoardID": {"0": f"{self.active_protect_board_list[port]}"},
                                "TimerT1s": {"0": T1Timer_s},
                                "TimerT1ms": {"0": T1Timer_ms},
                                "Status": {"0": 0},
                                "Position": {"0": 2,},
                                "ErrorCode": {
                                    "0": "OV",
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
                                    "0": int(2900),
                                    "1": int(self.temperature[port] * 10 + 2731),
                                    "2": int(self.temperature[port] * 10 + 2731),
                                    "3": int(self.temperature[port] * 10 + 2731),
                                    "4": int(self.temperature[port] * 10 + 2731),
                                    "5": int(self.temperature[port] * 10 + 2731),
                                },
                                #"Curr": {"0": int(round(random.uniform(75, 85), 1) * 1000)},  # noqa: S311
                                "Curr": {"0": int(22 * 1000)},  # noqa: S311
                                "WireVolt":{"0":120,"1":118,"2":121,"3":119,"4":122},
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
                        elif port == 20005:
                            response_data = {
                                "Msg": {"0": "ALARM"},
                                "ReturnCode": {"0": 2},
                                "SerialBoardID": {"0": f"{self.active_serial_board_list[port]}"},
                                "ProtectBoardID": {"0": f"{self.active_protect_board_list[port]}"},
                                "TimerT1s": {"0": T1Timer_s},
                                "TimerT1ms": {"0": T1Timer_ms},
                                "Status": {"0": 0},
                                "Position": {"0": 4},
                                "ErrorCode": {"0": "OT"},
                                "Volt": {
                                    "0": int(self.voltage[port] * 1000),
                                    "1": int(self.voltage[port] * 1000),
                                    "2": int(self.voltage[port] * 1000),
                                    "3": int(round(random.uniform(4.2, 4.25), 2) * 1000),  # noqa: S311
                                    "4": int(self.voltage[port] * 1000),
                                    "5": int(self.voltage[port] * 1000),
                                },
                                "Temp": {
                                    "0": int(self.temperature[port] * 10 + 2731),
                                    "1": int(self.temperature[port] * 10 + 2731),
                                    "2": int(self.temperature[port] * 10 + 2731),
                                    "3": int(self.temperature[port] * 10 + 2731),
                                    "4": int(self.temperature[port] * 10 + 2731),
                                    "5": int(self.temperature[port] * 10 + 2731),
                                },
                                "Curr": {"0": int(round(random.uniform(29.5, 30), 1) * 1000)},  # noqa: S311
                                "WireVolt":{"0":120,"1":118,"2":121,"3":119,"4":122},
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
                        elif port == 20021:
                            response_data = {
                                "Msg": {"0": "ALARM"},
                                "ReturnCode": {"0": 1},
                                "SerialBoardID": {"0": f"{self.active_serial_board_list[port]}"},
                                "ProtectBoardID": {"0": f"{self.active_protect_board_list[port]}"},
                                "TimerT1s": {"0": T1Timer_s},
                                "TimerT1ms": {"0": T1Timer_ms},
                                "Status": {"0": 0},
                                "Position": {"0": 1, "1": 2, "2": 3, "3": 4, "4": 5, "5": 6, "6": 7},
                                "ErrorCode": {
                                    "0": "OV",
                                    "1": "UV",
                                    "2": "OVS",
                                    "3": "OWVS",
                                    "4": "OT",
                                    "5": "OTS",
                                    "6": "OC",
                                },
                                "Volt": {
                                    "0": int(self.voltage[port] * 1000),
                                    "1": int(self.voltage[port] * 1000),
                                    "2": int(self.voltage[port] * 1000),
                                    "3": int(round(random.uniform(4.2, 4.25), 2) * 1000),  # noqa: S311
                                    "4": int(self.voltage[port] * 1000),
                                    "5": int(self.voltage[port] * 1000),
                                },
                                "Temp": {
                                    "0": int(self.temperature[port] * 10 + 2731),
                                    "1": int(self.temperature[port] * 10 + 2731),
                                    "2": int(self.temperature[port] * 10 + 2731),
                                    "3": int(self.temperature[port] * 10 + 2731),
                                    "4": int(self.temperature[port] * 10 + 2731),
                                    "5": int(self.temperature[port] * 10 + 2731),
                                },
                                "Curr": {"0": int(round(random.uniform(29.5, 30), 1) * 1000)},  # noqa: S311
                                "WireVolt":{"0":120,"1":118,"2":121,"3":119,"4":122},
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
                        elif port == 20023:
                            response_data = {
                                "Msg": {"0": "ALARM"},
                                "ReturnCode": {"0": 2},
                                "SerialBoardID": {"0": f"{self.active_serial_board_list[port]}"},
                                "ProtectBoardID": {"0": f"{self.active_protect_board_list[port]}"},
                                "TimerT1s": {"0": T1Timer_s},
                                "TimerT1ms": {"0": T1Timer_ms},
                                "Status": {"0": 0},
                                "Position": {"0": 7},
                                "ErrorCode": {
                                    "0": "UC",
                                },
                                "Volt": {
                                    "0": int(self.voltage[port] * 1000),
                                    "1": int(self.voltage[port] * 1000),
                                    "2": int(self.voltage[port] * 1000),
                                    "3": int(round(random.uniform(4.2, 4.25), 2) * 1000),  # noqa: S311
                                    "4": int(self.voltage[port] * 1000),
                                    "5": int(self.voltage[port] * 1000),
                                },
                                "Temp": {
                                    "0": int(self.temperature[port] * 10 + 2731),
                                    "1": int(self.temperature[port] * 10 + 2731),
                                    "2": int(self.temperature[port] * 10 + 2731),
                                    "3": int(self.temperature[port] * 10 + 2731),
                                    "4": int(self.temperature[port] * 10 + 2731),
                                    "5": int(self.temperature[port] * 10 + 2731),
                                },
                                "Curr": {"0": int(round(random.uniform(29.5, 30), 1) * 1000)},  # noqa: S311
                                "WireVolt":{"0":120,"1":118,"2":121,"3":119,"4":122},
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
                        elif port == 20024:
                            response_data = {
                                "Msg": {"0": "ALARM"},
                                "ReturnCode": {"0": 2},
                                "SerialBoardID": {"0": f"{self.active_serial_board_list[port]}"},
                                "ProtectBoardID": {"0": f"{self.active_protect_board_list[port]}"},
                                "TimerT1s": {"0": T1Timer_s},
                                "TimerT1ms": {"0": T1Timer_ms},
                                "Status": {"0": 0},
                                "Position": {"0": 1, "1": 7},
                                "ErrorCode": {
                                    "0": "CTO",
                                    "1": "UC",
                                },
                                "Volt": {
                                    "0": int(self.voltage[port] * 1000),
                                    "1": int(self.voltage[port] * 1000),
                                    "2": int(self.voltage[port] * 1000),
                                    "3": int(round(random.uniform(4.2, 4.25), 2) * 1000),  # noqa: S311
                                    "4": int(self.voltage[port] * 1000),
                                    "5": int(self.voltage[port] * 1000),
                                },
                                "Temp": {
                                    "0": int(self.temperature[port] * 10 + 2731),
                                    "1": int(self.temperature[port] * 10 + 2731),
                                    "2": int(self.temperature[port] * 10 + 2731),
                                    "3": int(self.temperature[port] * 10 + 2731),
                                    "4": int(self.temperature[port] * 10 + 2731),
                                    "5": int(self.temperature[port] * 10 + 2731),
                                },
                                "Curr": {"0": int(round(random.uniform(29.5, 30), 1) * 1000)},  # noqa: S311
                                "WireVolt":{"0":120,"1":118,"2":121,"3":119,"4":122},
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
                                "Status": {"0": 1},
                                "Volt": {
                                    "0": int(self.voltage[port] * 1000),
                                    "1": int(self.voltage[port] * 1000),
                                    "2": int(self.voltage[port] * 1000),
                                    "3": int(self.voltage[port] * 1000),
                                    "4": int(self.voltage[port] * 1000),
                                    "5": int(self.voltage[port] * 1000),
                                },
                                "Temp": {
                                    "0": int(self.temperature[port] * 10 + 2731),
                                    "1": int(self.temperature[port] * 10 + 2731),
                                    "2": int(self.temperature[port] * 10 + 2731),
                                    "3": int(self.temperature[port] * 10 + 2731),
                                    "4": int(self.temperature[port] * 10 + 2731),
                                    "5": int(self.temperature[port] * 10 + 2731),
                                },
                                "Curr": {"0": int(0)},  # noqa: S311
                                "WireVolt":{"0":120,"1":118,"2":121,"3":119,"4":122},
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

                            
                            # 確保 counter 已初始化
                            if port not in self.step1_curr_cnt:
                                self.step1_curr_cnt[port] = 0

                            # 只有這次真的會送資料時，才算一次 cnt
                            will_send = (time.time() - self.last_sent_data_time[port] > 5)

                            # 前 10 次正常，第 11 次開始 Curr = 0
                            if self.step1_curr_cnt[port] >= 10:
                                curr_value = 0
                            else:
                                curr_value = int(round(random.uniform(27.5, 28.5), 1) * 1000)  # noqa: S311

                            response_data = {
                                "Msg": {"0": "Data"},
                                "ReturnCode": {"0": 0},
                                "SerialBoardID": {"0": f"{self.active_serial_board_list[port]}"},
                                "ProtectBoardID": {"0": f"{self.active_protect_board_list[port]}"},
                                "TimerT1s": {"0": T1Timer_s},
                                "TimerT1ms": {"0": T1Timer_ms},
                                "Status": {"0": 1},
                                "Volt": {
                                    "0": int(self.voltage[port] * 1000),
                                    "1": int(self.voltage[port] * 1000),
                                    "2": int(self.voltage[port] * 1000),
                                    "3": int(self.voltage[port] * 1000),
                                    "4": int(self.voltage[port] * 1000),
                                    "5": int(self.voltage[port] * 1000),
                                },
                                "Temp": {
                                    "0": int(self.temperature[port] * 10 + 2731),
                                    "1": int(self.temperature[port] * 10 + 2731),
                                    "2": int(self.temperature[port] * 10 + 2731),
                                    "3": int(self.temperature[port] * 10 + 2731),
                                    "4": int(self.temperature[port] * 10 + 2731),
                                    "5": int(self.temperature[port] * 10 + 2731),
                                },
                                # "Curr": {"0": int(round(random.uniform(27.5, 28.5), 1) * 1000)},  # noqa: S311
                                "Curr": {"0": curr_value},
                                "WireVolt":{"0":120,"1":118,"2":121,"3":119,"4":122},
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
                            
                            # 只有真的送出去，才算一次 cnt
                            if will_send:
                                self.step1_curr_cnt[port] += 1

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
                                "Status": {"0": 1},
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
                                    "0": int(self.temperature[port] * 10 + 2731),
                                    "1": int(self.temperature[port] * 10 + 2731),
                                    "2": int(self.temperature[port] * 10 + 2731),
                                    "3": int(self.temperature[port] * 10 + 2731),
                                    "4": int(self.temperature[port] * 10 + 2731),
                                    "5": int(self.temperature[port] * 10 + 2731),
                                    "6": int(self.temperature[port] * 10 + 2731),
                                },
                                "Curr": {"0": 0},
                                "WireVolt":{"0":120,"1":118,"2":121,"3":119,"4":122},
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
                    self.sockets[port].settimeout(0.01)
                    header = self.sockets[port].recv(4)
                    if header != b"":
                        length = struct.unpack(">I", header)[0]
                        recv_data = self.sockets[port].recv(length)
                        length = length - len(recv_data)
                        data += recv_data
                        if length > 0:
                            recv_data = self.sockets[port].recv(length)
                            length = length - len(recv_data)
                            data += recv_data
                    if data != b"":
                        data = json.loads(data)
                        if data.get("Msg")["0"] == "HeartbeatRequest":
                            hb_ack = {"Msg": {"0": "HeartbeatAck"}, "Tid": {"0": data.get("Tid")["0"]}}
                            self.send_data(hb_ack, port, False)
                            # self.logging.error(f"CCMS2ESP heartbeat request [connected state] {port}")
                        elif data.get("Msg")["0"] == "HeartbeatAck":
                            self.last_heartbeat_timeout_time[port] = time.time()
                            # self.logging.error(f"CCMS2ESP heartbeat ack [connected state] {port}")
                        # Send Config Reply
                        if data.get("Msg")["0"] == "Config":
                            # self.sockets[port].sendall(struct.pack(">I", len("heartbeat".encode())) + "heartbeat".encode())
                            # print(json.dumps(data, indent=4))
                            self.protect_config = data
                            if data.get("SerialBoardID", {}).get("0") not in self.active_serial_board_list:
                                self.active_serial_board_list[port] = data.get("SerialBoardID")["0"]
                            response_data = {
                                "Msg": {"0": "ACK"},
                                "ReturnCode": {"0": 0},
                                "SerialBoardID": {"0": f"{self.active_serial_board_list[port]}"},
                                "ProtectBoardID": {"0": f"{self.active_protect_board_list[port]}"},
                                "TimerT1s": {"0": T1Timer_s},
                                "TimerT1ms": {"0": T1Timer_ms},
                                "Status": {"0": 0},
                                "Position": {"0": 0},
                                "RSSI": {"0": 0},
                                "WarCnt": {"0": 0},
                            }
                            self.send_data(response_data, port, False)
                            self.mode[port] = "Polling"
                            # self.logging.debug(f"{data.get('Msg')['0']} | {data.get('SerialBoardID')['0']} | Step_0")
                            self.logging.error(f"{data.get('Msg')['0']} | {data.get('SerialBoardID')['0']} | Step_0")
                        # Send Back the Check Results of Charging Step 1 for CC State
                        elif data.get("Msg")["0"] == "Check" and data.get("Step")["0"] == 1:
                            # self.logging.info(f"{json.dumps(data, indent=4)}")
                            response_data = {
                                "Msg": {"0": "ACK"},
                                "ReturnCode": {"0": 0},
                                "SerialBoardID": {"0": f"{self.active_serial_board_list[port]}"},
                                "ProtectBoardID": {"0": f"{self.active_protect_board_list[port]}"},
                                "TimerT1s": {"0": T1Timer_s},
                                "TimerT1ms": {"0": T1Timer_ms},
                                "Status": {"0": 0},
                                "Position": {"0": 1},
                                "RSSI": {"0": 0},
                                "WarCnt": {"0": 0},
                            }
                            self.send_data(response_data, port, False)
                            self.mode[port] = "Polling"
                            self.step[port] = data.get("Step")["0"]
                            self.step1_curr_cnt[port] = 0
                            # self.logging.debug(f"{data.get('Msg')['0']} | {data.get('SerialBoardID')['0']} | Step_1")
                            self.logging.error(f"{data.get('Msg')['0']} | {data.get('SerialBoardID')['0']} | Step_1")
                        # Send Back the Check Results of Charging Step 2 for Rest State
                        elif data.get("Msg")["0"] == "Check" and data.get("Step")["0"] == 2:
                            # self.logging.info(f"{json.dumps(data, indent=4)}")
                            response_data = {
                                "Msg": {"0": "ACK"},
                                "ReturnCode": {"0": 0},
                                "SerialBoardID": {"0": f"{self.active_serial_board_list[port]}"},
                                "ProtectBoardID": {"0": f"{self.active_protect_board_list[port]}"},
                                "TimerT1s": {"0": T1Timer_s},
                                "TimerT1ms": {"0": T1Timer_ms},
                                "Status": {"0": 0},
                                "Position": {"0": 1},
                                "RSSI": {"0": 0},
                                "WarCnt": {"0": 0},
                            }
                            self.send_data(response_data, port, False)
                            self.mode[port] = "Polling"
                            self.step[port] = data.get("Step")["0"]
                            # self.logging.debug(f"{data.get('Msg')['0']} | {data.get('SerialBoardID')['0']} | Step_2")
                            self.logging.error(f"{data.get('Msg')['0']} | {data.get('SerialBoardID')['0']} | Step_2")
                        # Send Back the Check Results of Charging Step 3 for End State
                        elif data.get("Msg")["0"] == "Check" and data.get("Step")["0"] == 3:
                            response_data = {
                                "Msg": {"0": "ACK"},
                                "ReturnCode": {"0": 0},
                                "SerialBoardID": {"0": f"{self.active_serial_board_list[port]}"},
                                "ProtectBoardID": {"0": f"{self.active_protect_board_list[port]}"},
                                "TimerT1s": {"0": T1Timer_s},
                                "TimerT1ms": {"0": T1Timer_ms},
                                "Status": {"0": 0},
                                "Position": {"0": 0},
                                "RSSI": {"0": 0},
                                "WarCnt": {"0": 0},
                            }
                            self.send_data(response_data, port, False)
                            # self.logging.debug(f"{data.get('Msg')['0']} | {data.get('SerialBoardID')['0']} | Step_3")
                            self.logging.error(f"{data.get('Msg')['0']} | {data.get('SerialBoardID')['0']} | Step_3")
                except TimeoutError:
                    pass
                
                # except Exception:
                #     self.logging.error(f"send_and_receive failed\n{self.logger.get_slim_error_log()}")

                except Exception as e:
                    self.logging.error(
                        f"send_and_receive failed | port={port} | {type(e).__name__}: {e}\n"
                        f"{self.logger.get_slim_error_log()}"
                    )

                    # # Pool mode: port 發生 exception 時，整個 rack 進 cooldown，
                    # # 不直接回 inactive_ports。
                    # rack = self.port_to_rack(port)
                    # self.move_rack_to_cooldown(
                    #     rack=rack,
                    #     cooldown_sec=self.rack_cooldown_sec,
                    # )

                    
                    # Pool mode:
                    # 只處理當前 port。
                    # 等同 rack 兩個 port 都斷線/被清掉後，才讓 rack 進 cooldown。
                    self.move_port_to_disconnected_and_maybe_cooldown(
                        port=port,
                        cooldown_sec=self.rack_cooldown_sec,
                    )


                    ############ MPD test #############
                    # self._write_port_status()
                    ####################################


            if self.Perf_flag:
                self.logging.info(f"SendRecvTime: {time.time() - start_time:.6f} s")
        except Exception:
            self.logging.error(f"send_and_receive failed\n{self.logger.get_slim_error_log()}")

    def run(self):
        """
        Handles scan ports and socket messages.
        Pool mode:
        - start from zero active/inactive ports
        - periodically choose one rack from available pool
        - max active ports is capped by self.max_active_ports
        - disconnected rack enters cooldown before returning to pool
        """
        try:
            self.last_process_data_time = time.time()
            self.last_process_heartbeat_time = time.time()
            while True:
                try:
                    start_time = time.time()

                    self.schedule_one_random_rack_from_pool(
                        interval_sec=self.random_enable_interval_sec,
                    )

                    self.poll_ports()
                    self.send_and_receive()

                    elapsed = time.time() - start_time
                    sleep_time = max(0, 2 - elapsed)
                    time.sleep(sleep_time)
                    self.current_time = time.time()
                    if self.current_time - self.last_time > 10:
                        self.logging.error(
                            f"Active Ports:[{len(self.active_ports)}]\n{self.active_ports}\n"
                            # f"Inactive Ports:[{len(self.inactive_ports)}]\n{self.inactive_ports}\n"
                            # f"Available rack pool size:{len(self.available_rack_pool)} | "
                            f"Cooldown racks:{sorted(self.cooldown_racks.keys())}"
                        )
                        self.last_time = self.current_time
                except Exception:
                    self.logging.error(f"Run failed\n{self.logger.get_slim_error_log()}")
        except Exception:
            self.logging.error(f"Run failed\n{self.logger.get_slim_error_log()}")

if __name__ == "__main__":
    
    try:
        env_max_active_racks = os.environ.get("ESP_MAX_ACTIVE_RACKS")
        max_active_racks = 20 if env_max_active_racks in (None, "", "None") else int(env_max_active_racks)

        env_enable_interval_sec = os.environ.get("ESP_RANDOM_ENABLE_INTERVAL_SEC")
        random_enable_interval_sec = 60 if env_enable_interval_sec in (None, "", "None") else int(env_enable_interval_sec)

        env_cooldown_sec = os.environ.get("ESP_RACK_COOLDOWN_SEC")
        rack_cooldown_sec = 120 if env_cooldown_sec in (None, "", "None") else int(env_cooldown_sec)

        esp_simulation_obj = ESP_simulation(
            max_active_racks=max_active_racks,
            random_enable_interval_sec=random_enable_interval_sec,
            rack_cooldown_sec=rack_cooldown_sec,
        )
        esp_simulation_obj.run()
    except Exception:
        exc_type, exc_value, exc_tb = sys.exc_info()
        tb_summary = traceback.extract_tb(exc_tb)
        last_call = tb_summary[-1]

        err_msg = f"{exc_type.__name__}: {exc_value}"
        logging.error(
            f"Main function failed\n"
            f"File {last_call.filename}, line {last_call.lineno}, in {last_call.name}: {err_msg}"
        )

