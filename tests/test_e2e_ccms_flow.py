'''Process-level E2E tests for XMLServer -> CCMS -> FlowControl -> ESPDevice.

The production message_api and FC_api are started against real localhost TCP
sockets.  Redis, SQL Server and InfluxDB are replaced with deterministic
in-memory adapters so the test remains isolated and repeatable.
'''

from __future__ import annotations

import json
import logging
import os
import socket
import struct
import sys
import threading
import time
import unittest
import xml.etree.ElementTree as ET  # noqa: S405
from collections import deque
from pathlib import Path
from unittest.mock import patch

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
CORE_ROOT = REPO_ROOT / 'core'
SIMULATION_ROOT = CORE_ROOT / 'simulation'
for import_path in (str(CORE_ROOT), str(SIMULATION_ROOT)):
    if import_path not in sys.path:
        sys.path.append(import_path)

from balps.message_api import message_api  # noqa: E402
from balps.task_mgr import TaskManager  # noqa: E402
from fc.FlowControl import FC_api  # noqa: E402
from XMLServer import XMLSocketServer  # noqa: E402


logging.disable(logging.CRITICAL)


def mapping_path() -> Path:
    '''Use the supplied 168-board mapping when it is available.'''
    candidates = []
    configured = os.environ.get('CCMS_MAPPING_CSV')
    if configured:
        candidates.append(Path(configured))
    candidates.extend([
        Path(r'C:\Users\defer\Downloads\mapping.csv'),
        REPO_ROOT / 'core' / 'db' / 'ServerMap.csv',
    ])
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError('No mapping.csv or core/db/ServerMap.csv was found')


class EventTrace:
    '''Thread-safe event list used to prove end-to-end ordering.'''

    def __init__(self):
        self.events: list[str] = []
        self._lock = threading.Lock()

    def add(self, event: str):
        with self._lock:
            self.events.append(event)

    def snapshot(self) -> list[str]:
        with self._lock:
            return list(self.events)


class MemoryRedis:
    '''Small Redis list implementation shared by CCMS and FlowControl.'''

    _queues: dict[str, deque] = {}
    _lock = threading.RLock()

    def __init__(self, *args, **kwargs):
        return None

    @classmethod
    def reset(cls):
        with cls._lock:
            cls._queues = {}

    def flushall(self):
        self.reset()

    def delete(self, *keys):
        with self._lock:
            for key in keys:
                self._queues.pop(self._key(key), None)
        return len(keys)

    def rpush(self, key, *values):
        with self._lock:
            queue = self._queues.setdefault(self._key(key), deque())
            for value in values:
                if isinstance(value, str):
                    value = value.encode('utf-8')
                queue.append(value)
            return len(queue)

    def lpop(self, key):
        with self._lock:
            queue = self._queues.get(self._key(key))
            if not queue:
                return None
            value = queue.popleft()
            if not queue:
                self._queues.pop(self._key(key), None)
            return value

    @staticmethod
    def _key(key):
        return key.decode('utf-8') if isinstance(key, bytes) else str(key)


class FakeInflux:
    '''No-op persistence sink with the formatting methods used by production.'''

    def __init__(self):
        self.records: list[tuple] = []

    def write_log_influxdb(self, *args):
        self.records.append(('log', *args))

    def write_ccs_logs_influxdb(self, *args):
        self.records.append(('ccs', *args))

    def write_perf_influxdb(self, *args):
        self.records.append(('perf', *args))

    def write_json_influxdb(self, *args):
        self.records.append(('json', *args))

    def write_msg_influxdb(self, *args):
        self.records.append(('msg', *args))

    def write_pd_influxdb(self, *args):
        self.records.append(('pd', *args))

    def reformat_json_horizontal(self, value):
        return json.dumps(value, ensure_ascii=False, default=str)

    def format_df_mixed(self, value):
        return str(value)

    def __getattr__(self, _name):
        def no_op(*_args, **_kwargs):
            return None

        return no_op


class FakeMssql:
    '''In-memory subset of SQL calls made by CCMS and FlowControl.'''

    def __init__(self, mapping: pd.DataFrame, trace: EventTrace):
        self.mapping = mapping.copy()
        self.trace = trace
        self.task_rows: list[dict] = []
        self.server_maps: list[pd.DataFrame] = []
        self._lock = threading.RLock()

    def query_db_pd(self, query):
        query_text = str(query)
        with self._lock:
            if 'BoardMapping' in query_text:
                result = self.mapping[['SerialBoardID', 'ProtectBoardID']]
                return result.drop_duplicates('SerialBoardID').reset_index(drop=True)
            if 'DISTINCT task_name' in query_text:
                return pd.DataFrame({
                    'task_name': [row['task_name'] for row in self.task_rows],
                })
            if 'COUNT(*)' in query_text:
                return pd.DataFrame([[len(self.task_rows)]])
            if 'pallet_info' in query_text and 'FROM BALPS.dbo.Task' in query_text:
                return pd.DataFrame()
            return pd.DataFrame()

    def write_db_pd(self, output, table_name):
        with self._lock:
            if table_name == 'Task':
                for row in output.to_dict(orient='records'):
                    normalized = {str(key): value for key, value in row.items()}
                    self.task_rows.append(normalized)
                    task_name = str(normalized.get('task_name', ''))
                    self.trace.add('Task_created:' + task_name)
            elif table_name == 'ServerMap':
                self.server_maps.append(output.copy())

    def delete_db_table(self, *_args, **_kwargs):
        return None

    def delete_rack_status(self, *_args, **_kwargs):
        return None

    def create_rack_status(self, *_args, **_kwargs):
        return None

    def update_db_pd(self, *_args, **_kwargs):
        return None

    def clear_task_table(self, *_args, **_kwargs):
        return None

    def update_pallet_status(self, *_args, **_kwargs):
        return None

    def close_db_connect_pd(self):
        return None

    def create_db_connect_pd(self):
        return None

    def __getattr__(self, _name):
        def no_op(*_args, **_kwargs):
            return None

        return no_op


class FramedSocket:
    '''Big-endian four-byte length framing used by XML and ESP sockets.'''

    @staticmethod
    def send(sock: socket.socket, payload: bytes):
        sock.sendall(struct.pack('>I', len(payload)) + payload)

    @staticmethod
    def recv_exact(sock: socket.socket, length: int) -> bytes:
        chunks = []
        remaining = length
        while remaining:
            chunk = sock.recv(remaining)
            if not chunk:
                raise ConnectionError('socket closed while receiving a frame')
            chunks.append(chunk)
            remaining -= len(chunk)
        return b''.join(chunks)

    @classmethod
    def recv(cls, sock: socket.socket) -> bytes:
        header = cls.recv_exact(sock, 4)
        length = struct.unpack('>I', header)[0]
        return cls.recv_exact(sock, length)


class FrontendEndpoint:
    '''A real TCP endpoint representing the XMLServer side of CCMS.'''

    def __init__(self, trace: EventTrace):
        self.trace = trace
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(('127.0.0.1', 0))
        self.listener.listen(1)
        self.address = self.listener.getsockname()
        self.ccms_socket = socket.create_connection(self.address, timeout=2)
        self.xml_socket, _ = self.listener.accept()
        self.ccms_socket.settimeout(0.01)
        self.xml_socket.settimeout(0.2)

    def send(self, payload: bytes):
        root = ET.fromstring(payload)  # noqa: S314
        name = root.findtext('./HEADER/MESSAGENAME') or ''
        self.trace.add('XMLServer_send:' + name)
        FramedSocket.send(self.xml_socket, payload)

    def receive(self) -> bytes:
        payload = FramedSocket.recv(self.xml_socket)
        root = ET.fromstring(payload)  # noqa: S314
        name = root.findtext('./HEADER/MESSAGENAME') or ''
        self.trace.add('XMLServer_receive:' + name)
        return payload

    def close(self):
        for sock in (self.ccms_socket, self.xml_socket, self.listener):
            try:
                sock.close()
            except OSError:
                pass


class ESPProtocolClient:
    '''Deterministic ESPDevice speaking the production JSON framed protocol.'''

    def __init__(self, serialboard_id: str, protectboard_id: str, port: int,
                 behavior: str, trace: EventTrace, alarm_code: int = 2):
        self.serialboard_id = serialboard_id
        self.protectboard_id = protectboard_id
        self.port = port
        self.behavior = behavior
        self.trace = trace
        self.alarm_code = alarm_code
        self.stop_event = threading.Event()
        self.alarm_event = threading.Event()
        self.ready = threading.Event()
        self.errors: list[str] = []
        self.sock: socket.socket | None = None
        self.thread = threading.Thread(
            target=self._run,
            name='esp-' + serialboard_id,
            daemon=True,
        )

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.alarm_event.set()
        if self.sock is not None:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self.sock.close()
            except OSError:
                pass
        self.thread.join(timeout=2)

    def _run(self):
        while not self.stop_event.is_set():
            try:
                self.sock = socket.create_connection(('127.0.0.1', self.port), timeout=0.2)
                self.sock.settimeout(0.2)
                self.ready.set()
                self._serve()
                return
            except (ConnectionRefusedError, TimeoutError, socket.timeout, OSError) as error:
                if self.stop_event.is_set():
                    return
                if not isinstance(error, ConnectionRefusedError):
                    self.errors.append(str(error))
                time.sleep(0.01)
            except Exception as error:  # pragma: no cover - diagnostic guard
                self.errors.append(type(error).__name__ + ': ' + str(error))
                return

    def _serve(self):
        while not self.stop_event.is_set():
            try:
                payload = FramedSocket.recv(self.sock)
            except socket.timeout:
                continue
            except (ConnectionError, OSError):
                return
            request = json.loads(payload.decode('utf-8'))
            message = request.get('Msg', {}).get('0', '')
            self.trace.add('ESP_receive:' + self.serialboard_id + ':' + message)
            if message == 'Config':
                if self.behavior == 'ack':
                    self._send(self._ack_payload())
                elif self.behavior == 'data':
                    self._send(self._ack_payload())
                    self._send(self._data_payload())
                elif self.behavior in ('alarm', 'alarm_sequence'):
                    self._send(self._ack_payload())
                    if self.behavior == 'alarm':
                        self._wait_for_alarm()
                    else:
                        while self._wait_for_alarm():
                            pass
            elif message == 'Check':
                self._send(self._ack_payload())
            elif message == 'HeartbeatRequest':
                self._send({'Msg': {'0': 'HeartbeatAck'}})

    def _send(self, value: dict):
        if self.sock is not None:
            message = value.get('Msg', {}).get('0', '')
            self.trace.add('ESP_send:' + self.serialboard_id + ':' + message)
            FramedSocket.send(self.sock, json.dumps(value).encode('utf-8'))

    def trigger_alarm(self):
        self.alarm_event.set()

    def _wait_for_alarm(self):
        while not self.stop_event.is_set() and not self.alarm_event.wait(0.05):
            pass
        if self.stop_event.is_set():
            return False
        self.alarm_event.clear()
        self._send(self._alarm_payload())
        return True

    def _identity(self):
        return {
            'SerialBoardID': {'0': self.serialboard_id},
            'ProtectBoardID': {'0': self.protectboard_id},
            'Position': {str(index): index + 1 for index in range(6)},
        }

    def _ack_payload(self):
        return {
            'Msg': {'0': 'ACK'},
            'ReturnCode': {'0': 0},
            **self._identity(),
        }

    def _data_payload(self):
        return {
            'Msg': {'0': 'Data'},
            'ReturnCode': {'0': 0},
            'Status': {'0': 0},
            'Step': {'0': 0},
            'TimerT1s': {'0': int(time.time())},
            'TimerT1ms': {'0': 0},
            'Volt': {str(index): 4000 for index in range(6)},
            'Temp': {str(index): 250 for index in range(6)},
            'Curr': {'0': 1000},
            'WireVolt': {str(index): 100 for index in range(5)},
            'FETState': {'0': 1},
            'Fuse': {'0': 0},
            'AFE': {'0': '0x12'},
            'WifiSTDisConn': {'0': 0},
            'WifiLTDisConn': {'0': 0},
            'SocketSTDisConn': {'0': 0},
            'SocketLTDisConn': {'0': 0},
            'HeartbeatTxCount': {'0': 10},
            'HeartbeatLossCount': {'0': 0},
            'HeartbeatLastRttMs': {'0': 32},
            'HeartbeatRttMaxMs': {'0': 40},
            'HeatbeatTimeoutFlag': {'0': 0},
            'WifiReconnLastMs': {'0': 0},
            'WifiReconnAvgMs': {'0': 0},
            'WifiReconnMaxMs': {'0': 0},
            'WifiReconnTimes': {'0': 0},
            'SocketReconnLastMs': {'0': 0},
            'SocketReconnAvgMs': {'0': 0},
            'SocketReconnMaxMs': {'0': 0},
            'SocketReconnTimes': {'0': 0},
            'RSSI': {'0': 0},
            'WarCnt': {'0': 0},
            **self._identity(),
        }

    def _alarm_payload(self):
        return {
            'Msg': {'0': 'ALARM'},
            'ReturnCode': {'0': self.alarm_code},
            'Status': {'0': self.alarm_code},
            'ErrorCode': {str(index): 'OT' for index in range(6)},
            **self._identity(),
        }


class E2ERuntime:
    '''Owns one isolated CCMS/FlowControl/ESP TCP topology.'''

    def __init__(self):
        self.trace = EventTrace()
        self.mapping = pd.read_csv(mapping_path())
        self.mapping['StoreHouseID'] = self.mapping['StoreHouseID'].astype(int)
        self.mapping['Position'] = self.mapping['Position'].astype(int)
        self.mapping['PalletPosition'] = self.mapping['PalletPosition'].str.upper()
        self.influx = FakeInflux()
        self.mssql = FakeMssql(self.mapping, self.trace)
        self.frontend = FrontendEndpoint(self.trace)
        self.fc_objects: list[FC_api] = []
        self.fc_threads: list[threading.Thread] = []
        self.esp_clients: list[ESPProtocolClient] = []
        self.errors: list[str] = []
        self.alarm_triggered = threading.Event()
        self.alarm_condition = threading.Condition()
        self.alarm_round = 0
        self._redis_patch = None
        self.ccms = None
        self.ccms_thread = None

        task_mgr = object.__new__(TaskManager)
        task_mgr.ip = '127.0.0.1'
        task_mgr.mssql_obj = self.mssql
        task_mgr.influxdb_obj = self.influx
        task_mgr.logging = logging.getLogger('e2e-task-manager')
        self.task_mgr = task_mgr

        self.xml_server = object.__new__(XMLSocketServer)
        self.xml_server.meas_map = self.mapping

    def start(self):
        MemoryRedis.reset()
        self._redis_patch = patch('redis.StrictRedis', MemoryRedis)
        self._redis_patch.start()
        self.ccms = message_api(
            '127.0.0.1',
            self.frontend.address[1],
            self.influx,
            self.mssql,
            self.task_mgr,
        )
        original_parse = self.ccms.parse_and_handle_messages
        original_send = self.ccms.send_request

        def traced_parse(xml_string):
            root = ET.fromstring(xml_string)  # noqa: S314
            name = root.findtext('./HEADER/MESSAGENAME') or ''
            self.trace.add('CCMS_receive:' + name)
            return original_parse(xml_string)

        def traced_send(message_name, request_xml, sbid='A0001', shid='0'):
            self.trace.add('CCMS_send:' + message_name)
            return original_send(message_name, request_xml, sbid, shid)

        self.ccms.parse_and_handle_messages = traced_parse
        self.ccms.send_request = traced_send
        self.ccms.socket = self.frontend.ccms_socket
        self.ccms_thread = threading.Thread(
            target=self.ccms.run,
            name='ccms-message-api',
            daemon=True,
        )
        self.ccms_thread.start()

    def send_w2002(self):
        payload = self.xml_server.create_request_w2002_xml(
            'StoreHouseStatusRequest',
            'E2E-TID-W2002',
            'E2E-TRX-W2002',
            1,
        )
        self.frontend.send(payload)

    def send_w2004(self):
        payload = self.xml_server.create_request_w2004_xml(
            'StoreHouseNGCheckRequest',
            'E2E-TID-W2004',
            'E2E-TRX-W2004',
            1,
        )
        self.frontend.send(payload)

    def send_w2005(self):
        payload = self.xml_server.create_request_w2005_xml(
            'JudgmentCompletionNotificationRequest',
            'E2E-TID-W2005',
            'E2E-TRX-W2005',
            1,
        )
        self.frontend.send(payload)

    def start_flow_controls(self, behavior: str, alarm_code: int = 2):
        rows = list(self.mssql.task_rows)
        if len(rows) != 2:
            raise AssertionError('expected two Task rows, got ' + str(len(rows)))
        for row in rows:
            serialboard_id = str(row['task_name'])
            protectboard_id = self._protect_for(serialboard_id)
            port = 20000 + int(protectboard_id[4:7])
            esp = ESPProtocolClient(
                serialboard_id,
                protectboard_id,
                port,
                behavior,
                self.trace,
                alarm_code,
            )
            esp.start()
            self.esp_clients.append(esp)

            args = type('FlowArgs', (), {
                'config': str(CORE_ROOT / 'fc' / 'FlowControl.json'),
                'storehouse_id': 1,
                'serialboard_id': serialboard_id,
                'protectboard_id': protectboard_id,
                'eport': port,
                'ser_ip': '127.0.0.1',
            })()
            with patch('balps.mssql_mgr.mssql_api', return_value=self.mssql), \
                    patch('balps.influxdb_mgr.influxdb_api', return_value=self.influx):
                fc = FC_api(args)
            fc.mssql_obj = self.mssql
            fc.influxdb_obj = self.influx
            fc.meas_map_obj.influxdb_obj = self.influx
            self.fc_objects.append(fc)

            worker = threading.Thread(
                target=self._run_flow_control,
                args=(fc, behavior),
                name='flow-control-' + serialboard_id,
                daemon=True,
            )
            self.fc_threads.append(worker)
            worker.start()

    def trigger_alarm(self, alarm_code=None):
        if alarm_code is not None:
            for client in self.esp_clients:
                client.alarm_code = alarm_code
        self.alarm_triggered.set()
        with self.alarm_condition:
            self.alarm_round += 1
            self.alarm_condition.notify_all()
        for client in self.esp_clients:
            client.trigger_alarm()

    def _run_flow_control(self, fc: FC_api, behavior: str):
        try:
            fc.init_algorithm(0)
            self.trace.add('FC_ready:' + fc.sbid)
            fc.process_xml_request()
            self.trace.add('FC_process:' + fc.sbid)
            self.trace.add('FC_forward:' + fc.sbid)
            fc.run_algorithm()
            self.trace.add('FC_done:' + fc.sbid)
            if behavior == 'data':
                fc.run_algorithm()
                self.trace.add('FC_data:' + fc.sbid)
            elif behavior == 'alarm':
                while not self.alarm_triggered.wait(0.05):
                    if not fc.running:
                        return
                fc.run_algorithm()
                self.trace.add('FC_alarm:' + fc.sbid)
            elif behavior == 'alarm_sequence':
                last_round = 0
                for _index in range(2):
                    with self.alarm_condition:
                        while self.alarm_round <= last_round and fc.running:
                            self.alarm_condition.wait(0.05)
                        if not fc.running:
                            return
                        last_round = self.alarm_round
                    fc.run_algorithm()
                    self.trace.add('FC_alarm:' + fc.sbid)
        except Exception as error:
            self.errors.append(
                'FC ' + str(getattr(fc, 'sbid', '?')) + ': '
                + type(error).__name__ + ': ' + str(error)
            )

    def _protect_for(self, serialboard_id):
        rows = self.mapping[
            self.mapping['SerialBoardID'].astype(str) == str(serialboard_id)
        ]
        if rows.empty:
            raise AssertionError('mapping does not contain ' + str(serialboard_id))
        return str(rows.iloc[0]['ProtectBoardID'])

    def wait_until(self, predicate, timeout=5.0, description='condition'):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.01)
        details = '\n'.join(self.trace.snapshot()[-30:])
        raise AssertionError(
            'timeout waiting for ' + description + '; events:\n'
            + details + '; errors: ' + str(self.errors)
        )

    def wait_for_response(self, message_name: str, timeout=10.0, occurrence=1):
        received = getattr(self, '_received_xml', [])
        matches = 0
        for payload in received:
            root = ET.fromstring(payload)  # noqa: S314
            if root.findtext('./HEADER/MESSAGENAME') == message_name:
                matches += 1
                if matches == occurrence:
                    return root
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                payload = self.frontend.receive()
            except socket.timeout:
                continue
            except (ConnectionError, OSError) as error:
                raise AssertionError('frontend connection failed: ' + str(error)) from error
            received.append(payload)
            self._received_xml = received
            root = ET.fromstring(payload)  # noqa: S314
            if root.findtext('./HEADER/MESSAGENAME') == message_name:
                matches += 1
                if matches == occurrence:
                    return root
        details = '\n'.join(self.trace.snapshot()[-40:])
        raise AssertionError(
            'timeout waiting for XML ' + message_name + '; events:\n'
            + details + '; errors: ' + str(self.errors)
        )

    def close(self):
        for client in self.esp_clients:
            client.stop()
        for fc in self.fc_objects:
            fc.running = False
            for name in ('conn_obj', 'server_socket'):
                sock = getattr(fc, name, None)
                if sock:
                    try:
                        sock.close()
                    except OSError:
                        pass
        for thread in self.fc_threads:
            thread.join(timeout=2)
        if self.ccms is not None:
            self.ccms.stop()
        if self.ccms_thread is not None:
            self.ccms_thread.join(timeout=2)
        self.frontend.close()
        if self._redis_patch is not None:
            self._redis_patch.stop()
        MemoryRedis.reset()


class CCMSFixtureE2ETest(unittest.TestCase):
    '''E2E cases selected by short names in run_function_tests.py.'''

    TEST_DESCRIPTIONS = {
        'test_e2e_ccms_flow_ack': 'E2E W2002 ACK',
        'test_e2e_ccms_flow_data_then_w2004': 'E2E DATA->W2004',
        'test_e2e_ccms_flow_alarm': 'E2E ALARM->W1001',
        'test_e2e_ccms_flow_ng_w2005_w2004': 'E2E NG->W2005->W2004',
        'test_e2e_ccms_flow_ng_escalates_to_water': 'E2E W1001 NG->Water',
    }

    def __str__(self):
        return self.TEST_DESCRIPTIONS.get(self._testMethodName, super().__str__())

    def shortDescription(self):
        return None

    def _failure_message(self, message=None):
        description = self.TEST_DESCRIPTIONS.get(self._testMethodName, self._testMethodName)
        return description + ': ' + (message or 'condition failed')

    def assertEqual(self, first, second, msg=None):
        return super().assertEqual(first, second, self._failure_message(msg))

    def setUp(self):
        self.runtime = E2ERuntime()
        self.runtime.start()

    def tearDown(self):
        self.runtime.close()

    def assert_event_order(self, *events):
        actual = self.runtime.trace.snapshot()
        cursor = -1
        for event in events:
            try:
                cursor = actual.index(event, cursor + 1)
            except ValueError:
                self.fail(
                    'condition failed: missing event ' + event
                    + '; actual events=' + str(actual)
                )

    def test_e2e_ccms_flow_ack(self):
        '''E2E W2002 ACK'''
        self.runtime.send_w2002()
        self.runtime.wait_until(
            lambda: len(self.runtime.mssql.task_rows) == 2,
            description='CCMS creates two FlowControl tasks',
        )
        self.runtime.start_flow_controls('ack')
        root = self.runtime.wait_for_response('StoreHouseStatusReply')
        self.assertEqual(root.findtext('./RETURN/RETURNCODE'), '0')
        self.assertEqual(root.findtext('./BODY/StoreHouseID'), '1')
        self.runtime.wait_until(
            lambda: all(
                any(
                    'ESP_receive:' + str(row.get('task_name')) + ':Config' == event
                    for event in self.runtime.trace.snapshot()
                )
                for row in self.runtime.mssql.task_rows
            ),
            description='both ESP devices receive Config',
        )
        self.assert_event_order(
            'XMLServer_send:StoreHouseStatusRequest',
            'CCMS_receive:StoreHouseStatusRequest',
            'Task_created:C00001',
            'FC_process:C00001',
            'ESP_receive:C00001:Config',
            'CCMS_send:StoreHouseStatusReply',
            'XMLServer_receive:StoreHouseStatusReply',
        )

    def test_e2e_ccms_flow_data_then_w2004(self):
        '''E2E DATA->W2004'''
        self.runtime.send_w2002()
        self.runtime.wait_until(
            lambda: len(self.runtime.mssql.task_rows) == 2,
            description='CCMS creates two FlowControl tasks',
        )
        self.runtime.start_flow_controls('data')
        self.runtime.wait_for_response('StoreHouseStatusReply')
        self.runtime.wait_until(
            lambda: all(
                any(
                    'FC_data:' + str(row.get('task_name')) == event
                    for event in self.runtime.trace.snapshot()
                )
                for row in self.runtime.mssql.task_rows
            ),
            description='both ESP devices return DATA',
        )
        self.runtime.send_w2004()
        root = self.runtime.wait_for_response('StoreHouseNGCheckReply')
        self.assertEqual(root.findtext('./RETURN/RETURNCODE'), '0')
        self.assertEqual(root.findtext('./BODY/NGStatus'), 'OK')
        self.assert_event_order(
            'XMLServer_send:StoreHouseStatusRequest',
            'CCMS_receive:StoreHouseStatusRequest',
            'ESP_send:C00001:Data',
            'XMLServer_send:StoreHouseNGCheckRequest',
            'CCMS_receive:StoreHouseNGCheckRequest',
            'CCMS_send:StoreHouseNGCheckReply',
            'XMLServer_receive:StoreHouseNGCheckReply',
        )

    def test_e2e_ccms_flow_alarm(self):
        '''E2E ALARM->W1001'''
        self.runtime.send_w2002()
        self.runtime.wait_until(
            lambda: len(self.runtime.mssql.task_rows) == 2,
            description='CCMS creates two FlowControl tasks',
        )
        self.runtime.start_flow_controls('alarm', alarm_code=2)
        self.runtime.wait_for_response('StoreHouseStatusReply')
        self.runtime.trigger_alarm()
        root = self.runtime.wait_for_response('AlarmStopReport')
        self.assertEqual(root.findtext('./RETURN/RETURNCODE'), '2')
        self.assertEqual(root.findtext('./BODY/NGStatus'), 'Water')
        self.assertEqual(len(root.findall('./BODY/ErrorInfoList/Pallet')), 2)
        self.assert_event_order(
            'XMLServer_send:StoreHouseStatusRequest',
            'CCMS_receive:StoreHouseStatusRequest',
            'ESP_send:C00001:ALARM',
            'CCMS_send:AlarmStopReport',
            'XMLServer_receive:AlarmStopReport',
        )

    def test_e2e_ccms_flow_ng_w2005_w2004(self):
        '''E2E NG->W2005->W2004'''
        self.runtime.send_w2002()
        self.runtime.wait_until(
            lambda: len(self.runtime.mssql.task_rows) == 2,
            description='CCMS creates two FlowControl tasks',
        )
        self.runtime.start_flow_controls('alarm', alarm_code=1)
        self.runtime.wait_for_response('StoreHouseStatusReply')
        self.runtime.trigger_alarm()
        alarm_root = self.runtime.wait_for_response('AlarmStopReport')
        self.assertEqual(alarm_root.findtext('./RETURN/RETURNCODE'), '1')
        self.assertEqual(alarm_root.findtext('./BODY/NGStatus'), 'NG')

        self.runtime.send_w2005()
        stop_root = self.runtime.wait_for_response('JudgmentCompletionNotificationReply')
        self.assertEqual(stop_root.findtext('./RETURN/RETURNCODE'), '0')

        self.runtime.send_w2004()
        status_root = self.runtime.wait_for_response('StoreHouseNGCheckReply')
        self.assertEqual(status_root.findtext('./RETURN/RETURNCODE'), '1')
        self.assertEqual(status_root.findtext('./BODY/NGStatus'), 'NG')
        self.assert_event_order(
            'XMLServer_send:StoreHouseStatusRequest',
            'CCMS_receive:StoreHouseStatusRequest',
            'ESP_send:C00001:ALARM',
            'CCMS_send:AlarmStopReport',
            'XMLServer_receive:AlarmStopReport',
            'XMLServer_send:JudgmentCompletionNotificationRequest',
            'CCMS_receive:JudgmentCompletionNotificationRequest',
            'CCMS_send:JudgmentCompletionNotificationReply',
            'XMLServer_receive:JudgmentCompletionNotificationReply',
            'XMLServer_send:StoreHouseNGCheckRequest',
            'CCMS_receive:StoreHouseNGCheckRequest',
            'CCMS_send:StoreHouseNGCheckReply',
            'XMLServer_receive:StoreHouseNGCheckReply',
        )

    def test_e2e_ccms_flow_ng_escalates_to_water(self):
        '''E2E W1001 NG->Water'''
        self.runtime.send_w2002()
        self.runtime.wait_until(
            lambda: len(self.runtime.mssql.task_rows) == 2,
            description='CCMS creates two FlowControl tasks',
        )
        self.runtime.start_flow_controls('alarm_sequence', alarm_code=1)
        self.runtime.wait_for_response('StoreHouseStatusReply')

        self.runtime.trigger_alarm(alarm_code=1)
        ng_root = self.runtime.wait_for_response('AlarmStopReport', occurrence=1)
        self.assertEqual(ng_root.findtext('./RETURN/RETURNCODE'), '1')
        self.assertEqual(ng_root.findtext('./BODY/NGStatus'), 'NG')

        self.runtime.trigger_alarm(alarm_code=2)
        water_root = self.runtime.wait_for_response('AlarmStopReport', occurrence=2)
        self.assertEqual(water_root.findtext('./RETURN/RETURNCODE'), '2')
        self.assertEqual(water_root.findtext('./BODY/NGStatus'), 'Water')

        self.runtime.send_w2005()
        self.runtime.wait_for_response('JudgmentCompletionNotificationReply')
        self.runtime.send_w2004()
        status_root = self.runtime.wait_for_response('StoreHouseNGCheckReply')
        self.assertEqual(status_root.findtext('./RETURN/RETURNCODE'), '2')
        self.assertEqual(status_root.findtext('./BODY/NGStatus'), 'Water')
        self.assert_event_order(
            'ESP_send:C00001:ALARM',
            'CCMS_send:AlarmStopReport',
            'ESP_send:C00001:ALARM',
            'CCMS_send:AlarmStopReport',
            'CCMS_send:JudgmentCompletionNotificationReply',
            'CCMS_send:StoreHouseNGCheckReply',
        )


if __name__ == '__main__':
    unittest.main(verbosity=2)
