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
    """
    Return the mapping CSV path used by the E2E test runtime.

    Args:
        None

    Returns:
        Path: A readable mapping CSV path.

    Raises:
        FileNotFoundError: If no mapping CSV can be found.
    """
    candidates = []
    configured = os.environ.get('CCMS_MAPPING_CSV')
    if configured:
        candidates.append(Path(configured))
    candidates.extend([
        REPO_ROOT / 'core' / 'db' / 'ServerMap.csv',
    ])
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError('No mapping.csv or core/db/ServerMap.csv was found')


class EventTrace:
    '''Thread-safe event list used to prove end-to-end ordering.'''

    def __init__(self):
        """
        Initialize an empty, thread-safe event trace.

        Args:
            None

        Returns:
            None
        """
        self.events: list[str] = []
        self._lock = threading.Lock()

    def add(self, event: str):
        """
        Append one event to the trace under the thread lock.

        Args:
            event (str): The event name to record.

        Returns:
            None
        """
        with self._lock:
            self.events.append(event)

    def snapshot(self) -> list[str]:
        """
        Return a copy of the recorded events in occurrence order.

        Args:
            None

        Returns:
            list[str]: A snapshot of the event sequence.
        """
        with self._lock:
            return list(self.events)


class MemoryRedis:
    '''Small Redis list implementation shared by CCMS and FlowControl.'''

    _queues: dict[str, deque] = {}
    _lock = threading.RLock()

    def __init__(self, *args, **kwargs):
        """
        Create a Redis-compatible in-memory client.

        Args:
            args (tuple): Positional Redis client arguments accepted for compatibility.
            kwargs (dict): Keyword Redis client arguments accepted for compatibility.

        Returns:
            None
        """
        return None

    @classmethod
    def reset(cls):
        """
        Clear all shared in-memory Redis queues.

        Args:
            None

        Returns:
            None
        """
        with cls._lock:
            cls._queues = {}

    def flushall(self):
        """
        Clear all test Redis data.

        Args:
            None

        Returns:
            None
        """
        self.reset()

    def delete(self, *keys):
        """
        Delete the specified keys from the in-memory Redis store.

        Args:
            keys (tuple): Keys to remove.

        Returns:
            int: The number of keys received.
        """
        with self._lock:
            for key in keys:
                self._queues.pop(self._key(key), None)
        return len(keys)

    def rpush(self, key, *values):
        """
        Append one or more values to the end of a Redis list.

        Args:
            key (str | bytes): The Redis list key.
            values (tuple): Values to append to the list.

        Returns:
            int: The list length after the append.
        """
        with self._lock:
            queue = self._queues.setdefault(self._key(key), deque())
            for value in values:
                if isinstance(value, str):
                    value = value.encode('utf-8')
                queue.append(value)
            return len(queue)

    def lpop(self, key):
        """
        Remove and return the first value from a Redis list.

        Args:
            key (str | bytes): The Redis list key.

        Returns:
            bytes | None: The first value, or None when the list is empty.
        """
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
        """
        Normalize a Redis key to a string.

        Args:
            key (str | bytes | object): The original Redis key.

        Returns:
            str: The normalized dictionary key.
        """
        return key.decode('utf-8') if isinstance(key, bytes) else str(key)


class FakeInflux:
    '''No-op persistence sink with the formatting methods used by production.'''

    def __init__(self):
        """
        Initialize the in-memory InfluxDB persistence sink.

        Args:
            None

        Returns:
            None
        """
        self.records: list[tuple] = []

    def write_log_influxdb(self, *args):
        """
        Record a production log write without connecting to InfluxDB.

        Args:
            args (tuple): Arguments supplied by the production caller.

        Returns:
            None
        """
        self.records.append(('log', *args))

    def write_ccs_logs_influxdb(self, *args):
        """
        Record a CCS log write without connecting to InfluxDB.

        Args:
            args (tuple): Arguments supplied by the production caller.

        Returns:
            None
        """
        self.records.append(('ccs', *args))

    def write_perf_influxdb(self, *args):
        """
        Record a performance-data write without connecting to InfluxDB.

        Args:
            args (tuple): Arguments supplied by the production caller.

        Returns:
            None
        """
        self.records.append(('perf', *args))

    def write_json_influxdb(self, *args):
        """
        Record a JSON-data write without connecting to InfluxDB.

        Args:
            args (tuple): Arguments supplied by the production caller.

        Returns:
            None
        """
        self.records.append(('json', *args))

    def write_msg_influxdb(self, *args):
        """
        Record an XML-message write without connecting to InfluxDB.

        Args:
            args (tuple): Arguments supplied by the production caller.

        Returns:
            None
        """
        self.records.append(('msg', *args))

    def write_pd_influxdb(self, *args):
        """
        Record a DataFrame write without connecting to InfluxDB.

        Args:
            args (tuple): Arguments supplied by the production caller.

        Returns:
            None
        """
        self.records.append(('pd', *args))

    def reformat_json_horizontal(self, value):
        """
        Serialize a value as one-line JSON for the production helper contract.

        Args:
            value (object): The value to serialize.

        Returns:
            str: The serialized JSON string.
        """
        return json.dumps(value, ensure_ascii=False, default=str)

    def format_df_mixed(self, value):
        """
        Convert a DataFrame or other value to a test-friendly string.

        Args:
            value (object): The value to format.

        Returns:
            str: The string representation of the value.
        """
        return str(value)

    def __getattr__(self, _name):
        """
        Return a no-op fallback for an unsupported InfluxDB API method.

        Args:
            _name (str): The missing attribute name.

        Returns:
            Callable: A fallback function that returns None.
        """
        def no_op(*_args, **_kwargs):
            """
            Execute an InfluxDB fallback call without side effects.

            Args:
                _args (tuple): Ignored positional arguments.
                _kwargs (dict): Ignored keyword arguments.

            Returns:
                None
            """
            return None

        return no_op


class FakeMssql:
    '''In-memory subset of SQL calls made by CCMS and FlowControl.'''

    def __init__(self, mapping: pd.DataFrame, trace: EventTrace):
        """
        Initialize the in-memory SQL Server adapter.

        Args:
            mapping (pandas.DataFrame): The ServerMap data used by the test.
            trace (EventTrace): The trace used to record Task creation.

        Returns:
            None
        """
        self.mapping = mapping.copy()
        self.trace = trace
        self.task_rows: list[dict] = []
        self.server_maps: list[pd.DataFrame] = []
        self._lock = threading.RLock()

    def query_db_pd(self, query):
        """
        Return in-memory data for the supported production SQL queries.

        Args:
            query (str | object): The production SQL query.

        Returns:
            pandas.DataFrame: The query result, or an empty DataFrame for unsupported queries.
        """
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
        """
        Write a DataFrame to the in-memory Task or ServerMap store.

        Args:
            output (pandas.DataFrame): The data to write.
            table_name (str): The target table name.

        Returns:
            None
        """
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
        """
        Ignore a production delete-table request.

        Args:
            _args (tuple): Ignored positional arguments.
            _kwargs (dict): Ignored keyword arguments.

        Returns:
            None
        """
        return None

    def delete_rack_status(self, *_args, **_kwargs):
        """
        Ignore a production rack-status delete request.

        Args:
            _args (tuple): Ignored positional arguments.
            _kwargs (dict): Ignored keyword arguments.

        Returns:
            None
        """
        return None

    def create_rack_status(self, *_args, **_kwargs):
        """
        Ignore a production rack-status create request.

        Args:
            _args (tuple): Ignored positional arguments.
            _kwargs (dict): Ignored keyword arguments.

        Returns:
            None
        """
        return None

    def update_db_pd(self, *_args, **_kwargs):
        """
        Ignore a production DataFrame update request.

        Args:
            _args (tuple): Ignored positional arguments.
            _kwargs (dict): Ignored keyword arguments.

        Returns:
            None
        """
        return None

    def clear_task_table(self, *_args, **_kwargs):
        """
        Ignore a production Task-table clear request.

        Args:
            _args (tuple): Ignored positional arguments.
            _kwargs (dict): Ignored keyword arguments.

        Returns:
            None
        """
        return None

    def update_pallet_status(self, *_args, **_kwargs):
        """
        Ignore a production pallet-status update request.

        Args:
            _args (tuple): Ignored positional arguments.
            _kwargs (dict): Ignored keyword arguments.

        Returns:
            None
        """
        return None

    def close_db_connect_pd(self):
        """
        Close the in-memory SQL adapter.

        Args:
            None

        Returns:
            None
        """
        return None

    def create_db_connect_pd(self):
        """
        Initialize the in-memory SQL adapter connection.

        Args:
            None

        Returns:
            None
        """
        return None

    def __getattr__(self, _name):
        """
        Return a no-op fallback for an unsupported SQL API method.

        Args:
            _name (str): The missing attribute name.

        Returns:
            Callable: A fallback function that returns None.
        """
        def no_op(*_args, **_kwargs):
            """
            Execute a SQL fallback call without side effects.

            Args:
                _args (tuple): Ignored positional arguments.
                _kwargs (dict): Ignored keyword arguments.

            Returns:
                None
            """
            return None

        return no_op


class FramedSocket:
    '''Big-endian four-byte length framing used by XML and ESP sockets.'''

    @staticmethod
    def send(sock: socket.socket, payload: bytes):
        """
        Send one payload with a four-byte big-endian length prefix.

        Args:
            sock (socket.socket): The connected TCP socket.
            payload (bytes): The payload to send.

        Returns:
            None
        """
        sock.sendall(struct.pack('>I', len(payload)) + payload)

    @staticmethod
    def recv_exact(sock: socket.socket, length: int) -> bytes:
        """
        Read an exact number of bytes from a socket.

        Args:
            sock (socket.socket): The connected TCP socket.
            length (int): The number of bytes to read.

        Returns:
            bytes: The complete byte sequence.

        Raises:
            ConnectionError: If the socket closes before all bytes are received.
        """
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
        """
        Receive one complete length-prefixed frame.

        Args:
            sock (socket.socket): The connected TCP socket.

        Returns:
            bytes: The frame payload without its length prefix.
        """
        header = cls.recv_exact(sock, 4)
        length = struct.unpack('>I', header)[0]
        return cls.recv_exact(sock, length)


class FrontendEndpoint:
    '''A real TCP endpoint representing the XMLServer side of CCMS.'''

    def __init__(self, trace: EventTrace):
        """
        Create the local TCP endpoint representing XMLServer.

        Args:
            trace (EventTrace): The trace used to record XMLServer events.

        Returns:
            None
        """
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
        """
        Send an XMLServer request to CCMS.

        Args:
            payload (bytes): The serialized XML request bytes.

        Returns:
            None
        """
        root = ET.fromstring(payload)  # noqa: S314
        name = root.findtext('./HEADER/MESSAGENAME') or ''
        self.trace.add('XMLServer_send:' + name)
        FramedSocket.send(self.xml_socket, payload)

    def receive(self) -> bytes:
        """
        Receive an XML response sent by CCMS to XMLServer.

        Args:
            None

        Returns:
            bytes: The response XML bytes.

        Raises:
            socket.timeout: If no complete frame arrives before the socket timeout.
        """
        payload = FramedSocket.recv(self.xml_socket)
        root = ET.fromstring(payload)  # noqa: S314
        name = root.findtext('./HEADER/MESSAGENAME') or ''
        self.trace.add('XMLServer_receive:' + name)
        return payload

    def close(self):
        """
        Close the frontend client, server, and listener sockets.

        Args:
            None

        Returns:
            None
        """
        for sock in (self.ccms_socket, self.xml_socket, self.listener):
            try:
                sock.close()
            except OSError:
                pass


class ESPProtocolClient:
    '''Deterministic ESPDevice speaking the production JSON framed protocol.'''

    def __init__(self, serialboard_id: str, protectboard_id: str, port: int,
                 behavior: str, trace: EventTrace, alarm_code: int = 2):
        """
        Create an ESPDevice client using the production JSON framing protocol.

        Args:
            serialboard_id (str): The ESPDevice SerialBoardID.
            protectboard_id (str): The ESPDevice ProtectBoardID.
            port (int): The local TCP port used by FlowControl.
            behavior (str): The response mode, such as ack, data, or alarm_sequence.
            trace (EventTrace): The trace used to record ESP events.
            alarm_code (int): The ALARM status code; defaults to 2 for Water.

        Returns:
            None
        """
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
        """
        Start the ESPDevice worker thread.

        Args:
            None

        Returns:
            None
        """
        self.thread.start()

    def stop(self):
        """
        Stop the ESPDevice worker and close its socket.

        Args:
            None

        Returns:
            None
        """
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
        """
        Retry the connection to the FlowControl ESP TCP server until stopped.

        Args:
            None

        Returns:
            None
        """
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
        """
        Receive FlowControl requests and respond with ACK, DATA, or ALARM payloads.

        Args:
            None

        Returns:
            None
        """
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
        """
        Serialize and send a JSON response using the production frame format.

        Args:
            value (dict): The JSON mapping to serialize and send.

        Returns:
            None
        """
        if self.sock is not None:
            message = value.get('Msg', {}).get('0', '')
            self.trace.add('ESP_send:' + self.serialboard_id + ':' + message)
            FramedSocket.send(self.sock, json.dumps(value).encode('utf-8'))

    def trigger_alarm(self):
        """
        Signal the ESPDevice to send its next ALARM payload.

        Args:
            None

        Returns:
            None
        """
        self.alarm_event.set()

    def _wait_for_alarm(self):
        """
        Wait for an alarm trigger and send one ALARM response.

        Args:
            None

        Returns:
            bool: True when an ALARM payload was sent; otherwise False when stopping.
        """
        while not self.stop_event.is_set() and not self.alarm_event.wait(0.05):
            pass
        if self.stop_event.is_set():
            return False
        self.alarm_event.clear()
        self._send(self._alarm_payload())
        return True

    def _identity(self):
        """
        Build the device identity fields shared by ESP responses.

        Args:
            None

        Returns:
            dict: SerialBoardID, ProtectBoardID, and Position fields.
        """
        return {
            'SerialBoardID': {'0': self.serialboard_id},
            'ProtectBoardID': {'0': self.protectboard_id},
            'Position': {str(index): index + 1 for index in range(6)},
        }

    def _ack_payload(self):
        """
        Build an ESP ACK response payload.

        Args:
            None

        Returns:
            dict: An ACK mapping with ReturnCode set to 0.
        """
        return {
            'Msg': {'0': 'ACK'},
            'ReturnCode': {'0': 0},
            **self._identity(),
        }

    def _data_payload(self):
        """
        Build a DATA payload containing the measurements expected by FlowControl.

        Args:
            None

        Returns:
            dict: A DATA mapping with voltage, temperature, current, and heartbeat fields.
        """
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
        """
        Build an ALARM payload containing OT error information.

        Args:
            None

        Returns:
            dict: An ALARM mapping with status, error, and identity fields.
        """
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
        """
        Create an isolated CCMS, FlowControl, and ESPDevice test topology.

        Args:
            None

        Returns:
            None
        """
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
        """
        Start the production message_api and CCMS receiver thread.

        Args:
            None

        Returns:
            None
        """
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
            """
            Record the received message name before calling the production XML parser.

            Args:
                xml_string (str): The XML request received by CCMS.

            Returns:
                object: The result returned by the production parser.
            """
            root = ET.fromstring(xml_string)  # noqa: S314
            name = root.findtext('./HEADER/MESSAGENAME') or ''
            self.trace.add('CCMS_receive:' + name)
            return original_parse(xml_string)

        def traced_send(message_name, request_xml, sbid='A0001', shid='0'):
            """
            Record a CCMS response before calling the production sender.

            Args:
                message_name (str): The XML message name being sent.
                request_xml (str): The XML content being sent.
                sbid (str): The response SerialBoardID; defaults to A0001.
                shid (str): The response StoreHouseID; defaults to 0.

            Returns:
                object: The result returned by the production sender.
            """
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
        """
        Build and send a W2002 request using the production XMLServer builder.

        Args:
            None

        Returns:
            None
        """
        payload = self.xml_server.create_request_w2002_xml(
            'StoreHouseStatusRequest',
            'E2E-TID-W2002',
            'E2E-TRX-W2002',
            1,
        )
        self.frontend.send(payload)

    def send_w2004(self):
        """
        Build and send a W2004 request using the production XMLServer builder.

        Args:
            None

        Returns:
            None
        """
        payload = self.xml_server.create_request_w2004_xml(
            'StoreHouseNGCheckRequest',
            'E2E-TID-W2004',
            'E2E-TRX-W2004',
            1,
        )
        self.frontend.send(payload)

    def send_w2005(self):
        """
        Build and send a W2005 request using the production XMLServer builder.

        Args:
            None

        Returns:
            None
        """
        payload = self.xml_server.create_request_w2005_xml(
            'JudgmentCompletionNotificationRequest',
            'E2E-TID-W2005',
            'E2E-TRX-W2005',
            1,
        )
        self.frontend.send(payload)

    def start_flow_controls(self, behavior: str, alarm_code: int = 2):
        """
        Start FlowControl and ESPDevice workers for the Tasks created by CCMS.

        Args:
            behavior (str): The ESP response mode: ack, data, alarm, or alarm_sequence.
            alarm_code (int): The ALARM status code; defaults to 2 for Water.

        Returns:
            None

        Raises:
            AssertionError: If CCMS did not create the expected two Task rows.
        """
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
        """
        Tell every test ESPDevice to send its next ALARM response.

        Args:
            alarm_code (int | None): An optional replacement ALARM status code.

        Returns:
            None
        """
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
        """
        Run one FlowControl initialization, request-processing, and algorithm cycle.

        Args:
            fc (FC_api): The production FlowControl instance to run.
            behavior (str): The ESP response mode for this test case.

        Returns:
            None
        """
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
        """
        Find the ProtectBoardID mapped to a SerialBoardID.

        Args:
            serialboard_id (str): The SerialBoardID to look up.

        Returns:
            str: The corresponding ProtectBoardID.

        Raises:
            AssertionError: If the SerialBoardID is absent from the mapping.
        """
        rows = self.mapping[
            self.mapping['SerialBoardID'].astype(str) == str(serialboard_id)
        ]
        if rows.empty:
            raise AssertionError('mapping does not contain ' + str(serialboard_id))
        return str(rows.iloc[0]['ProtectBoardID'])

    def wait_until(self, predicate, timeout=5.0, description='condition'):
        """
        Wait until a predicate becomes true and include recent trace events on timeout.

        Args:
            predicate (Callable[[], bool]): A zero-argument condition function.
            timeout (float): The maximum wait time in seconds; defaults to 5.0.
            description (str): The condition name shown in timeout errors.

        Returns:
            None

        Raises:
            AssertionError: If the predicate is still false when the timeout expires.
        """
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
        """
        Wait for an XMLServer response with the requested message name.

        Args:
            message_name (str): The HEADER/MESSAGENAME value to find.
            timeout (float): The maximum wait time in seconds; defaults to 10.0.
            occurrence (int): The occurrence number to return; defaults to 1.

        Returns:
            Element: The matching XML root element.

        Raises:
            AssertionError: If the frontend connection fails or the response times out.
        """
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
        """
        Stop workers, close sockets, and remove the test patches.

        Args:
            None

        Returns:
            None
        """
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
        """
        Return the compact test name used by the custom runner.

        Args:
            None

        Returns:
            str: The configured short name or unittest's default name.
        """
        return self.TEST_DESCRIPTIONS.get(self._testMethodName, super().__str__())

    def shortDescription(self):
        """
        Disable unittest's duplicate display of the test docstring.

        Args:
            None

        Returns:
            None
        """
        return None

    def _failure_message(self, message=None):
        """
        Build a failure message prefixed with the compact test name.

        Args:
            message (str | None): An optional failure-condition description.

        Returns:
            str: The prefixed failure message.
        """
        description = self.TEST_DESCRIPTIONS.get(self._testMethodName, self._testMethodName)
        return description + ': ' + (message or 'condition failed')

    def assertEqual(self, first, second, msg=None):
        """
        Wrap unittest equality assertions with a compact failure description.

        Args:
            first (object): The first value to compare.
            second (object): The second value to compare.
            msg (str | None): An optional failure-condition description.

        Returns:
            None

        Raises:
            AssertionError: If first and second are not equal.
        """
        return super().assertEqual(first, second, self._failure_message(msg))

    def setUp(self):
        """
        Create and start an isolated E2E runtime before each test.

        Args:
            None

        Returns:
            None
        """
        self.runtime = E2ERuntime()
        self.runtime.start()

    def tearDown(self):
        """
        Release the E2E runtime after each test.

        Args:
            None

        Returns:
            None
        """
        self.runtime.close()

    def assert_event_order(self, *events):
        """
        Verify that trace events occur in the requested order.

        Args:
            events (tuple[str, ...]): The event names that must appear in order.

        Returns:
            None

        Raises:
            AssertionError: If an event is missing or appears out of order.
        """
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
        """
        Verify that W2002 creates Tasks and receives ESP ACK responses.

        Args:
            None

        Returns:
            None

        Raises:
            AssertionError: If ACK fields, Task creation, or event order is incorrect.
        """
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
        """
        Verify that ESP DATA is returned and W2004 reports an OK status.

        Args:
            None

        Returns:
            None

        Raises:
            AssertionError: If DATA, W2004 fields, or event order is incorrect.
        """
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
        """
        Verify that an ESP ALARM becomes a W1001 AlarmStopReport.

        Args:
            None

        Returns:
            None

        Raises:
            AssertionError: If the alarm code, Water status, pallet count, or event order is incorrect.
        """
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
        """
        Verify the NG ALARM, W2005 completion, and W2004 NG status sequence.

        Args:
            None

        Returns:
            None

        Raises:
            AssertionError: If any response field or sequence step is incorrect.
        """
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
        """
        Verify that a temperature alarm escalates from W1001 NG to Water and completes W2005/W2004.

        Args:
            None

        Returns:
            None

        Raises:
            AssertionError: If the NG, Water, or follow-up status is incorrect.
        """
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
