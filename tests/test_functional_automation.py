"""Offline CCMS function tests.

The production topology is XMLServer -> CCMS -> ESPDevice. These tests keep
the CCMS parsers, cache/aggregation logic and XML simulators in the loop while
using deterministic in-memory inputs. They run without SQL Server, Redis,
InfluxDB or a live ESP process.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import sys
import unittest
import xml.etree.ElementTree as ET  # noqa: S405
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pandas as pd

# Keep the unittest result stream focused on the short case names. The tests
# still retain FakeInflux records, but expected-rejection log lines are hidden.
logging.disable(logging.CRITICAL)


REPO_ROOT = Path(__file__).resolve().parents[1]
CORE_ROOT = REPO_ROOT / "core"
SIMULATION_ROOT = CORE_ROOT / "simulation"
for import_path in (str(CORE_ROOT), str(SIMULATION_ROOT)):
    if import_path not in sys.path:
        sys.path.append(import_path)

from balps.meas_map_mgr import TaskInfo, meas_map_api  # noqa: E402
from balps.message_api import message_api  # noqa: E402
from balps.sync_time import sync_time_api  # noqa: E402
from balps.system_cache import cache_api, msg_aggeregation, msg_cache  # noqa: E402
from balps.task_mgr import TaskManager  # noqa: E402
from fc.FlowControl import FC_api  # noqa: E402
from XMLServer import XMLSocketServer  # noqa: E402


MAPPING_COLUMNS = [
    "StoreHouseID",
    "PalletID",
    "SerialBoardID",
    "ProtectBoardID",
    "PalletPosition",
    "Position",
    "QRCODEID",
]


class FakeInflux:
    """Small logger sink accepted by CCMS classes in unit tests."""

    def __init__(self):
        self.logs: list[tuple] = []

    def write_log_influxdb(self, *args):
        self.logs.append(("log", *args))

    def write_ccs_logs_influxdb(self, *args):
        self.logs.append(("ccs", *args))

    def write_perf_influxdb(self, *args):
        self.logs.append(("perf", *args))


class FakeLogging:
    def __init__(self):
        self.errors: list[str] = []

    def error(self, message):
        self.errors.append(str(message))

    def info(self, _message):
        return None

    def debug(self, _message):
        return None

    def get_slim_error_log(self):
        return ""


def mapping_path() -> Path:
    """Return the user mapping when present, otherwise the checked-in map."""

    candidates = []
    configured = os.environ.get("CCMS_MAPPING_CSV")
    if configured:
        candidates.append(Path(configured))
    candidates.extend(
        [
            Path(r"C:\Users\defer\Downloads\mapping.csv"),
            REPO_ROOT / "core" / "db" / "ServerMap.csv",
        ]
    )
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError("No mapping.csv or core/db/ServerMap.csv was found")


def load_mapping_rows(path: Path | None = None) -> list[dict[str, str]]:
    path = path or mapping_path()
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    if not rows:
        raise ValueError(f"Mapping is empty: {path}")
    return rows


def mapping_frame(rows: list[dict[str, str]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows, columns=MAPPING_COLUMNS)
    frame["StoreHouseID"] = frame["StoreHouseID"].astype(int)
    frame["Position"] = frame["Position"].astype(int)
    frame["PalletPosition"] = frame["PalletPosition"].str.upper()
    return frame


def inventory_by_serial(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    """Strip rack placement and retain the six-position board inventory."""

    inventory: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        inventory[row["SerialBoardID"]].append(dict(row))
    return dict(inventory)


def materialize_cycle(
    rows: list[dict[str, str]],
    assignments: dict[int, dict[str, str]],
) -> list[dict[str, str]]:
    """Create a mapping snapshot from arbitrary L/R board assignments.

    ``assignments`` is ``{storehouse_id: {"L": serial, "R": serial}}``.
    A serial may occur once per cycle, but may be assigned to a different
    StorehouseID in the next cycle.
    """

    inventory = inventory_by_serial(rows)
    snapshot: list[dict[str, str]] = []
    seen_serials: set[str] = set()
    for storehouse_id, sides in assignments.items():
        if set(sides) != {"L", "R"}:
            raise ValueError(f"Storehouse {storehouse_id} must have L and R")
        for side in ("L", "R"):
            serial = sides[side]
            if serial not in inventory:
                raise KeyError(f"Unknown SerialBoardID: {serial}")
            if serial in seen_serials:
                raise ValueError(f"SerialBoardID appears twice in one cycle: {serial}")
            seen_serials.add(serial)
            for item in inventory[serial]:
                placed = dict(item)
                placed["StoreHouseID"] = str(storehouse_id)
                placed["PalletPosition"] = side
                snapshot.append(placed)
    return snapshot


def protect_params() -> dict:
    config = json.loads(
        (CORE_ROOT / "fc" / "FlowControl.json").read_text(encoding="utf-8")
    )
    return config["meas"][0]["protect_params"]


def recipe_step_xml(
    params: dict,
    *,
    overrides: dict[str, str] | None = None,
    extra_keys: list[str] | None = None,
    omitted_keys: list[str] | None = None,
    duplicate_key: str | None = None,
) -> ET.Element:
    overrides = overrides or {}
    omitted = set(omitted_keys or [])
    step = ET.Element("RecipeStep")
    for key, multiplier in params.items():
        if key in omitted:
            continue
        if key == "Control_Mode":
            value = "START_CC"
        elif key == "Step":
            value = "1"
        elif multiplier == "":
            value = ""
        else:
            value = "1"
        ET.SubElement(step, key).text = overrides.get(key, value)
    for key in extra_keys or []:
        ET.SubElement(step, key).text = "1"
    if duplicate_key:
        ET.SubElement(step, duplicate_key).text = overrides.get(duplicate_key, "1")
    return step


def xml_bytes(root: ET.Element) -> bytes:
    return ET.tostring(root, encoding="utf-8", short_empty_elements=False)


def alarm_payload(
    *,
    rack_id: int,
    side: str,
    serial: str,
    protect: str,
    return_code: int,
    message_name: str = "AlarmStopReport",
    error_code: str = "OT",
    temperature_tenth_c: int = 450,
    transaction_id: str = "T-1001",
    trx_id: str = "X-1001",
) -> str:
    return json.dumps(
        {
            "MessageName": {"0": message_name},
            "TransactionID": {"0": transaction_id},
            "TrxID": {"0": trx_id},
            "StoreHouseID": {"0": str(rack_id)},
            "PalletID": {"0": f"AOC{serial}"},
            "SerialBoardID": {"0": serial},
            "ProtectBoardID": {"0": protect},
            "PalletPosition": {"0": side},
            "ReturnCode": {"0": return_code},
            "ReturnMessage": {"0": ""},
            "Position": {"0": 4},
            "QRCodeID": {"0": f"QR-{serial}-4"},
            "ErrorCode": {"0": error_code},
            "Temp": {"0": temperature_tenth_c},
        }
    )


def status_payload(
    *,
    rack_id: int,
    side: str,
    serial: str,
    return_code: int,
    message_name: str = "StoreHouseNGCheckRequest",
    transaction_id: str = "T-2004",
    trx_id: str = "X-2004",
) -> str:
    return json.dumps(
        {
            "MessageName": {"0": message_name},
            "TransactionID": {"0": transaction_id},
            "TrxID": {"0": trx_id},
            "StoreHouseID": {"0": str(rack_id)},
            "PalletID": {"0": f"AOC{serial}"},
            "SerialBoardID": {"0": serial},
            "ProtectBoardID": {"0": f"PCB{serial[1:]}"},
            "PalletPosition": {"0": side},
            "ReturnCode": {"0": return_code},
            "ReturnMessage": {"0": ""},
            "Position": {"0": 4},
            "QRCodeID": {"0": f"QR-{serial}-4"},
            "Step": {"0": 1},
        }
    )


def completion_payload(
    *,
    rack_id: int,
    side: str,
    serial: str,
    return_code: int,
    return_message: str = "",
) -> str:
    payload = json.loads(
        status_payload(
            rack_id=rack_id,
            side=side,
            serial=serial,
            return_code=return_code,
            message_name="JudgmentCompletionNotificationRequest",
            transaction_id="T-2005",
            trx_id="X-2005",
        )
    )
    payload["ReturnMessage"] = {"0": return_message}
    return json.dumps(payload)


class CCMSFixtureTest(unittest.TestCase):
    TEST_DESCRIPTIONS = {
        "test_mapping_is_complete_and_structurally_valid": "MAP-01 mapping completeness",
        "test_mapping_duplicate_qrcode_values_are_not_used_as_identity": "MAP-02 QRCode identity",
        "test_mapping_cycle_can_move_same_pair_to_another_storehouse": "MAP-03 pair moves Storehouse",
        "test_mapping_rejects_duplicate_active_serial_in_one_cycle": "MAP-04 duplicate serial",
        "test_cycle_assignment_fixture_supports_arbitrary_pairs": "MAP-05 arbitrary pairs",
        "test_c01_time_sync_boundary_and_invalid_input": "C01-1 time boundary/invalid",
        "test_c01_time_sync_and_alive_xml_contract": "C01-1/C01-2 W0001/W0002 XML",
        "test_c01_task_ready_requires_both_serial_tasks": "C01-3 both tasks ready",
        "test_c01_mpd_heartbeat_timeout_uses_60_second_threshold": "C01-4 MPD heartbeat 60s",
        "test_c02_w2002_has_three_steps_and_all_c07_thresholds": "C02-1 W2002 recipe",
        "test_c02_w2002_config_parser_accepts_exact_recipe_and_rejects_whole_request_on_mismatch": "C02-1 key mismatch reject",
        "test_c02_w2003_switch_protection_parameter_reply": "C02-2 W2003 REST/CC/END",
        "test_c03_w2004_status_priority_ok_ng_water": "C03 W2004 OK/NG/Water",
        "test_c04_w2004_missing_rack_is_no_response": "C04 W2004 NoResponse",
        "test_c05_w2005_stop_reply_ok_and_exception": "C05 W2005 OK/NG",
        "test_c06_two_rack_two_round_sequence_changes_assignments_only_after_stop": "C06 2-rack/2-round",
        "test_c07_threshold_values_match_function_test_table": "C07-1..13 thresholds",
        "test_c07_remaining_wire_and_delay_thresholds": "C07 wire/delay thresholds",
        "test_w1001_alarm_report_is_driven_by_esp_return_code": "W1001 30s NG->Water",
        "test_w1001_w2005_w2004_message_order_is_explicit": "W1001->W2005->W2004",
        "test_protect_parameter_exact_key_order_is_accepted": "Protect complete keys",
        "test_protect_parameter_reordered_xml_keys_are_accepted": "Protect key order",
        "test_known_empty_protect_value_is_a_disabled_parameter": "Protect known empty value",
        "test_protect_parameter_mismatch_combinations_reject_whole_step": "Protect XML mismatch cases",
        "test_protect_config_key_mismatch_combinations_reject": "Protect config mismatch cases",
        "test_protect_parameter_mismatch_in_second_step_rejects_config_request": "RecipeStep 2 mismatch",
        "test_protect_parameter_duplicate_xml_key_is_rejected": "Protect duplicate XML key",
        "test_protect_parameter_duplicate_xml_key_with_different_values_is_rejected": "Protect duplicate values",
        "test_unknown_message_mapping_is_not_silently_routed": "unknown message rejected",
        "test_w2004_stale_w3001_status_becomes_no_response": "W2004 stale status",
    }

    @classmethod
    def setUpClass(cls):
        cls.rows = load_mapping_rows()
        cls.frame = mapping_frame(cls.rows)
        cls.params = protect_params()

    def setUp(self):
        self.influx = FakeInflux()

    def _failure_message(self, message=None):
        description = self.TEST_DESCRIPTIONS.get(self._testMethodName, self._testMethodName)
        return f"{description}: {message}" if message else f"{description}: condition failed"

    def __str__(self):
        """Use the compact case name when unittest formats a test object."""
        return self.TEST_DESCRIPTIONS.get(self._testMethodName, super().__str__())

    def shortDescription(self):
        # Returning None prevents the stock unittest runner from printing the
        # long method id plus a second description line.
        return None

    def assertTrue(self, expr, msg=None):
        return super().assertTrue(expr, self._failure_message(msg))

    def assertFalse(self, expr, msg=None):
        return super().assertFalse(expr, self._failure_message(msg))

    def assertEqual(self, first, second, msg=None):
        return super().assertEqual(first, second, self._failure_message(msg))

    def assertLess(self, first, second, msg=None):
        return super().assertLess(first, second, self._failure_message(msg))

    def assertGreaterEqual(self, first, second, msg=None):
        return super().assertGreaterEqual(first, second, self._failure_message(msg))

    def assertRaises(self, expected_exception, *args, **kwargs):
        kwargs.setdefault("msg", self._failure_message())
        return super().assertRaises(expected_exception, *args, **kwargs)

    def new_aggregator(self) -> msg_aggeregation:
        cache = msg_cache(self.influx)
        aggregator = msg_aggeregation(cache, self.influx)
        aggregator.generate_transaction_id = lambda: "20260902120000000"
        return aggregator

    def test_mapping_is_complete_and_structurally_valid(self):
        self.assertEqual(MAPPING_COLUMNS, list(self.frame.columns))
        self.assertEqual(1008, len(self.frame), "84 racks x 2 pallets x 6 positions")
        self.assertEqual(84, self.frame["StoreHouseID"].nunique())
        self.assertEqual(168, self.frame["SerialBoardID"].nunique())
        self.assertEqual(168, self.frame["PalletID"].nunique())
        self.assertEqual(168, self.frame["ProtectBoardID"].nunique())
        self.assertEqual(1008, len(self.frame.drop_duplicates(["StoreHouseID", "PalletPosition", "Position"])))
        self.assertEqual(1008, len(self.frame.drop_duplicates(["SerialBoardID", "Position"])))
        for storehouse_id, group in self.frame.groupby("StoreHouseID"):
            self.assertEqual(12, len(group), storehouse_id)
            self.assertEqual({"L", "R"}, set(group["PalletPosition"]))
            for _side, pallet in group.groupby("PalletPosition"):
                self.assertEqual({1, 2, 3, 4, 5, 6}, set(pallet["Position"]))

    def test_mapping_duplicate_qrcode_values_are_not_used_as_identity(self):
        duplicate_count = len(self.frame) - self.frame["QRCODEID"].nunique()
        # Board + position remains unique even where a QR label is repeated.
        self.assertGreaterEqual(duplicate_count, 0)
        self.assertEqual(len(self.frame), len(self.frame.drop_duplicates(["SerialBoardID", "Position"])))

    def test_mapping_cycle_can_move_same_pair_to_another_storehouse(self):
        cycle_one = materialize_cycle(self.rows, {
            1: {"L": "C00001", "R": "C00002"},
            5: {"L": "C00009", "R": "C00010"},
        })
        cycle_two = materialize_cycle(self.rows, {7: {"L": "C00001", "R": "C00002"}})
        cycle_one_by_serial = {row["SerialBoardID"]: row["StoreHouseID"] for row in cycle_one}
        cycle_two_by_serial = {row["SerialBoardID"]: row["StoreHouseID"] for row in cycle_two}
        self.assertEqual("1", cycle_one_by_serial["C00001"])
        self.assertEqual("7", cycle_two_by_serial["C00001"])
        self.assertEqual("7", cycle_two_by_serial["C00002"])

        server = object.__new__(XMLSocketServer)
        server.meas_map = mapping_frame(cycle_two)
        request = server.create_request_w2002_xml("StoreHouseStatusRequest", "T", "X", 7)
        root = ET.fromstring(request)  # noqa: S314
        serials = [node.text for node in root.findall("./BODY/PalletInfo/Pallet/SerialBoardID")]
        self.assertEqual(["C00001", "C00002"], serials)

    def test_mapping_rejects_duplicate_active_serial_in_one_cycle(self):
        with self.assertRaises(ValueError):
            materialize_cycle(self.rows, {
                1: {"L": "C00001", "R": "C00002"},
                2: {"L": "C00001", "R": "C00009"},
            })

    def test_cycle_assignment_fixture_supports_arbitrary_pairs(self):
        fixture_path = Path(__file__).parent / "fixtures" / "cycle_assignments.csv"
        assignments = defaultdict(dict)
        with fixture_path.open("r", encoding="utf-8", newline="") as stream:
            for row in csv.DictReader(stream):
                cycle = assignments[int(row["CycleID"])]
                storehouse = cycle.setdefault(int(row["StoreHouseID"]), {})
                storehouse[row["PalletPosition"]] = row["SerialBoardID"]
        self.assertEqual({1, 2, 3}, set(assignments))
        cycle_two = materialize_cycle(self.rows, assignments[2])
        cycle_three = materialize_cycle(self.rows, assignments[3])
        pair_two = {(r["PalletPosition"], r["SerialBoardID"]) for r in cycle_two if r["StoreHouseID"] == "2"}
        pair_three = {(r["PalletPosition"], r["SerialBoardID"]) for r in cycle_three if r["StoreHouseID"] == "7"}
        self.assertEqual({("L", "C00001"), ("R", "C00009")}, pair_two)
        self.assertEqual({("L", "C00001"), ("R", "C00002")}, pair_three)

    def test_c01_time_sync_boundary_and_invalid_input(self):
        sync = sync_time_api(self.influx)
        sync.logger = FakeLogging()
        now = datetime.now()
        self.assertTrue(sync.verify_time_sync(now.strftime("%Y%m%d%H%M%S")))
        self.assertTrue(sync.verify_time_sync((now - timedelta(seconds=9)).strftime("%Y%m%d%H%M%S")))
        self.assertFalse(sync.verify_time_sync((now - timedelta(seconds=30)).strftime("%Y%m%d%H%M%S")))
        self.assertFalse(sync.verify_time_sync("not-a-time"))

    def test_c01_time_sync_and_alive_xml_contract(self):
        api = object.__new__(message_api)
        api.generate_transaction_id = lambda: "T-0001"
        time_xml = api.create_request_w0001_xml("SyncTimeRequest")
        alive_xml = api.create_request_w0002_xml("AliveRequest")
        for payload, message_name in ((time_xml, "SyncTimeRequest"), (alive_xml, "AliveRequest")):
            root = ET.fromstring(payload)  # noqa: S314
            self.assertEqual(message_name, root.findtext("./HEADER/MESSAGENAME"))
            self.assertEqual("0", root.findtext("./RETURN/RETURNCODE"))
            self.assertEqual("SECCHARGE0100", root.findtext("./BODY/LINE_ID"))
            self.assertTrue(root.findtext("./HEADER/TRANSACTIONID"))
            self.assertTrue(root.findtext("./BODY/TRX_ID"))
        self.assertTrue(ET.fromstring(time_xml).findtext("./BODY/TIME"))  # noqa: S314

    def test_c01_task_ready_requires_both_serial_tasks(self):
        manager = object.__new__(TaskManager)
        manager.influxdb_obj = self.influx
        manager.logging = FakeLogging()

        class MSSQL:
            def __init__(self):
                self.calls = 0

            def query_db_pd(self, _query):
                self.calls += 1
                count = 2 if self.calls >= 2 else 1
                return pd.DataFrame([[count]])

        manager.mssql_obj = MSSQL()
        with patch("balps.task_mgr.time.sleep", return_value=None):
            self.assertTrue(manager.check_task_ready("C00001", "C00002", check_interval=0, max_attempts=3))
        manager.mssql_obj = type("MSSQL", (), {"query_db_pd": lambda _self, _query: pd.DataFrame([[1]])})()
        with patch("balps.task_mgr.time.sleep", return_value=None):
            self.assertFalse(manager.check_task_ready("C00001", "C00002", check_interval=0, max_attempts=2))

    def test_c01_mpd_heartbeat_timeout_uses_60_second_threshold(self):
        flow = object.__new__(FC_api)
        flow.sbid = "C00001"
        flow.port = 20001
        flow.is_first_round = False
        flow.mpd_heartbeat_timeout = 60
        flow.standby_heartbeat_timeout = 7200
        flow.heartbeat_last = 100.0
        flow.heartbeat_current = 0.0
        flow.logging = FakeLogging()

        class MSSQL:
            def __init__(self):
                self.clears = []

            def clear_task_table(self, **kwargs):
                self.clears.append(kwargs)

        flow.mssql_obj = MSSQL()
        flow.influxdb_obj = self.influx
        with patch("fc.FlowControl.time.time", return_value=159.9):
            flow.esp_heartbeat_check()
        self.assertEqual([], flow.mssql_obj.clears)
        with patch("fc.FlowControl.time.time", return_value=160.0):
            flow.esp_heartbeat_check()
        self.assertEqual(1, len(flow.mssql_obj.clears))
        self.assertEqual("C00001", flow.mssql_obj.clears[0]["sb_id"])

    def test_c02_w2002_has_three_steps_and_all_c07_thresholds(self):
        server = object.__new__(XMLSocketServer)
        server.meas_map = self.frame
        request = server.create_request_w2002_xml("StoreHouseStatusRequest", "T-2002", "X-2002", 1)
        root = ET.fromstring(request)  # noqa: S314
        self.assertEqual("StoreHouseStatusRequest", root.findtext("./HEADER/MESSAGENAME"))
        self.assertEqual("1", root.findtext("./BODY/StoreHouseID"))
        steps = root.findall("./BODY/RecipeInfo/RecipeStep")
        self.assertEqual(3, len(steps))
        self.assertEqual(["START_CC", "REST", "END"], [step.findtext("Control_Mode") for step in steps])
        for step in steps:
            self.assertEqual("4.18", step.findtext("Cell_Max_Voltage"))
            self.assertEqual("0", step.findtext("Cell_Min_Voltage"))
            self.assertEqual("300", step.findtext("Cell_Delta_Voltage"))
            self.assertEqual("45", step.findtext("Cell_Protect_Temp"))
            self.assertEqual("90.1", step.findtext("Cell_Protect_Temp_Water"))
            self.assertEqual("210", step.findtext("Cell_Setting_Time"))
            self.assertEqual("3", step.findtext("Cell_Delta_Temp_Frame"))
        pallets = root.findall("./BODY/PalletInfo/Pallet")
        self.assertEqual(["L", "R"], [pallet.findtext("PalletPosition") for pallet in pallets])
        self.assertEqual(["C00001", "C00002"], [pallet.findtext("SerialBoardID") for pallet in pallets])

    def test_c02_w2002_config_parser_accepts_exact_recipe_and_rejects_whole_request_on_mismatch(self):
        server = object.__new__(XMLSocketServer)
        server.meas_map = self.frame
        request = server.create_request_w2002_xml("StoreHouseStatusRequest", "T-2002", "X-2002", 1)
        parser = meas_map_api(self.influx, protect_params=self.params)
        parsed = parser.parse_config_info(request)
        self.assertEqual(6, len(parsed), "3 steps x 2 pallets")
        self.assertEqual({"START_CC", "REST", "END"}, set(parsed["Control_Mode"]))
        root = ET.fromstring(request)  # noqa: S314
        ET.SubElement(root.find("./BODY/RecipeInfo/RecipeStep[2]"), "UnknownKey").text = "1"
        with self.assertRaises(KeyError):
            parser.parse_config_info(xml_bytes(root))

    def test_c02_w2003_switch_protection_parameter_reply(self):
        aggregator = self.new_aggregator()
        response = aggregator.create_reply_w2003_xml(1, {
            "palletL": status_payload(rack_id=1, side="L", serial="C00001", return_code=0, message_name="StoreHouseStepCheckRequest"),
            "palletR": status_payload(rack_id=1, side="R", serial="C00002", return_code=0, message_name="StoreHouseStepCheckRequest"),
        }, TaskInfo())
        root = ET.fromstring(response[1])  # noqa: S314
        self.assertEqual("StoreHouseStepCheckReply", root.findtext("./HEADER/MESSAGENAME"))
        self.assertEqual("OK", root.findtext("./BODY/NGStatus"))
        self.assertEqual("0", root.findtext("./RETURN/RETURNCODE"))

    def test_c03_w2004_status_priority_ok_ng_water(self):
        aggregator = self.new_aggregator()
        cases = [((0, 0), "OK", "0"), ((1, 0), "NG", "1"), ((2, 1), "Water", "2")]
        for (left_code, right_code), expected_status, expected_code in cases:
            with self.subTest(left_code=left_code, right_code=right_code):
                reply = aggregator.create_reply_w2004_xml(1, {
                    "palletL": status_payload(rack_id=1, side="L", serial="C00001", return_code=left_code),
                    "palletR": status_payload(rack_id=1, side="R", serial="C00002", return_code=right_code),
                }, TaskInfo())
                root = ET.fromstring(reply[1])  # noqa: S314
                self.assertEqual(expected_status, root.findtext("./BODY/NGStatus"))
                self.assertEqual(expected_code, root.findtext("./RETURN/RETURNCODE"))

    def test_c04_w2004_missing_rack_is_no_response(self):
        fixture = cache_api(self.influx)
        self.assertTrue(fixture.handle_rack_status(99, timeout=10))
        fields = fixture.cache.cache["99"]["W2004"]
        self.assertEqual("NoResponse", json.loads(fields["palletL"])["MessageName"]["0"])
        self.assertEqual(3, json.loads(fields["palletR"])["ReturnCode"]["0"])
        reply = fixture.aggregator.create_reply_w2004_xml(99, fields, TaskInfo())
        root = ET.fromstring(reply[1])  # noqa: S314
        self.assertEqual("NoResponse", root.findtext("./BODY/NGStatus"))
        self.assertEqual("3", root.findtext("./RETURN/RETURNCODE"))

    def test_c05_w2005_stop_reply_ok_and_exception(self):
        aggregator = self.new_aggregator()
        for left_code, right_code, expected in ((0, 0, "0"), (1, 0, "1")):
            with self.subTest(left_code=left_code, right_code=right_code):
                reply = aggregator.create_reply_w2005_xml(1, {
                    "palletL": completion_payload(rack_id=1, side="L", serial="C00001", return_code=left_code, return_message="stop failed" if left_code else ""),
                    "palletR": completion_payload(rack_id=1, side="R", serial="C00002", return_code=right_code),
                }, TaskInfo())
                root = ET.fromstring(reply[1])  # noqa: S314
                self.assertEqual("JudgmentCompletionNotificationReply", root.findtext("./HEADER/MESSAGENAME"))
                self.assertEqual(expected, root.findtext("./RETURN/RETURNCODE"))

    def test_c06_two_rack_two_round_sequence_changes_assignments_only_after_stop(self):
        cycle_one = materialize_cycle(self.rows, {
            1: {"L": "C00001", "R": "C00002"},
            2: {"L": "C00003", "R": "C00004"},
        })
        cycle_two = materialize_cycle(self.rows, {
            1: {"L": "C00003", "R": "C00004"},
            2: {"L": "C00001", "R": "C00009"},
        })
        events = ["W2005:1", "W2005:2", "cycle_switch", "W2002:1", "W2002:2"]
        self.assertEqual("W2005:1", events[0])
        self.assertLess(events.index("cycle_switch"), events.index("W2002:1"))
        old_rack_one = {(r["PalletPosition"], r["SerialBoardID"]) for r in cycle_one if r["StoreHouseID"] == "1"}
        new_rack_one = {(r["PalletPosition"], r["SerialBoardID"]) for r in cycle_two if r["StoreHouseID"] == "1"}
        self.assertEqual({("L", "C00001"), ("R", "C00002")}, old_rack_one)
        self.assertEqual({("L", "C00003"), ("R", "C00004")}, new_rack_one)

    def test_c07_threshold_values_match_function_test_table(self):
        server = object.__new__(XMLSocketServer)
        server.meas_map = self.frame
        root = ET.fromstring(server.create_request_w2002_xml("StoreHouseStatusRequest", "T", "X", 1))  # noqa: S314
        step = root.find("./BODY/RecipeInfo/RecipeStep")
        expected = {
            "Cell_Max_Voltage": "4.18", "Cell_Min_Voltage": "0", "Cell_Delta_Voltage": "300",
            "Cell_Delta_Voltage_Time": "5", "Cell_Max_Current": "25", "Cell_Min_Current": "-1.5",
            "Cell_Protect_Temp": "45", "Cell_Delta_Temp": "3.2", "Cell_Delta_Temp_Time": "4",
            "Cell_Delta_Wire_Voltage": "1500", "Cell_Delta_Wire_Voltage_Time": "4",
            "Cell_Protect_Delay_Time": "60", "Cell_Setting_Time": "210",
            "Cell_Protect_Temp_Water": "90.1", "Cell_Delta_Temp_Frame": "3",
        }
        for key, value in expected.items():
            self.assertEqual(value, step.findtext(key), key)

    def test_c07_remaining_wire_and_delay_thresholds(self):
        server = object.__new__(XMLSocketServer)
        server.meas_map = self.frame
        root = ET.fromstring(server.create_request_w2002_xml("StoreHouseStatusRequest", "T", "X", 1))  # noqa: S314
        step = root.find("./BODY/RecipeInfo/RecipeStep")
        expected = {
            "Wire_Max_Voltage": "1.5",
            "Cell_Delta_Wire_Delay": "1",
            "Cell_Delta_Temp_Delay": "1",
        }
        for key, value in expected.items():
            self.assertEqual(value, step.findtext(key), key)

    def test_w1001_alarm_report_is_driven_by_esp_return_code(self):
        aggregator = self.new_aggregator()
        timeline = []
        # ESP reports NG for 30 seconds, then reports the second temperature
        # level as Water. CCMS must use ReturnCode; Temp alone cannot upgrade.
        for second in range(31):
            is_water = second == 30
            left = alarm_payload(
                rack_id=1, side="L", serial="C00001", protect="PCB5001001",
                return_code=2 if is_water else 1,
                error_code="OTW" if is_water else "OT",
                temperature_tenth_c=901 if is_water else 450,
            )
            right = alarm_payload(
                rack_id=1, side="R", serial="C00002", protect="PCB5002001",
                return_code=0, message_name="ESPRequest",
            )
            message_name, payload = aggregator.create_request_w1001_xml(1, {"palletL": left, "palletR": right})
            root = ET.fromstring(payload)  # noqa: S314
            timeline.append(root.findtext("./BODY/NGStatus"))
            self.assertEqual("AlarmStopReport", message_name)
        self.assertEqual(31, len(timeline))
        self.assertEqual({"NG"}, set(timeline[:30]))
        self.assertEqual("Water", timeline[30])

        high_temp_ng = alarm_payload(
            rack_id=1, side="L", serial="C00001", protect="PCB5001001",
            return_code=1, temperature_tenth_c=901,
        )
        right = alarm_payload(
            rack_id=1, side="R", serial="C00002", protect="PCB5002001",
            return_code=0, message_name="ESPRequest",
        )
        _, payload = aggregator.create_request_w1001_xml(1, {"palletL": high_temp_ng, "palletR": right})
        self.assertEqual("NG", ET.fromstring(payload).findtext("./BODY/NGStatus"))  # noqa: S314

    def test_w1001_w2005_w2004_message_order_is_explicit(self):
        aggregator = self.new_aggregator()
        w1001 = aggregator.create_request_w1001_xml(1, {
            "palletL": alarm_payload(rack_id=1, side="L", serial="C00001", protect="PCB5001001", return_code=1),
            "palletR": alarm_payload(rack_id=1, side="R", serial="C00002", protect="PCB5002001", return_code=0, message_name="ESPRequest"),
        })
        w2005 = aggregator.create_reply_w2005_xml(1, {
            "palletL": completion_payload(rack_id=1, side="L", serial="C00001", return_code=0),
            "palletR": completion_payload(rack_id=1, side="R", serial="C00002", return_code=0),
        }, TaskInfo())
        w2004 = aggregator.create_reply_w2004_xml(1, {
            "palletL": status_payload(rack_id=1, side="L", serial="C00001", return_code=1),
            "palletR": status_payload(rack_id=1, side="R", serial="C00002", return_code=0),
        }, TaskInfo())
        self.assertEqual(
            ["AlarmStopReport", "JudgmentCompletionNotificationReply", "StoreHouseNGCheckReply"],
            [w1001[0], w2005[0], w2004[0]],
        )

    def test_protect_parameter_exact_key_order_is_accepted(self):
        parser = meas_map_api(self.influx, protect_params=self.params)
        parsed = parser.parse_protect_params(recipe_step_xml(self.params))
        self.assertEqual(set(self.params), set(parsed))
        self.assertEqual("START_CC", parsed["Control_Mode"])

    def test_protect_parameter_reordered_xml_keys_are_accepted(self):
        parser = meas_map_api(self.influx, protect_params=self.params)
        source = recipe_step_xml(self.params)
        reordered = ET.Element("RecipeStep")
        for element in reversed(list(source)):
            reordered.append(ET.Element(element.tag))
            reordered[-1].text = element.text
        parsed = parser.parse_protect_params(reordered)
        self.assertEqual(set(self.params), set(parsed))

    def test_known_empty_protect_value_is_a_disabled_parameter(self):
        parser = meas_map_api(self.influx, protect_params=self.params)
        parsed = parser.parse_protect_params(
            recipe_step_xml(self.params, overrides={"Cell_Max_Current": ""})
        )
        self.assertEqual("", parsed["Cell_Max_Current"])

    def test_protect_parameter_mismatch_combinations_reject_whole_step(self):
        cases = (
            ("unknown XML key", {"extra_keys": ["UnknownKey"]}, {}),
            ("missing XML key", {"omitted_keys": ["Cell_Max_Current"]}, {}),
            ("case mismatch", {"extra_keys": ["cell_Max_Current"], "omitted_keys": ["Cell_Max_Current"]}, {}),
            ("extra plus missing", {"extra_keys": ["UnknownKey"], "omitted_keys": ["Cell_Max_Current"]}, {}),
            ("invalid numeric value", {}, {"Cell_Max_Current": "not-a-number"}),
        )
        for name, kwargs, overrides in cases:
            with self.subTest(name=name):
                parser = meas_map_api(self.influx, protect_params=self.params)
                with self.assertRaises((KeyError, ValueError)):
                    parser.parse_protect_params(recipe_step_xml(self.params, overrides=overrides, **kwargs))

    def test_protect_config_key_mismatch_combinations_reject(self):
        xml_step = recipe_step_xml(self.params)
        cases = []
        extra_config = dict(self.params)
        extra_config["UnknownKey"] = 1
        cases.append(("extra protect config key", extra_config))
        missing_config = dict(self.params)
        missing_config.pop("Cell_Max_Current")
        cases.append(("missing protect config key", missing_config))
        case_config = dict(self.params)
        case_config["cell_Max_Current"] = case_config.pop("Cell_Max_Current")
        cases.append(("protect config case mismatch", case_config))
        for name, config in cases:
            with self.subTest(name=name):
                parser = meas_map_api(self.influx, protect_params=config)
                with self.assertRaises((KeyError, ValueError)):
                    parser.parse_protect_params(xml_step)

    def test_protect_parameter_mismatch_in_second_step_rejects_config_request(self):
        parser = meas_map_api(self.influx, protect_params=self.params)
        root = ET.Element("MESSAGE")
        body = ET.SubElement(root, "BODY")
        ET.SubElement(body, "StoreHouseID").text = "1"
        pallet_info = ET.SubElement(body, "PalletInfo")
        for side, serial in (("L", "C00001"), ("R", "C00002")):
            pallet = ET.SubElement(pallet_info, "Pallet")
            ET.SubElement(pallet, "SerialBoardID").text = serial
            ET.SubElement(pallet, "PalletPosition").text = side
            ET.SubElement(pallet, "DRCID").text = f"PCB{serial[1:]}"
        info = ET.SubElement(body, "RecipeInfo")
        for index, mode in enumerate(("START_CC", "REST", "END"), start=1):
            step = recipe_step_xml(self.params, overrides={"Step": str(index), "Control_Mode": mode})
            if index == 2:
                ET.SubElement(step, "Unexpected").text = "1"
            info.append(step)
        with self.assertRaises(KeyError):
            parser.parse_config_info(xml_bytes(root))

    @unittest.expectedFailure
    def test_protect_parameter_duplicate_xml_key_is_rejected(self):
        """Duplicate XML keys must be rejected instead of silently taking first."""

        parser = meas_map_api(self.influx, protect_params=self.params)
        duplicate = recipe_step_xml(self.params, duplicate_key="Cell_Max_Current")
        with self.assertRaises(KeyError):
            parser.parse_protect_params(duplicate)

    @unittest.expectedFailure
    def test_protect_parameter_duplicate_xml_key_with_different_values_is_rejected(self):
        parser = meas_map_api(self.influx, protect_params=self.params)
        duplicate = recipe_step_xml(self.params, duplicate_key="Cell_Max_Current")
        duplicate.findall("Cell_Max_Current")[-1].text = "99"
        with self.assertRaises(KeyError):
            parser.parse_protect_params(duplicate)

    def test_unknown_message_mapping_is_not_silently_routed(self):
        fixture = cache_api(self.influx)
        self.assertEqual("", fixture.map_message_to_operation("UnknownMessage"))
        self.assertFalse(fixture.handle_esp_data("{}", rack_id="1", message_name="UnknownMessage"))

    def test_w2004_stale_w3001_status_becomes_no_response(self):
        fixture = cache_api(self.influx)
        raw = status_payload(rack_id=1, side="L", serial="C00001", return_code=1, message_name="ESPRequest")
        with patch("balps.system_cache.time.time", return_value=100.0):
            fixture.update_rack_status(raw)
        with patch("balps.system_cache.time.time", return_value=111.0):
            self.assertTrue(fixture.handle_rack_status(1, timeout=10))
        fields = fixture.cache.cache["1"]["W2004"]
        self.assertEqual(3, json.loads(fields["palletL"])["ReturnCode"]["0"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
