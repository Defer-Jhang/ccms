"""InfluxDB read/write benchmark for large and small existing datasets.

The benchmark deliberately keeps the application write path out of the test:
it writes test batches directly to InfluxDB, while the production application
can continue using ``StoreHouseMeasData``.  It measures four independent cases:

* ``large``: query historical data written by ``influx_stress_writer.py`` and
  write new current-time points to the same target
* ``small``: query the smaller dataset written by
  ``influx_stress_writer_month.py`` and write new current-time points to its
  target

The default ``production`` schema and generator use the same ServerMap order,
QRCode cycle, deterministic sensor values, field order, numeric formatting,
and tag/field layout as ``influx_stress_writer.py``.  Timestamps are generated
for the benchmark run and therefore do not need to match the historical
writer.  Use ``--schema legacy --unique-qrcode`` only when deliberately
testing high-cardinality tag pressure.

Use ``--dry-run`` first.  Tokens are read from ``INFLUXDB_TOKEN`` unless a
token file is supplied; no token is stored in this source file.

For long-range read tests, ``--query-shape stream`` avoids the expensive
record pivot.  Stress readers use sequential 10-second windows by default
(``--stress-query-chunk-seconds 0`` restores one query over the entire range).
With ``--stress-query-mode random``, the explicit query start/stop values are
the full data bounds and each worker chooses a random query-duration window.
Use ``--query-count-only`` when measuring query/HTTP throughput without
charging the result loop for Python-side JSON encoding.
"""

from __future__ import annotations

import argparse
import csv
import contextlib
import gzip
import json
import math
import multiprocessing
import os
import queue
import random
import re
import statistics
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from influxdb_client import InfluxDBClient
from influxdb_client.domain.bucket_retention_rules import BucketRetentionRules


DEFAULT_SERVERMAP = Path(__file__).resolve().parents[1] / "db" / "ServerMap.csv"
SECONDS_PER_CYCLE = 3 * 60 * 60
NS_PER_SECOND = 1_000_000_000
TAIPEI_UTC_OFFSET_SECONDS = 8 * 60 * 60
DEFAULT_DATA_SEED = 20250818
VOLTAGE_STEPS = tuple(value / 1000.0 for value in range(10, 31))
TEMPERATURE_STEPS = (0.03, 0.04, 0.05)
DEFAULT_TAG_COLUMNS = (
    "storehouse_id",
    "pallet_position",
    "position_id",
    "return_code",
)
LEGACY_TAG_COLUMNS = (
    "storehouse_id",
    "pallet_position",
    "serialboard_id",
    "protectboard_id",
    "position_id",
    "qrcode",
    "return_code",
)
SYSTEM_COLUMNS = {
    "result",
    "table",
    "_start",
    "_stop",
    "_time",
    "_measurement",
    "_field",
    "_value",
}
UTC = timezone.utc


@dataclass(frozen=True)
class Device:
    storehouse_id: str
    pallet_position: str
    serialboard_id: str
    protectboard_id: str
    position_id: str
    qrcode: str
    pallet_number: int = 0


@dataclass(frozen=True)
class Target:
    name: str
    bucket: str
    measurement: str
    query_start: datetime | None = None
    query_stop: datetime | None = None


@dataclass(frozen=True)
class Config:
    url: str
    token: str
    org: str
    large: Target
    small: Target
    devices: tuple[Device, ...]
    query_start: datetime
    query_stop: datetime
    query_duration_hours: float
    query_points: int
    write_points: int
    write_batch_size: int
    write_runs: int
    query_runs: int
    warmup_runs: int
    timeout: float
    retries: int
    gzip_body: bool
    fields: tuple[str, ...]
    query_fields: tuple[str, ...]
    tags: tuple[str, ...]
    schema: str
    return_code: str
    unique_qrcode: bool
    data_seed: int
    qrcode_mode: str
    qrcode_prefix: str
    qrcode_width: int
    dry_run: bool
    # ``wide`` preserves the original record-shaped query (pivot).  ``stream``
    # keeps the result in the native Flux row shape and applies the limit
    # before any pivot, which puts a hard bound on query memory.
    query_shape: str = "wide"
    # JSON encoding is useful when estimating decoded payload size, but it is
    # not part of InfluxDB query throughput.  Count-only mode avoids charging
    # the benchmark for that Python-side work.
    query_count_only: bool = False


@dataclass
class StressStats:
    write_points: int = 0
    write_requests: int = 0
    write_bytes: int = 0
    write_errors: int = 0
    query_rows: int = 0
    query_requests: int = 0
    query_requests_started: int = 0
    query_bytes: int = 0
    query_errors: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def add(self, **values: int) -> None:
        with self.lock:
            for key, value in values.items():
                setattr(self, key, getattr(self, key) + value)


def escape_key(value: object) -> str:
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace(" ", "\\ ")
        .replace(",", "\\,")
        .replace("=", "\\=")
    )


def escape_measurement(value: object) -> str:
    return str(value).replace("\\", "\\\\").replace(" ", "\\ ").replace(",", "\\,")


def escape_string(value: object) -> str:
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\r", "\\r")
        .replace("\n", "\\n")
    )


def escape_field_string(value: object) -> str:
    """Escape a string field exactly as influx_stress_writer does."""

    return str(value).replace("\\", "\\\\").replace('"', '\\"')


def escape_tag(value: object) -> str:
    return escape_key(value)


def load_servermap(path: Path) -> tuple[Device, ...]:
    required = {
        "StoreHouseID",
        "PalletID",
        "PalletPosition",
        "SerialBoardID",
        "ProtectBoardID",
        "Position",
        "QRCODEID",
    }
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise ValueError(f"ServerMap contains no rows: {path}")
    missing = required.difference(rows[0])
    if missing:
        raise ValueError(f"ServerMap missing columns: {', '.join(sorted(missing))}")
    try:
        rows.sort(
            key=lambda row: (
                int(row["StoreHouseID"]),
                {"L": 0, "R": 1}.get(row["PalletPosition"], 2),
                int(row["Position"]),
            )
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Failed to sort ServerMap: {exc}") from exc

    pallet_numbers: dict[tuple[str, ...], int] = {}
    identities: set[tuple[str, str, str]] = set()
    devices: list[Device] = []
    for row in rows:
        identity = (row["StoreHouseID"], row["PalletPosition"], row["Position"])
        if identity in identities:
            raise ValueError(
                "Duplicate ServerMap topology: "
                f"StoreHouseID={identity[0]}, PalletPosition={identity[1]}, "
                f"Position={identity[2]}"
            )
        identities.add(identity)
        pallet_key = (
            row["StoreHouseID"],
            row["PalletID"],
            row["PalletPosition"],
            row["SerialBoardID"],
            row["ProtectBoardID"],
        )
        pallet_number = pallet_numbers.setdefault(pallet_key, len(pallet_numbers))
        devices.append(
            Device(
                storehouse_id=row["StoreHouseID"],
                pallet_position=row["PalletPosition"],
                serialboard_id=row["SerialBoardID"],
                protectboard_id=row["ProtectBoardID"],
                position_id=row["Position"],
                qrcode=row["QRCODEID"],
                pallet_number=pallet_number,
            )
        )
    return tuple(devices)


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def format_time(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def field_value(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return f"{value}i"
    if isinstance(value, float):
        return repr(value) if math.isfinite(value) else "0.0"
    return f'"{escape_string(value)}"'


def deterministic_int(low: int, high: int, seed: int, *parts: int) -> int:
    """Match influx_stress_writer's deterministic integer generator."""

    value = seed
    for part in parts:
        value = (
            value * 6364136223846793005 + part * 1442695040888963407
        ) % (2**64)
    return low + value % (high - low + 1)


def cumulative_steps(steps: tuple[float, ...], updates: int, offset: int) -> float:
    """Match influx_stress_writer's cyclic accumulated-step generator."""

    cycles, remainder = divmod(updates, len(steps))
    return cycles * sum(steps) + sum(
        steps[(offset + index) % len(steps)] for index in range(remainder)
    )


def esp_values(
    second_index: int,
    pallet_number: int,
    timestamp_start_ns: int,
    seed: int,
) -> tuple[float, float, float, tuple[int, ...]]:
    """Generate the same sensor values as influx_stress_writer."""

    timestamp_seconds = timestamp_start_ns // NS_PER_SECOND + second_index
    cycle_number, second_in_cycle = divmod(
        timestamp_seconds + TAIPEI_UTC_OFFSET_SECONDS,
        SECONDS_PER_CYCLE,
    )
    updates = second_in_cycle // 3
    voltage_offset = deterministic_int(
        0, len(VOLTAGE_STEPS) - 1, seed, cycle_number, pallet_number, 1
    )
    temperature_offset = deterministic_int(
        0, len(TEMPERATURE_STEPS) - 1, seed, cycle_number, pallet_number, 2
    )
    voltage = min(
        3.1 + cumulative_steps(VOLTAGE_STEPS, updates, voltage_offset),
        4.085,
    )
    temperature = min(
        23.0 + cumulative_steps(TEMPERATURE_STEPS, updates, temperature_offset),
        24.5,
    )
    current = deterministic_int(
        171,
        174,
        seed,
        cycle_number,
        second_in_cycle,
        pallet_number,
        3,
    ) / 10.0
    wire_voltage = tuple(
        deterministic_int(
            100,
            300,
            seed,
            cycle_number,
            second_in_cycle,
            pallet_number,
            channel,
        )
        for channel in range(5)
    )
    return (
        int(voltage * 1000) / 1000.0,
        (int(temperature * 10 + 2731) - 2731) / 10.0,
        current,
        wire_voltage,
    )


def absolute_cycle(timestamp_seconds: int) -> int:
    """Return the Taipei-local three-hour cycle used for QRCode generation."""

    return (timestamp_seconds + TAIPEI_UTC_OFFSET_SECONDS) // SECONDS_PER_CYCLE


def cycle_qrcode(
    absolute_cycle_number: int,
    device_index: int,
    devices: tuple[Device, ...],
    *,
    mode: str,
    prefix: str,
    width: int,
    history_start_cycle: int,
) -> str:
    """Match influx_stress_writer's cycle/servermap QRCode rules."""

    device = devices[device_index]
    if mode == "servermap":
        return device.qrcode
    cycle_index = absolute_cycle_number - history_start_cycle
    qrcode_number = cycle_index * len(devices) + device_index + 1
    return f"{prefix}{qrcode_number:0{width}d}"


def make_line(
    device: Device,
    measurement: str,
    timestamp_ns: int,
    sequence: int,
    *,
    schema: str = "production",
    return_code: str = "OK",
    unique_qrcode: bool = False,
    qrcode: str | None = None,
    second_index: int = 0,
    timestamp_start_ns: int | None = None,
    data_seed: int = DEFAULT_DATA_SEED,
    sensor_values: tuple[float, float, float, tuple[int, ...]] | None = None,
) -> str:
    """Create one ESP-style line-protocol point.

    ``production`` matches influx_stress_writer's line protocol: only
    low-cardinality topology values are tags and device identities/QRCode are
    fields.  ``legacy`` preserves the old benchmark layout for an intentional
    high-cardinality test.
    """
    if schema not in {"production", "legacy"}:
        raise ValueError(f"unknown benchmark schema: {schema}")

    qrcode = qrcode or device.qrcode
    if unique_qrcode:
        qrcode = f"{qrcode}-P{sequence % 100000:05d}"

    if schema == "legacy":
        tags = [
            f"storehouse_id={escape_tag(device.storehouse_id)}",
            f"pallet_position={escape_tag(device.pallet_position)}",
            f"serialboard_id={escape_tag(device.serialboard_id)}",
            f"protectboard_id={escape_tag(device.protectboard_id)}",
            f"position_id={escape_tag(device.position_id)}",
            f"qrcode={escape_tag(qrcode)}",
            f"return_code={escape_tag(return_code)}",
        ]
        identity_fields: list[str] = []
    else:
        tags = [
            f"storehouse_id={escape_tag(device.storehouse_id)}",
            f"pallet_position={escape_tag(device.pallet_position)}",
            f"position_id={escape_tag(device.position_id)}",
            f"return_code={escape_tag(return_code)}",
        ]
        identity_fields = [
            f'qrcode="{escape_field_string(qrcode)}"',
            f'serialboard_id="{escape_field_string(device.serialboard_id)}"',
            f'protectboard_id="{escape_field_string(device.protectboard_id)}"',
        ]

    if timestamp_start_ns is None:
        timestamp_start_ns = timestamp_ns
    if sensor_values is None:
        sensor_values = esp_values(
            second_index,
            device.pallet_number,
            timestamp_start_ns,
            data_seed,
        )
    voltage, temperature, current, wire_voltage = sensor_values

    # Keep the exact field order, numeric precision, and integer suffix used
    # by influx_stress_writer.make_batch().
    fields = [
        f"voltage={voltage:.3f}",
        f"temperature={temperature:.1f}",
        f"current={current:.1f}",
        'error_code="OK"',
        'wire_voltage_status="OK"',
        "fetstate=1i",
        "fuse=0i",
        'afe="0x12"',
        "wifistdisconn=0i",
        "wifiltdisconn=0i",
        "socketstdisconn=0i",
        "socketltdisconn=0i",
        "heartbeattxcount=10i",
        "heartbeatlosscount=0i",
        "heartbeatlastrttms=32i",
        "heartbeattrttmaxms=40i",
        "heatbeattimeoutflag=0i",
        "wifireconnlastms=0i",
        "wifireconnavgms=0i",
        "wifireconntimes=0i",
        "socketreconnlastms=0i",
        "socketreconnavgms=0i",
        "socketreconnmaxms=0i",
        "socketreconntimes=0i",
        'error_code_curr="OK"',
        f"wire_voltage_1={wire_voltage[0]}i",
        f"wire_voltage_2={wire_voltage[1]}i",
        f"wire_voltage_3={wire_voltage[2]}i",
        f"wire_voltage_4={wire_voltage[3]}i",
        f"wire_voltage_5={wire_voltage[4]}i",
    ]
    all_fields = identity_fields + fields
    return f"{escape_measurement(measurement)},{','.join(tags)} {','.join(all_fields)} {timestamp_ns}"


def generate_body(
    target: Target,
    devices: tuple[Device, ...],
    count: int,
    base_ns: int,
    *,
    sequence_start: int = 0,
    schema: str = "production",
    return_code: str = "OK",
    unique_qrcode: bool = False,
    data_seed: int = DEFAULT_DATA_SEED,
    qrcode_mode: str = "cycle",
    qrcode_prefix: str = "",
    qrcode_width: int = 1,
    qrcode_start_ns: int | None = None,
    timestamp_sequence_start: int | None = None,
) -> bytes:
    if not devices:
        raise ValueError("ServerMap contains no devices")
    if qrcode_mode not in {"cycle", "servermap"}:
        raise ValueError(f"unknown qrcode mode: {qrcode_mode}")
    if qrcode_width <= 0:
        raise ValueError("qrcode_width must be greater than zero")

    device_count = len(devices)
    qrcode_start_ns = base_ns if qrcode_start_ns is None else qrcode_start_ns
    history_start_cycle = absolute_cycle(qrcode_start_ns // NS_PER_SECOND)
    if timestamp_sequence_start is None:
        timestamp_sequence_start = sequence_start
    cached_value_key: tuple[int, int] | None = None
    cached_sensor_values: tuple[float, float, float, tuple[int, ...]] | None = None
    cached_qrcode_cycle: int | None = None
    cached_qrcodes: tuple[str, ...] | None = None
    lines: list[str] = []
    for offset in range(count):
        point_index = sequence_start + offset
        second_index, device_index = divmod(point_index, device_count)
        device = devices[device_index]
        timestamp_point = timestamp_sequence_start + offset
        timestamp_second_index = timestamp_point // device_count
        timestamp_ns = base_ns + timestamp_second_index * NS_PER_SECOND
        point_cycle = absolute_cycle(
            base_ns // NS_PER_SECOND + second_index
        )
        value_key = (second_index, device.pallet_number)
        if value_key != cached_value_key:
            cached_sensor_values = esp_values(
                second_index,
                device.pallet_number,
                base_ns,
                data_seed,
            )
            cached_value_key = value_key
        if point_cycle != cached_qrcode_cycle:
            cached_qrcodes = tuple(
                cycle_qrcode(
                    point_cycle,
                    index,
                    devices,
                    mode=qrcode_mode,
                    prefix=qrcode_prefix,
                    width=qrcode_width,
                    history_start_cycle=history_start_cycle,
                )
                for index in range(device_count)
            )
            cached_qrcode_cycle = point_cycle
        if cached_qrcodes is None:
            raise RuntimeError("QRCode cache was not initialized")
        qrcode = cached_qrcodes[device_index]
        lines.append(
            make_line(
                device,
                target.measurement,
                timestamp_ns,
                point_index,
                schema=schema,
                return_code=return_code,
                unique_qrcode=unique_qrcode,
                qrcode=qrcode,
                second_index=second_index,
                timestamp_start_ns=base_ns,
                data_seed=data_seed,
                sensor_values=cached_sensor_values,
            )
        )
    return ("\n".join(lines) + "\n").encode("utf-8")


def write_body(target: Target, body: bytes, config: Config) -> int:
    payload = gzip.compress(body, compresslevel=1) if config.gzip_body else body
    params = urllib.parse.urlencode(
        {"org": config.org, "bucket": target.bucket, "precision": "ns"}
    )
    endpoint = f"{config.url.rstrip('/')}/api/v2/write?{params}"
    headers = {
        "Authorization": f"Token {config.token}",
        "Content-Type": "text/plain; charset=utf-8",
        "Accept": "application/json",
    }
    if config.gzip_body:
        headers["Content-Encoding"] = "gzip"
    for attempt in range(config.retries + 1):
        request = urllib.request.Request(endpoint, data=payload, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=config.timeout) as response:
                if response.status != 204:
                    raise RuntimeError(f"unexpected HTTP {response.status}")
            return len(payload)
        except urllib.error.HTTPError as exc:
            detail = exc.read(500).decode("utf-8", errors="replace")
            if exc.code not in (429,) and exc.code < 500:
                raise RuntimeError(f"InfluxDB HTTP {exc.code}: {detail}") from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            if attempt == config.retries:
                raise RuntimeError(f"InfluxDB write failed: {exc}") from exc
        if attempt == config.retries:
            raise RuntimeError("InfluxDB write retries exhausted")
        time.sleep(min(30.0, 0.5 * (2**attempt)))
    raise AssertionError("unreachable")


def target_query_range(config: Config, target: Target) -> tuple[datetime, datetime]:
    """Return the explicit query bounds configured for one dataset."""

    query_start = target.query_start or config.query_start
    query_stop = target.query_stop or config.query_stop
    return query_start, query_stop


def resolve_query_range(
    config: Config,
    rolling: bool = False,
    target: Target | None = None,
) -> tuple[datetime, datetime]:
    """Return the range a query worker should use.

    A fixed stress query uses the explicit bounds attached to the selected
    target.  Rolling mode intentionally ignores those historical bounds.
    """
    if rolling:
        query_stop = datetime.now(UTC)
        return query_stop - timedelta(hours=config.query_duration_hours), query_stop
    if target is None:
        return config.query_start, config.query_stop
    return target_query_range(config, target)


def random_query_range(
    config: Config,
    generator: random.Random,
    target: Target | None = None,
) -> tuple[datetime, datetime]:
    """Choose one random query-duration window inside the configured bounds."""
    duration = timedelta(hours=config.query_duration_hours)
    range_start, range_stop = (
        (config.query_start, config.query_stop)
        if target is None
        else target_query_range(config, target)
    )
    available_seconds = int((range_stop - range_start - duration).total_seconds())
    if available_seconds < 0:
        raise ValueError(
            "query-duration-hours is longer than the configured random query range"
        )
    offset_seconds = generator.randint(0, available_seconds)
    query_start = range_start + timedelta(seconds=offset_seconds)
    return query_start, query_start + duration


def query_text(
    target: Target,
    config: Config,
    query_api: Any,
    rolling: bool = False,
    query_start: datetime | None = None,
    query_stop: datetime | None = None,
    progress_callback: Any = None,
    progress_interval: int = 10_000,
) -> tuple[int, int]:
    if query_start is None or query_stop is None:
        query_start, query_stop = resolve_query_range(
            config,
            rolling=rolling,
            target=target,
        )
    query = build_query(target, config, query_start, query_stop)
    rows = 0
    bytes_read = 0
    reported_rows = 0
    reported_bytes = 0
    try:
        for record in query_api.query_stream(query=query, org=config.org):
            rows += 1
            if not config.query_count_only:
                bytes_read += len(json.dumps(record.values, default=str, ensure_ascii=False))
            if (
                progress_callback is not None
                and progress_interval > 0
                and rows - reported_rows >= progress_interval
            ):
                progress_callback(rows - reported_rows, bytes_read - reported_bytes)
                reported_rows = rows
                reported_bytes = bytes_read
    finally:
        # Flush a partial final batch as well, including a partial response if
        # the HTTP/Flux request raises after returning some records.
        if progress_callback is not None and rows > reported_rows:
            progress_callback(rows - reported_rows, bytes_read - reported_bytes)
    return rows, bytes_read


def build_query(target: Target, config: Config, query_start: datetime, query_stop: datetime) -> str:
    stages = [
        f"from(bucket: {json.dumps(target.bucket)})",
        (
            "|> range(start: time(v: "
            f"{json.dumps(format_time(query_start))}), stop: time(v: "
            f"{json.dumps(format_time(query_stop))}))"
        ),
        f"|> filter(fn: (r) => r._measurement == {json.dumps(target.measurement)})",
    ]
    if config.query_fields:
        predicates = " or ".join(
            f"r._field == {json.dumps(field)}" for field in config.query_fields
        )
        stages.append(f"|> filter(fn: (r) => {predicates})")

    if config.query_shape == "stream":
        # Keep Flux in its native row shape.  Group by _field instead of using
        # group(columns: []): a query containing all fields can have float,
        # integer and string _value types, which cannot share one Flux table
        # (InfluxDB reports "schema collision" at limit in that case).
        # With one selected field this is still a single global limit.  With
        # multiple fields, the limit applies once per field, which is both
        # type-safe and bounded by the time chunk used by stress readers.
        stages.extend(
            [
                '|> group(columns: ["_field"])',
                f"|> limit(n: {config.query_points})",
            ]
        )
    elif config.query_shape == "wide":
        # This is the original record-shaped query.  It is useful when the
        # consumer needs all fields in one row, but pivot can materialise the
        # complete selected range.  Use a small time chunk for long scans.
        stages.extend(
            [
                '|> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")',
                "|> group(columns: [])",
                f"|> limit(n: {config.query_points})",
            ]
        )
    else:
        raise ValueError(f"unknown query shape: {config.query_shape}")
    return "\n".join(stages)


def stress_write_worker(
    target: Target,
    config: Config,
    worker_id: int,
    duration_seconds: int,
    progress_queue: Any = None,
) -> dict[str, int]:
    stats = {"write_points": 0, "write_requests": 0, "write_bytes": 0, "write_errors": 0}
    deadline = time.monotonic() + duration_seconds
    sequence = worker_id * 1_000_000_000
    while time.monotonic() < deadline:
        try:
            # Keep line timestamps close to "now" for rolling queries.  The
            # point sequence remains worker-specific so the generated sensor
            # values and device ordering are deterministic and collision-free.
            base_ns = time.time_ns() + worker_id * NS_PER_SECOND
            transferred = 0
            request_count = 0
            for offset in range(0, config.write_points, config.write_batch_size):
                count = min(config.write_batch_size, config.write_points - offset)
                chunk = generate_body(
                    target,
                    config.devices,
                    count,
                    base_ns,
                    sequence_start=sequence + offset,
                    schema=config.schema,
                    return_code=config.return_code,
                    unique_qrcode=config.unique_qrcode,
                    data_seed=config.data_seed,
                    qrcode_mode=config.qrcode_mode,
                    qrcode_prefix=config.qrcode_prefix,
                    qrcode_width=config.qrcode_width,
                    qrcode_start_ns=base_ns,
                    timestamp_sequence_start=offset,
                )
                transferred += write_body(target, chunk, config)
                request_count += 1
            stats["write_points"] += config.write_points
            stats["write_requests"] += request_count
            stats["write_bytes"] += transferred
            if progress_queue is not None:
                progress_queue.put(
                    {
                        "write_points": config.write_points,
                        "write_requests": request_count,
                        "write_bytes": transferred,
                        "write_errors": 0,
                    }
                )
            sequence += config.write_points
        except Exception:
            stats["write_errors"] += 1
            if progress_queue is not None:
                progress_queue.put(
                    {
                        "write_points": 0,
                        "write_requests": 0,
                        "write_bytes": 0,
                        "write_errors": 1,
                    }
                )
            time.sleep(0.2)
    return stats


def stress_query_worker(
    target: Target,
    config: Config,
    deadline: float,
    stats: StressStats,
    query_timeout_seconds: float,
    rolling: bool,
    chunk_seconds: int = 0,
    random_mode: bool = False,
    random_seed: int = 0,
    worker_id: int = 0,
) -> None:
    client = InfluxDBClient(
        url=config.url,
        token=config.token,
        org=config.org,
        timeout=int(query_timeout_seconds * 1000),
        enable_gzip=True,
    )
    try:
        query_api = client.query_api()
        reported_error = False
        reported_empty = False
        reported_random_window = False
        cursor: datetime | None = None
        random_window: tuple[datetime, datetime] | None = None
        seed = random_seed if random_seed else time.time_ns()
        generator = random.Random(seed + worker_id)

        def report_query_progress(rows: int, bytes_read: int) -> None:
            stats.add(query_rows=rows, query_bytes=bytes_read)

        while time.monotonic() < deadline:
            try:
                if random_mode:
                    if (
                        random_window is None
                        or cursor is None
                        or cursor >= random_window[1]
                    ):
                        random_window = random_query_range(config, generator, target)
                        cursor = random_window[0]
                        if not reported_random_window:
                            print(
                                f"  query worker {worker_id} random window: "
                                f"{format_time(random_window[0])}.."
                                f"{format_time(random_window[1])}",
                                flush=True,
                            )
                            reported_random_window = True
                    range_start, range_stop = random_window
                else:
                    range_start, range_stop = resolve_query_range(
                        config,
                        rolling=rolling,
                        target=target,
                    )

                if chunk_seconds > 0:
                    # Walk the requested range from left to right.  Once the
                    # end is reached, start again so a sustained stress run
                    # continues to issue bounded, sequential range queries.
                    if (
                        cursor is None
                        or cursor < range_start
                        or cursor >= range_stop
                    ):
                        cursor = range_start
                    chunk_start = cursor
                    chunk_stop = min(
                        chunk_start + timedelta(seconds=chunk_seconds),
                        range_stop,
                    )
                    if chunk_stop <= chunk_start:
                        cursor = range_start
                        continue
                    query_start, query_stop = chunk_start, chunk_stop
                    cursor = chunk_stop
                else:
                    query_start, query_stop = range_start, range_stop
                    if random_mode:
                        # A complete random window is one request.  Select a
                        # new window on the next loop iteration.
                        cursor = range_stop

                stats.add(query_requests_started=1)
                rows, _bytes_read = query_text(
                    target,
                    config,
                    query_api,
                    rolling=False,
                    query_start=query_start,
                    query_stop=query_stop,
                    progress_callback=report_query_progress,
                )
                stats.add(query_requests=1)
                if rows == 0 and not reported_empty:
                    print(
                        f"  query worker returned 0 rows ({target.name}): "
                        f"range={format_time(query_start)}..{format_time(query_stop)}, "
                        f"measurement={target.measurement}, "
                        f"fields={','.join(config.query_fields) if config.query_fields else 'all'}",
                        file=sys.stderr,
                        flush=True,
                    )
                    reported_empty = True
            except Exception as exc:
                stats.add(query_errors=1)
                # Keep the stress loop running, but expose the first concrete
                # failure.  A counter alone cannot distinguish an empty range
                # from a server-side timeout or Flux/schema error.
                if not reported_error:
                    print(
                        f"  query worker error ({target.name}): "
                        f"{type(exc).__name__}: {exc}",
                        file=sys.stderr,
                        flush=True,
                    )
                    reported_error = True
                time.sleep(0.2)
    finally:
        client.close()


def benchmark_stress(target: Target, config: Config, args: argparse.Namespace) -> None:
    """Run sustained concurrent write/query load for one target."""
    import concurrent.futures

    run_writers = args.stress_operation in {"write", "both"} and args.stress_write_workers > 0
    run_queries = args.stress_operation in {"query", "both"} and args.stress_query_workers > 0
    if not run_writers and not run_queries:
        raise ValueError("stress operation has no active workers")
    effective_operation = (
        "both" if run_writers and run_queries else "write" if run_writers else "query"
    )
    query_start, query_stop = target_query_range(config, target)

    print(
        f"\nSTRESS {target.name}: operation={effective_operation}, "
        f"duration={args.stress_duration_seconds}s, "
        f"writers={args.stress_write_workers if run_writers else 0}, "
        f"readers={args.stress_query_workers if run_queries else 0}, "
        f"query_mode={args.stress_query_mode}, "
        f"query_shape={config.query_shape}, "
        f"query_chunk={args.stress_query_chunk_seconds:g}s, "
        f"query_seed={args.stress_query_random_seed}, "
        f"query_range={format_time(query_start)}..{format_time(query_stop)}, "
        f"write_points/worker={config.write_points:,}, query_limit={config.query_points:,}"
    )
    stats = StressStats()
    deadline = time.monotonic() + args.stress_duration_seconds
    started = time.perf_counter()
    query_context = (
        concurrent.futures.ThreadPoolExecutor(max_workers=args.stress_query_workers)
        if run_queries
        else contextlib.nullcontext(None)
    )
    write_context = (
        concurrent.futures.ProcessPoolExecutor(max_workers=args.stress_write_workers)
        if run_writers
        else contextlib.nullcontext(None)
    )
    progress_manager = multiprocessing.Manager() if run_writers else None
    progress_queue = progress_manager.Queue() if progress_manager is not None else None
    live_write_points = 0
    live_write_requests = 0
    live_write_errors = 0

    def drain_write_progress() -> None:
        nonlocal live_write_points, live_write_requests, live_write_errors
        if progress_queue is None:
            return
        while True:
            try:
                message = progress_queue.get_nowait()
            except queue.Empty:
                return
            live_write_points += int(message.get("write_points", 0))
            live_write_requests += int(message.get("write_requests", 0))
            live_write_errors += int(message.get("write_errors", 0))

    write_futures: list[concurrent.futures.Future[dict[str, int]]] = []
    completed_write_futures: set[concurrent.futures.Future[dict[str, int]]] = set()
    query_futures: list[concurrent.futures.Future[Any]] = []
    with query_context as query_executor, write_context as write_executor:
        if write_executor is not None:
            write_futures = [
                write_executor.submit(
                    stress_write_worker,
                    target,
                    config,
                    worker,
                    args.stress_duration_seconds,
                    progress_queue,
                )
                for worker in range(args.stress_write_workers)
            ]
        if query_executor is not None:
            query_futures = [
                query_executor.submit(
                    stress_query_worker,
                    target,
                    config,
                    deadline,
                    stats,
                    args.stress_query_timeout,
                    args.stress_query_mode == "rolling",
                    args.stress_query_chunk_seconds,
                    args.stress_query_mode == "random",
                    args.stress_query_random_seed,
                    worker,
                )
                for worker in range(args.stress_query_workers)
            ]
        while time.monotonic() < deadline:
            time.sleep(min(5.0, max(0.0, deadline - time.monotonic())))
            drain_write_progress()
            elapsed = max(min(time.perf_counter() - started, args.stress_duration_seconds), 1e-9)
            # A process-pool worker returns one aggregate result at the end of
            # its run.  Collect an early result if one exists; otherwise the
            # periodic write counter is intentionally still pending.
            for future in write_futures:
                if future.done() and future not in completed_write_futures:
                    stats.add(**future.result())
                    completed_write_futures.add(future)
            with stats.lock:
                query_started = stats.query_requests_started
                query_completed = stats.query_requests
                print(
                    f"  elapsed={elapsed:.0f}s "
                    f"write={live_write_points / elapsed:,.0f} points/s "
                    f"({live_write_points:,} points, {live_write_requests:,} requests) "
                    f"query={stats.query_rows / elapsed:,.0f} rows/s "
                    f"({stats.query_rows:,} rows, "
                    f"{query_completed:,} completed/{query_started:,} started requests) "
                    f"write_errors={live_write_errors} query_errors={stats.query_errors}"
                )
        for future in write_futures:
            if future not in completed_write_futures:
                stats.add(**future.result())
                completed_write_futures.add(future)
        for future in query_futures:
            future.result()
    drain_write_progress()
    if progress_manager is not None:
        progress_manager.shutdown()
    wall_elapsed = max(time.perf_counter() - started, 1e-9)
    load_elapsed = max(float(args.stress_duration_seconds), 1e-9)
    tail_wait = max(0.0, wall_elapsed - load_elapsed)
    with stats.lock:
        print(
            f"{target.name} stress summary: elapsed={wall_elapsed:.2f}s, "
            f"load_elapsed={load_elapsed:.2f}s, tail_wait={tail_wait:.2f}s, "
            f"write_points={stats.write_points:,}, write_requests={stats.write_requests:,}, "
            f"write_rate={stats.write_points / load_elapsed:,.0f} points/s, "
            f"write_wall_rate={stats.write_points / wall_elapsed:,.0f} points/s, "
            f"query_rows={stats.query_rows:,}, query_requests={stats.query_requests:,}, "
            f"query_requests_started={stats.query_requests_started:,}, "
            f"query_rate={stats.query_rows / load_elapsed:,.0f} rows/s, "
            f"query_wall_rate={stats.query_rows / wall_elapsed:,.0f} rows/s, "
            f"write_errors={stats.write_errors}, query_errors={stats.query_errors}"
        )


def summarize(name: str, seconds: list[float], points: int, bytes_count: int = 0) -> None:
    ordered = sorted(seconds)
    p50 = statistics.median(ordered)
    p95 = ordered[min(len(ordered) - 1, math.ceil(len(ordered) * 0.95) - 1)]
    mean = statistics.mean(ordered)
    rate = points / mean if mean else 0.0
    suffix = f", decoded {bytes_count / 1_048_576:.2f} MiB" if bytes_count else ""
    print(
        f"{name}: runs={len(seconds)}, points/run={points:,}, "
        f"mean={mean:.4f}s, p50={p50:.4f}s, p95={p95:.4f}s, "
        f"throughput={rate:,.0f} points/s{suffix}"
    )


def benchmark_write(target: Target, config: Config) -> None:
    print(f"\nWRITE {target.name}: {target.bucket}/{target.measurement}")
    body = generate_body(
        target,
        config.devices,
        config.write_points,
        time.time_ns(),
        schema=config.schema,
        return_code=config.return_code,
        unique_qrcode=config.unique_qrcode,
        data_seed=config.data_seed,
        qrcode_mode=config.qrcode_mode,
        qrcode_prefix=config.qrcode_prefix,
        qrcode_width=config.qrcode_width,
    )
    print(
        f"payload: {config.write_points:,} points, "
        f"{len(body) / 1_048_576:.2f} MiB line protocol, "
        f"batch={config.write_batch_size:,}"
    )
    if config.dry_run:
        print(body.splitlines()[0].decode("utf-8")[:500])
        return
    # Warmup is intentionally not included in measured runs.
    for _ in range(config.warmup_runs):
        write_body(target, body, config)
    durations: list[float] = []
    payload_bytes = 0
    for run in range(config.write_runs):
        run_body = generate_body(
            target,
            config.devices,
            config.write_points,
            time.time_ns() + run * 10_000_000,
            sequence_start=run * config.write_points,
            schema=config.schema,
            return_code=config.return_code,
            unique_qrcode=config.unique_qrcode,
            data_seed=config.data_seed,
            qrcode_mode=config.qrcode_mode,
            qrcode_prefix=config.qrcode_prefix,
            qrcode_width=config.qrcode_width,
        )
        started = time.perf_counter()
        # Split the request to exercise the same batch behaviour as production.
        lines = run_body.splitlines()
        for offset in range(0, len(lines), config.write_batch_size):
            chunk = b"\n".join(lines[offset : offset + config.write_batch_size]) + b"\n"
            payload_bytes += write_body(target, chunk, config)
        durations.append(time.perf_counter() - started)
        print(f"  run {run + 1}/{config.write_runs}: {durations[-1]:.4f}s")
    summarize(target.name + " write", durations, config.write_points, payload_bytes // max(1, config.write_runs))


def benchmark_query(target: Target, config: Config, query_api: Any) -> None:
    print(f"\nQUERY {target.name}: {target.bucket}/{target.measurement}")
    query_start, query_stop = target_query_range(config, target)
    print(
        f"range: {format_time(query_start)} <= time < "
        f"{format_time(query_stop)}"
    )
    if config.dry_run:
        query = build_query(target, config, query_start, query_stop)
        print(query)
        return
    for _ in range(config.warmup_runs):
        query_text(target, config, query_api)
    durations: list[float] = []
    actual_rows = 0
    actual_bytes = 0
    for run in range(config.query_runs):
        started = time.perf_counter()
        rows, bytes_read = query_text(
            target,
            config,
            query_api,
            query_start=query_start,
            query_stop=query_stop,
        )
        durations.append(time.perf_counter() - started)
        actual_rows = rows
        actual_bytes = bytes_read
        print(f"  run {run + 1}/{config.query_runs}: {durations[-1]:.4f}s, rows={rows:,}")
    summarize(target.name + " query", durations, actual_rows, actual_bytes)


def ensure_bucket(client: InfluxDBClient, target: Target, org: str, retention_hours: int) -> None:
    buckets_api = client.buckets_api()
    bucket = buckets_api.find_bucket_by_name(target.bucket)
    if bucket is not None:
        return
    retention_rules = None
    if retention_hours > 0:
        retention_rules = BucketRetentionRules(
            type="expire", every_seconds=retention_hours * 3600
        )
    buckets_api.create_bucket(
        bucket_name=target.bucket,
        org=org,
        retention_rules=retention_rules,
    )
    print(f"created new bucket: {target.bucket} (retention={retention_hours}h or infinite)")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark InfluxDB read/write performance")
    parser.add_argument("--url", default=os.getenv("INFLUXDB_URL", "http://localhost:8086"))
    parser.add_argument("--token-file", type=Path)
    parser.add_argument("--org", default=os.getenv("INFLUXDB_ORG", "delta"))
    parser.add_argument(
        "--large-bucket",
        "--existing-bucket",
        dest="large_bucket",
        default="StoreHouseMeasData",
        help="bucket containing influx_stress_writer.py's large dataset",
    )
    parser.add_argument(
        "--large-measurement",
        "--existing-measurement",
        dest="large_measurement",
        default="meas_data",
        help="measurement containing the large dataset",
    )
    parser.add_argument(
        "--small-bucket",
        "--new-bucket",
        dest="small_bucket",
        default="StoreHouseMeasData",
        help="bucket containing influx_stress_writer_month.py's small dataset",
    )
    parser.add_argument(
        "--small-measurement",
        "--new-measurement",
        dest="small_measurement",
        default="meas_data",
        help="measurement containing the small dataset",
    )
    parser.add_argument(
        "--new-bucket-retention-hours",
        type=int,
        default=0,
        help="deprecated compatibility option; used only with --create-missing-buckets",
    )
    parser.add_argument(
        "--create-missing-buckets",
        action="store_true",
        help="create a target bucket if it does not exist (disabled by default)",
    )
    parser.add_argument("--servermap", type=Path, default=DEFAULT_SERVERMAP)
    parser.add_argument(
        "--schema",
        choices=("production", "legacy"),
        default="production",
        help=(
            "line-protocol schema: production matches "
            "influx_stress_writer; legacy keeps the old all-identity-tags layout"
        ),
    )
    parser.add_argument(
        "--return-code",
        default="OK",
        help="return_code tag value used by generated points (default: OK)",
    )
    parser.add_argument(
        "--data-seed",
        "--seed",
        dest="data_seed",
        type=int,
        default=DEFAULT_DATA_SEED,
        help=(
            "deterministic sensor seed; use the same value as "
            "influx_stress_writer (default: 20250818)"
        ),
    )
    parser.add_argument(
        "--qrcode-mode",
        choices=("cycle", "servermap"),
        default="cycle",
        help=(
            "QRCode generation mode; cycle matches influx_stress_writer's "
            "default, servermap keeps the CSV QRCode"
        ),
    )
    parser.add_argument(
        "--qrcode-width",
        type=int,
        default=None,
        help="numeric QRCode suffix width; default uses the first ServerMap QRCode",
    )
    parser.add_argument(
        "--unique-qrcode",
        action="store_true",
        help=(
            "append a sequence suffix to every QR code; this intentionally "
            "deviates from influx_stress_writer and increases cardinality"
        ),
    )
    parser.add_argument(
        "--large-query-start",
        "--query-start",
        dest="large_query_start",
        type=parse_time,
        help="inclusive range start for the large dataset",
    )
    parser.add_argument(
        "--large-query-stop",
        "--query-stop",
        dest="large_query_stop",
        type=parse_time,
        help="exclusive range stop for the large dataset",
    )
    parser.add_argument(
        "--small-query-start",
        type=parse_time,
        help="inclusive range start for the small dataset; defaults to large range",
    )
    parser.add_argument(
        "--small-query-stop",
        type=parse_time,
        help="exclusive range stop for the small dataset; defaults to large range",
    )
    parser.add_argument("--query-duration-hours", type=float, default=1.0)
    parser.add_argument("--query-points", type=int, default=10_000)
    parser.add_argument("--query-fields", default="", help="comma-separated fields; empty means all fields")
    parser.add_argument(
        "--query-shape",
        choices=("wide", "stream"),
        default="wide",
        help=(
            "wide returns pivoted records (more expensive); stream keeps "
            "native Flux rows and limits before pivot/materialisation"
        ),
    )
    parser.add_argument(
        "--query-count-only",
        action="store_true",
        help="count streamed rows without JSON-serializing each record in Python",
    )
    parser.add_argument("--write-points", type=int, default=10_000)
    parser.add_argument("--write-batch-size", type=int, default=5_000)
    parser.add_argument("--write-runs", type=int, default=5)
    parser.add_argument("--query-runs", type=int, default=5)
    parser.add_argument("--warmup-runs", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--no-gzip", action="store_true")
    parser.add_argument(
        "--seed-small-points",
        "--seed-new-points",
        dest="seed_small_points",
        type=int,
        default=0,
        help="write this many points to the small target before querying",
    )
    parser.add_argument("--stress-duration-seconds", type=int, default=0, help="sustained concurrent load duration; 0 disables stress mode")
    parser.add_argument("--stress-write-workers", type=int, default=8)
    parser.add_argument("--stress-query-workers", type=int, default=4)
    parser.add_argument(
        "--stress-operation",
        choices=("write", "query", "both"),
        default="both",
        help=(
            "stress operation to run: write-only, query-only, or both; "
            "workers for the other operation are ignored"
        ),
    )
    parser.add_argument(
        "--stress-query-mode",
        choices=("fixed", "rolling", "random"),
        default="fixed",
        help=(
            "query range for stress readers: fixed uses the target's explicit "
            "large/small query bounds; rolling queries the most recent "
            "query-duration-hours; random selects a random window inside "
            "those bounds"
        ),
    )
    parser.add_argument(
        "--stress-query-random-seed",
        type=int,
        default=0,
        help="random query seed; 0 uses a time-based seed",
    )
    parser.add_argument(
        "--stress-query-chunk-seconds",
        type=int,
        default=10,
        help=(
            "split each stress query into sequential time windows (default: "
            "10s); 0 keeps the whole fixed/rolling range in one query"
        ),
    )
    parser.add_argument("--stress-query-timeout", type=float, default=30.0)
    parser.add_argument(
        "--stress-target",
        choices=("large", "small", "both", "existing", "new"),
        default="both",
        help="dataset to stress; existing=large and new=small are compatibility aliases",
    )
    parser.add_argument("--stress-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--skip-large-write",
        "--skip-existing-write",
        dest="skip_large_write",
        action="store_true",
    )
    parser.add_argument(
        "--skip-small-write",
        "--skip-new-write",
        dest="skip_small_write",
        action="store_true",
    )
    parser.add_argument(
        "--skip-large-query",
        "--skip-existing-query",
        dest="skip_large_query",
        action="store_true",
    )
    parser.add_argument(
        "--skip-small-query",
        "--skip-new-query",
        dest="skip_small_query",
        action="store_true",
    )
    return parser.parse_args(argv)


def read_token(args: argparse.Namespace) -> str:
    token = os.getenv("INFLUXDB_TOKEN", "").strip()
    if args.token_file:
        token = args.token_file.read_text(encoding="utf-8-sig").strip()
    if not token and not args.dry_run:
        raise SystemExit("set INFLUXDB_TOKEN or use --token-file")
    return token or "dry-run"


def validate_args(args: argparse.Namespace) -> None:
    for name in (
        "query_points",
        "write_points",
        "write_batch_size",
        "write_runs",
        "query_runs",
    ):
        if getattr(args, name) <= 0:
            raise SystemExit(f"--{name.replace('_', '-')} must be greater than zero")
    if args.warmup_runs < 0 or args.retries < 0:
        raise SystemExit("--warmup-runs and --retries must not be negative")
    if args.query_duration_hours <= 0:
        raise SystemExit("--query-duration-hours must be greater than zero")
    if args.stress_query_chunk_seconds < 0:
        raise SystemExit("--stress-query-chunk-seconds must not be negative")
    if args.seed_small_points < 0:
        raise SystemExit("--seed-small-points must not be negative")
    if args.new_bucket_retention_hours < 0:
        raise SystemExit("--new-bucket-retention-hours must not be negative")
    if args.stress_duration_seconds < 0:
        raise SystemExit("--stress-duration-seconds must not be negative")
    if args.stress_write_workers < 0 or args.stress_query_workers < 0:
        raise SystemExit("stress workers cannot be negative")
    if args.stress_operation == "write" and args.stress_write_workers <= 0:
        raise SystemExit("--stress-operation write requires --stress-write-workers greater than zero")
    if args.stress_operation == "query" and args.stress_query_workers <= 0:
        raise SystemExit("--stress-operation query requires --stress-query-workers greater than zero")
    if args.stress_operation == "both" and args.stress_write_workers <= 0 and args.stress_query_workers <= 0:
        raise SystemExit("--stress-operation both requires at least one writer or query worker")
    if args.stress_query_timeout <= 0:
        raise SystemExit("--stress-query-timeout must be greater than zero")
    if args.qrcode_width is not None and args.qrcode_width <= 0:
        raise SystemExit("--qrcode-width must be greater than zero")
    if args.stress_query_mode == "random":
        large_bounds = args.large_query_start is not None and args.large_query_stop is not None
        small_bounds = args.small_query_start is not None and args.small_query_stop is not None
        selected = {"existing": "large", "new": "small"}.get(
            args.stress_target,
            args.stress_target,
        )
        if selected in {"large", "both"} and not large_bounds:
            raise SystemExit(
                "--stress-query-mode random requires both large query bounds"
            )
        if selected in {"small", "both"} and not (small_bounds or large_bounds):
            raise SystemExit(
                "--stress-query-mode random requires small bounds or shared large bounds"
            )
    if args.stress_only and args.stress_duration_seconds <= 0:
        raise SystemExit("--stress-only requires --stress-duration-seconds")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    validate_args(args)
    token = read_token(args)

    # The large and small datasets can live in different buckets and have
    # independent historical query ranges.  If only the large range is
    # supplied, use it for the small dataset as a convenient default.
    large_query_stop = args.large_query_stop or datetime.now(UTC)
    large_query_start = args.large_query_start or (
        large_query_stop - timedelta(hours=args.query_duration_hours)
    )
    small_query_stop = args.small_query_stop or large_query_stop
    small_query_start = args.small_query_start or (
        small_query_stop - timedelta(hours=args.query_duration_hours)
        if args.small_query_stop is not None
        else large_query_start
    )
    for label, query_start, query_stop in (
        ("large", large_query_start, large_query_stop),
        ("small", small_query_start, small_query_stop),
    ):
        if query_start >= query_stop:
            raise SystemExit(f"{label} query start must be earlier than query stop")

    devices = load_servermap(args.servermap)
    qrcode_match = re.fullmatch(r"(.*?)(\d+)", devices[0].qrcode)
    if args.qrcode_mode == "cycle" and qrcode_match is None:
        raise SystemExit(
            "the first ServerMap QRCODEID must end with a numeric suffix "
            "when --qrcode-mode cycle is used"
        )
    qrcode_prefix = qrcode_match.group(1) if qrcode_match else devices[0].qrcode
    qrcode_width = (
        args.qrcode_width
        if args.qrcode_width is not None
        else (len(qrcode_match.group(2)) if qrcode_match else 1)
    )
    query_fields = tuple(field.strip() for field in args.query_fields.split(",") if field.strip())
    tags = DEFAULT_TAG_COLUMNS
    fields = (
        "voltage",
        "temperature",
        "current",
        "error_code",
        "wire_voltage_status",
        "fetstate",
        "fuse",
        "afe",
    )
    large_target = Target(
        "large",
        args.large_bucket,
        args.large_measurement,
        large_query_start.astimezone(UTC),
        large_query_stop.astimezone(UTC),
    )
    small_target = Target(
        "small",
        args.small_bucket,
        args.small_measurement,
        small_query_start.astimezone(UTC),
        small_query_stop.astimezone(UTC),
    )
    config = Config(
        url=args.url,
        token=token,
        org=args.org,
        large=large_target,
        small=small_target,
        devices=devices,
        query_start=large_query_start.astimezone(UTC),
        query_stop=large_query_stop.astimezone(UTC),
        query_duration_hours=args.query_duration_hours,
        query_points=args.query_points,
        write_points=args.write_points,
        write_batch_size=args.write_batch_size,
        write_runs=args.write_runs,
        query_runs=args.query_runs,
        warmup_runs=args.warmup_runs,
        timeout=args.timeout,
        retries=args.retries,
        gzip_body=not args.no_gzip,
        fields=fields,
        query_fields=query_fields,
        tags=LEGACY_TAG_COLUMNS if args.schema == "legacy" else DEFAULT_TAG_COLUMNS,
        schema=args.schema,
        return_code=args.return_code,
        unique_qrcode=args.unique_qrcode,
        data_seed=args.data_seed,
        qrcode_mode=args.qrcode_mode,
        qrcode_prefix=qrcode_prefix,
        qrcode_width=qrcode_width,
        dry_run=args.dry_run,
        query_shape=args.query_shape,
        query_count_only=args.query_count_only,
    )
    print(f"InfluxDB: {config.url} | org={config.org} | ServerMap devices={len(devices):,}")
    print(
        f"Data generator: seed={config.data_seed}, "
        f"qrcode_mode={config.qrcode_mode}, schema={config.schema}"
    )
    for target in (config.large, config.small):
        target_start, target_stop = target_query_range(config, target)
        print(
            f"{target.name} target: {target.bucket}/{target.measurement} | "
            f"query range: {format_time(target_start)} <= time < "
            f"{format_time(target_stop)}"
        )
    print(
        f"Query limit: {config.query_points:,} rows | shape={config.query_shape} | "
        f"count_only={config.query_count_only} | "
        f"write: {config.write_points:,} points/run"
    )
    for target in (config.large, config.small):
        target_start, target_stop = target_query_range(config, target)
        requested_hours = (target_stop - target_start).total_seconds() / 3600.0
        if args.stress_query_mode == "random" and (
            config.query_duration_hours > requested_hours
        ):
            raise SystemExit(
                f"--query-duration-hours must not exceed the {target.name} "
                "random query range"
            )
        if (
            args.stress_duration_seconds > 0
            and args.stress_operation in {"query", "both"}
            and args.stress_query_chunk_seconds == 0
            and config.query_shape == "wide"
            and requested_hours > 1
        ):
            print(
                f"WARNING: {target.name} wide long-range stress queries pivot "
                f"the complete {requested_hours:.2f}h range before limit; "
                "this can use GBs of InfluxDB RAM. Set "
                "--stress-query-chunk-seconds (1-10) or --query-shape stream."
            )
    if config.dry_run:
        for target in (config.large, config.small):
            benchmark_write(target, config)
            benchmark_query(target, config, None)
        return 0

    client = InfluxDBClient(
        url=config.url,
        token=config.token,
        org=config.org,
        timeout=int(config.timeout * 1000),
        enable_gzip=True,
    )
    try:
        query_api = client.query_api()
        if args.create_missing_buckets:
            for target in (config.large, config.small):
                ensure_bucket(
                    client,
                    target,
                    config.org,
                    args.new_bucket_retention_hours,
                )
        if args.stress_duration_seconds > 0:
            stress_aliases = {"existing": "large", "new": "small"}
            stress_name = stress_aliases.get(args.stress_target, args.stress_target)
            target_by_name = {
                "large": config.large,
                "small": config.small,
            }
            stress_targets = {
                "large": (target_by_name["large"],),
                "small": (target_by_name["small"],),
                "both": (target_by_name["large"], target_by_name["small"]),
            }[stress_name]
            for target in stress_targets:
                benchmark_stress(target, config, args)
            if args.stress_only:
                return 0
        if args.seed_small_points:
            seed_config = Config(
                **{
                    **config.__dict__,
                    "write_points": args.seed_small_points,
                    "write_runs": 1,
                    "warmup_runs": 0,
                }
            )
            print(
                f"\nSeeding small target with "
                f"{args.seed_small_points:,} points before query tests"
            )
            benchmark_write(config.small, seed_config)
        if not args.skip_large_write:
            benchmark_write(config.large, config)
        if not args.skip_small_write:
            benchmark_write(config.small, config)
        if not args.skip_large_query:
            benchmark_query(config.large, config, query_api)
        if not args.skip_small_query:
            benchmark_query(config.small, config, query_api)
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
