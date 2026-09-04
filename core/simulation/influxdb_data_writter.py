"""Standalone high-volume writer for the CCMS ``meas_data`` measurement.

This intentionally uses InfluxDB line protocol instead of pandas/Point objects so
the amount of memory used is bounded by ``batch_size * workers``.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import os
import random
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


LARGE_RUN = 1_000_000_000
SECONDS_PER_CYCLE = 3 * 60 * 60
NS_PER_SECOND = 1_000_000_000
TAIPEI_UTC_OFFSET_SECONDS = 8 * 60 * 60
VOLTAGE_STEPS = tuple(value / 1000 for value in range(10, 31))
TEMPERATURE_STEPS = (0.03, 0.04, 0.05)
DEFAULT_SERVERMAP = Path(__file__).resolve().parents[1] / "db" / "ServerMap.csv"


@dataclass(frozen=True)
class DeviceMapping:
    storehouse_id: str
    pallet_position: str
    serialboard_id: str
    protectboard_id: str
    position_id: str
    qrcode: str
    pallet_number: int
    tag_prefix: str


@dataclass(frozen=True)
class Config:
    url: str
    token: str
    org: str
    bucket: str
    measurement: str
    timeout: float
    retries: int
    gzip_body: bool
    timestamp_start_ns: int
    seed: int
    devices: tuple[DeviceMapping, ...]
    qrcode_mode: str
    qrcode_prefix: str
    qrcode_width: int
    history_start_cycle: int


def escape_tag(value: object) -> str:
    return str(value).replace("\\", "\\\\").replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=")


def load_servermap(path: Path) -> tuple[DeviceMapping, ...]:
    required = {
        "StoreHouseID", "PalletID", "SerialBoardID", "ProtectBoardID",
        "PalletPosition", "Position", "QRCODEID",
    }
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            missing = required.difference(reader.fieldnames or ())
            if missing:
                raise SystemExit(f"ServerMap is missing columns: {', '.join(sorted(missing))}")
            rows = list(reader)
    except OSError as exc:
        raise SystemExit(f"cannot read ServerMap {path}: {exc}") from exc
    rows.sort(
        key=lambda row: (
            int(row["StoreHouseID"]),
            {"L": 0, "R": 1}.get(row["PalletPosition"], 2),
            int(row["Position"]),
        )
    )
    pallet_numbers: dict[tuple[str, ...], int] = {}
    devices: list[DeviceMapping] = []
    identities: set[tuple[str, str, str]] = set()
    for row in rows:
        identity = (row["StoreHouseID"], row["PalletPosition"], row["Position"])
        if identity in identities:
            raise SystemExit(f"duplicate ServerMap rack/pallet/position: {row}")
        identities.add(identity)
        pallet_key = (
            row["StoreHouseID"], row["PalletID"], row["PalletPosition"],
            row["SerialBoardID"], row["ProtectBoardID"],
        )
        pallet_number = pallet_numbers.setdefault(pallet_key, len(pallet_numbers))
        tag_prefix = (
            f"storehouse_id={escape_tag(row['StoreHouseID'])},"
            f"pallet_position={escape_tag(row['PalletPosition'])},"
            f"serialboard_id={escape_tag(row['SerialBoardID'])},"
            f"protectboard_id={escape_tag(row['ProtectBoardID'])},"
            f"position_id={escape_tag(row['Position'])},qrcode="
        )
        devices.append(DeviceMapping(
            row["StoreHouseID"], row["PalletPosition"], row["SerialBoardID"],
            row["ProtectBoardID"], row["Position"], row["QRCODEID"], pallet_number,
            tag_prefix,
        ))
    if not devices:
        raise SystemExit(f"ServerMap contains no data rows: {path}")
    return tuple(devices)


def deterministic_int(low: int, high: int, seed: int, *parts: int) -> int:
    """Return a stable pseudo-random integer, independent of worker ordering."""
    value = seed
    for part in parts:
        value = (value * 6364136223846793005 + part * 1442695040888963407) % (2**64)
    return low + value % (high - low + 1)


def cumulative_steps(steps: tuple[float, ...], updates: int, offset: int) -> float:
    cycles, remainder = divmod(updates, len(steps))
    return cycles * sum(steps) + sum(steps[(offset + index) % len(steps)] for index in range(remainder))


def esp_values(second_index: int, pallet_number: int, cfg: Config) -> tuple[float, float, float, tuple[int, ...]]:
    """Generate Step 1 values following core/simulation/ESPDevice.py."""
    timestamp_seconds = cfg.timestamp_start_ns // NS_PER_SECOND + second_index
    cycle_number, second_in_cycle = divmod(
        timestamp_seconds + TAIPEI_UTC_OFFSET_SECONDS,
        SECONDS_PER_CYCLE,
    )
    updates = second_in_cycle // 3
    voltage_offset = deterministic_int(0, len(VOLTAGE_STEPS) - 1, cfg.seed, cycle_number, pallet_number, 1)
    temp_offset = deterministic_int(0, len(TEMPERATURE_STEPS) - 1, cfg.seed, cycle_number, pallet_number, 2)
    voltage = min(3.1 + cumulative_steps(VOLTAGE_STEPS, updates, voltage_offset), 4.085)
    temperature = min(23.0 + cumulative_steps(TEMPERATURE_STEPS, updates, temp_offset), 24.5)
    current = deterministic_int(171, 174, cfg.seed, cycle_number, second_in_cycle, pallet_number, 3) / 10
    wire_voltage = tuple(
        deterministic_int(100, 300, cfg.seed, cycle_number, second_in_cycle, pallet_number, channel)
        for channel in range(5)
    )
    return int(voltage * 1000) / 1000, (int(temperature * 10 + 2731) - 2731) / 10, current, wire_voltage


def make_batch(start: int, count: int, cfg: Config) -> bytes:
    """Generate chronological ESP-style points with one timestamp per second."""
    lines: list[str] = []
    append = lines.append
    cached_key: tuple[int, int] | None = None
    cached_fields = ""
    cached_tag_cycle: int | None = None
    cycle_tags: tuple[str, ...] | None = None
    servermap_tags = None
    if cfg.qrcode_mode == "servermap":
        servermap_tags = tuple(
            f"{device.tag_prefix}{escape_tag(device.qrcode)},return_code=OK"
            for device in cfg.devices
        )
    for index in range(start, start + count):
        second_index, device_index = divmod(index, len(cfg.devices))
        device = cfg.devices[device_index]
        value_key = (second_index, device.pallet_number)
        if value_key != cached_key:
            voltage, temperature, current, wire_voltage = esp_values(
                second_index, device.pallet_number, cfg
            )
            cached_fields = (
                f"voltage={voltage:.3f},temperature={temperature:.1f},current={current:.1f},"
                "error_code=\"OK\",wire_voltage_status=\"OK\",fetstate=1i,fuse=0i,afe=\"0x12\","
                "wifistdisconn=0i,wifiltdisconn=0i,socketstdisconn=0i,socketltdisconn=0i,"
                "heartbeattxcount=10i,heartbeatlosscount=0i,heartbeatlastrttms=32i,"
                "heartbeattrttmaxms=40i,heatbeattimeoutflag=0i,wifireconnlastms=0i,"
                "wifireconnavgms=0i,wifireconntimes=0i,socketreconnlastms=0i,"
                "socketreconnavgms=0i,socketreconnmaxms=0i,socketreconntimes=0i,"
                f"error_code_curr=\"OK\",wire_voltage_1={wire_voltage[0]}i,"
                f"wire_voltage_2={wire_voltage[1]}i,wire_voltage_3={wire_voltage[2]}i,"
                f"wire_voltage_4={wire_voltage[3]}i,wire_voltage_5={wire_voltage[4]}i"
            )
            cached_key = value_key
        if cfg.qrcode_mode == "cycle":
            timestamp_seconds = cfg.timestamp_start_ns // NS_PER_SECOND + second_index
            absolute_cycle = (timestamp_seconds + TAIPEI_UTC_OFFSET_SECONDS) // SECONDS_PER_CYCLE
            if absolute_cycle != cached_tag_cycle:
                cycle_index = absolute_cycle - cfg.history_start_cycle
                first_qrcode = cycle_index * len(cfg.devices) + 1
                escaped_prefix = escape_tag(cfg.qrcode_prefix)
                cycle_tags = tuple(
                    f"{mapped.tag_prefix}{escaped_prefix}"
                    f"{first_qrcode + offset:0{cfg.qrcode_width}d},return_code=OK"
                    for offset, mapped in enumerate(cfg.devices)
                )
                cached_tag_cycle = absolute_cycle
            assert cycle_tags is not None
            tags = cycle_tags[device_index]
        else:
            assert servermap_tags is not None
            tags = servermap_tags[device_index]
        timestamp_ns = cfg.timestamp_start_ns + second_index * NS_PER_SECOND
        append(f"{escape_tag(cfg.measurement)},{tags} {cached_fields} {timestamp_ns}")
    return ("\n".join(lines) + "\n").encode("utf-8")


def write_batch(start: int, count: int, cfg: Config) -> tuple[int, int]:
    body = make_batch(start, count, cfg)
    if cfg.gzip_body:
        body = gzip.compress(body, compresslevel=1)
    query = urllib.parse.urlencode({"org": cfg.org, "bucket": cfg.bucket, "precision": "ns"})
    endpoint = f"{cfg.url.rstrip('/')}/api/v2/write?{query}"
    headers = {
        "Authorization": f"Token {cfg.token}",
        "Content-Type": "text/plain; charset=utf-8",
        "Accept": "application/json",
    }
    if cfg.gzip_body:
        headers["Content-Encoding"] = "gzip"

    for attempt in range(cfg.retries + 1):
        request = urllib.request.Request(endpoint, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=cfg.timeout) as response:
                if response.status != 204:
                    raise RuntimeError(f"unexpected HTTP status {response.status}")
            return count, len(body)
        except urllib.error.HTTPError as exc:
            retryable = exc.code == 429 or exc.code >= 500
            detail = exc.read(500).decode("utf-8", errors="replace")
            if not retryable or attempt == cfg.retries:
                raise RuntimeError(f"InfluxDB HTTP {exc.code}: {detail}") from exc
        except (urllib.error.URLError, TimeoutError) as exc:
            if attempt == cfg.retries:
                raise RuntimeError(f"InfluxDB write failed: {exc}") from exc
        time.sleep(min(30.0, 0.5 * (2**attempt)) + random.random() * 0.25)
    raise AssertionError("retry loop exited unexpectedly")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write synthetic CCMS meas_data to InfluxDB 2.x")
    parser.add_argument("--count", type=int, default=None, help="limit points; default writes the complete range")
    parser.add_argument("--start-index", type=int, default=0, help="resume point index")
    parser.add_argument("--years", type=int, default=2, help="calendar years before end time")
    parser.add_argument("--start-time", help="ISO-8601 history start; overrides --years")
    parser.add_argument("--end-time", help="ISO-8601 history end; default is now")
    parser.add_argument("--batch-size", type=int, default=10_000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--worker-mode", choices=("process", "thread"), default="process")
    parser.add_argument("--url", default=os.getenv("INFLUXDB_URL", "http://localhost:8086"))
    parser.add_argument("--token", default=os.getenv("INFLUXDB_TOKEN"))
    parser.add_argument("--org", default=os.getenv("INFLUXDB_ORG", "delta"))
    parser.add_argument("--bucket", default=os.getenv("INFLUXDB_BUCKET", "StoreHouseMeasData"))
    parser.add_argument("--measurement", default="meas_data")
    parser.add_argument("--servermap", type=Path, default=DEFAULT_SERVERMAP)
    parser.add_argument("--qrcode-mode", choices=("cycle", "servermap"), default="cycle")
    parser.add_argument("--qrcode-width", type=int, help="minimum numeric suffix width; default follows ServerMap")
    parser.add_argument("--seed", type=int, default=20250818)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--retries", type=int, default=5)
    parser.add_argument("--no-gzip", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="generate one batch but do not connect")
    parser.add_argument("--confirm-large-run", action="store_true", help="required for one billion or more points")
    return parser.parse_args()


def validate(args: argparse.Namespace, count: int, available_points: int) -> None:
    for name in ("batch_size", "workers", "years"):
        if getattr(args, name) <= 0:
            raise SystemExit(f"--{name.replace('_', '-')} must be greater than zero")
    if count <= 0:
        raise SystemExit("--count must be greater than zero")
    if args.qrcode_width is not None and args.qrcode_width <= 0:
        raise SystemExit("--qrcode-width must be greater than zero")
    if args.start_index < 0:
        raise SystemExit("--start-index must not be negative")
    if args.start_index + count > available_points:
        raise SystemExit("requested points exceed --end-time")
    if count >= LARGE_RUN and not args.confirm_large_run and not args.dry_run:
        raise SystemExit("one-billion-point runs require --confirm-large-run")
    if not args.dry_run and not args.token:
        raise SystemExit("set INFLUXDB_TOKEN or pass --token")


def parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(microsecond=0)


def years_before(value: datetime, years: int) -> datetime:
    try:
        return value.replace(year=value.year - years)
    except ValueError:
        return value.replace(year=value.year - years, day=28)


def main() -> int:
    args = parse_args()
    devices = load_servermap(args.servermap)
    points_per_second = len(devices)
    end_time = parse_timestamp(args.end_time) if args.end_time else datetime.now(timezone.utc).replace(microsecond=0)
    start_time = parse_timestamp(args.start_time) if args.start_time else years_before(end_time, args.years)
    if start_time >= end_time:
        raise SystemExit("--start-time must be earlier than --end-time")
    available_points = int((end_time - start_time).total_seconds()) * points_per_second
    count = args.count if args.count is not None else available_points - args.start_index
    validate(args, count, available_points)
    qrcode_match = re.fullmatch(r"(.*?)(\d+)", devices[0].qrcode)
    if qrcode_match is None:
        raise SystemExit(f"QRCODEID has no numeric suffix: {devices[0].qrcode}")
    qrcode_prefix, qrcode_suffix = qrcode_match.groups()
    qrcode_width = args.qrcode_width or len(qrcode_suffix)
    start_timestamp = int(start_time.timestamp())
    cfg = Config(
        url=args.url,
        token=args.token or "dry-run",
        org=args.org,
        bucket=args.bucket,
        measurement=args.measurement,
        timeout=args.timeout,
        retries=args.retries,
        gzip_body=not args.no_gzip,
        timestamp_start_ns=int(start_time.timestamp() * NS_PER_SECOND),
        seed=args.seed,
        devices=devices,
        qrcode_mode=args.qrcode_mode,
        qrcode_prefix=qrcode_prefix,
        qrcode_width=qrcode_width,
        history_start_cycle=(start_timestamp + TAIPEI_UTC_OFFSET_SECONDS) // SECONDS_PER_CYCLE,
    )
    if args.dry_run:
        sample_count = min(count, args.batch_size)
        started = time.perf_counter()
        payload = make_batch(args.start_index, sample_count, cfg)
        elapsed = time.perf_counter() - started
        print(f"dry-run: generated {sample_count:,} points, {len(payload):,} bytes in {elapsed:.3f}s")
        print(f"range: {start_time.isoformat()} to {end_time.isoformat()} | total: {count:,} points")
        print(f"ServerMap: {args.servermap} | {points_per_second:,} points/second")
        print(f"QR code mode: {args.qrcode_mode}")
        print(payload.splitlines()[0].decode("utf-8"))
        return 0

    first = args.start_index
    stop = first + count
    next_index = first
    completed = 0
    transferred = 0
    started = time.perf_counter()
    pending = set()
    print(f"range: {start_time.isoformat()} to {end_time.isoformat()}")
    print(f"ServerMap: {args.servermap} | {points_per_second:,} points/second")
    print(f"QR code mode: {args.qrcode_mode}")
    print(
        f"writing {count:,} points to {cfg.bucket}/{cfg.measurement} with "
        f"{args.workers} {args.worker_mode} workers"
    )
    executor_class = ProcessPoolExecutor if args.worker_mode == "process" else ThreadPoolExecutor
    try:
        with executor_class(max_workers=args.workers) as executor:
            while next_index < stop or pending:
                while next_index < stop and len(pending) < args.workers * 2:
                    size = min(args.batch_size, stop - next_index)
                    pending.add(executor.submit(write_batch, next_index, size, cfg))
                    next_index += size
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    points, byte_count = future.result()
                    completed += points
                    transferred += byte_count
                elapsed = max(time.perf_counter() - started, 1e-9)
                print(
                    f"\r{completed:,}/{count:,} ({completed / count:.1%}) | "
                    f"{completed / elapsed:,.0f} points/s | {transferred / elapsed / 1_048_576:.1f} MiB/s",
                    end="",
                    flush=True,
                )
    except KeyboardInterrupt:
        print(f"\ninterrupted; {completed:,} points acknowledged", file=sys.stderr)
        return 130
    elapsed = time.perf_counter() - started
    print(f"\ncomplete: {completed:,} points in {elapsed:.2f}s ({completed / elapsed:,.0f} points/s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
