"""Downsample CCMS measurements from InfluxDB :8086 to archive :8087.

The source application continues to write only to the primary InfluxDB.  This
process queries completed source ranges, keeps the last value in each five
second window, writes the result to the archive InfluxDB, and then advances an
atomic checkpoint.  Retrying an unfinished range is safe because InfluxDB
points are identified by measurement, tag set, and timestamp.
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import math
import os
import random
import signal
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from influxdb_client import InfluxDBClient


DEFAULT_TAG_COLUMNS = (
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
class ArchiveConfig:
    source_url: str
    source_token: str
    source_org: str
    source_bucket: str
    destination_url: str
    destination_token: str
    destination_org: str
    destination_bucket: str
    measurement: str
    tag_columns: tuple[str, ...]
    interval_seconds: int
    chunk_seconds: int
    overlap_seconds: int
    safety_delay_seconds: int
    poll_seconds: float
    write_batch_size: int
    timeout_seconds: float
    retries: int
    gzip_body: bool
    checkpoint_path: Path
    dry_run: bool

    @property
    def checkpoint_identity(self) -> dict[str, Any]:
        return {
            "source_url": self.source_url.rstrip("/"),
            "source_org": self.source_org,
            "source_bucket": self.source_bucket,
            "destination_url": self.destination_url.rstrip("/"),
            "destination_org": self.destination_org,
            "destination_bucket": self.destination_bucket,
            "measurement": self.measurement,
            "interval_seconds": self.interval_seconds,
        }


class CheckpointError(RuntimeError):
    """Raised when a checkpoint is invalid or belongs to another job."""


class FileLock:
    """Hold an advisory one-byte lock for the lifetime of the process."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.handle: Any | None = None

    def __enter__(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+b")
        self.handle.seek(0, os.SEEK_END)
        if self.handle.tell() == 0:
            self.handle.write(b"0")
            self.handle.flush()
        self.handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self.handle.close()
            self.handle = None
            raise SystemExit(f"archive writer is already running (lock: {self.path})") from exc
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if self.handle is None:
            return
        try:
            self.handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        finally:
            self.handle.close()
            self.handle = None


def utc_now() -> datetime:
    return datetime.now(UTC)


def parse_time(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO-8601 time: {value}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def format_time(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def floor_time(value: datetime, seconds: int) -> datetime:
    epoch_microseconds = (
        int(value.astimezone(UTC).timestamp()) * 1_000_000 + value.microsecond
    )
    interval_microseconds = seconds * 1_000_000
    floored = epoch_microseconds - epoch_microseconds % interval_microseconds
    return datetime.fromtimestamp(floored / 1_000_000, tz=UTC)


def datetime_to_ns(value: datetime) -> int:
    utc_value = value.astimezone(UTC)
    seconds = int(utc_value.timestamp())
    return seconds * 1_000_000_000 + utc_value.microsecond * 1_000


def flux_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def build_flux_query(config: ArchiveConfig, start: datetime, stop: datetime) -> str:
    interval = f"{config.interval_seconds}s"
    return f'''from(bucket: {flux_string(config.source_bucket)})
    |> range(
        start: time(v: {flux_string(format_time(start))}),
        stop: time(v: {flux_string(format_time(stop))}),
    )
    |> filter(fn: (r) => r._measurement == {flux_string(config.measurement)})
    |> aggregateWindow(every: {interval}, fn: last, createEmpty: false)
    |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")'''


def escape_measurement(value: object) -> str:
    return str(value).replace("\\", "\\\\").replace(" ", "\\ ").replace(",", "\\,")


def escape_key(value: object) -> str:
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace(" ", "\\ ")
        .replace(",", "\\,")
        .replace("=", "\\=")
    )


def escape_tag_value(value: object) -> str:
    return escape_key(value)


def encode_field_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return f"{value}i"
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return repr(value)
    if isinstance(value, datetime):
        value = format_time(value)
    elif isinstance(value, date):
        value = value.isoformat()
    text = str(value)
    text = (
        text.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\r", "\\r")
        .replace("\n", "\\n")
    )
    return f'"{text}"'


def record_to_line(record: Any, config: ArchiveConfig) -> str | None:
    values = dict(record.values)
    measurement = values.get("_measurement") or config.measurement
    record_time = values.get("_time")
    if record_time is None and hasattr(record, "get_time"):
        record_time = record.get_time()
    if not isinstance(record_time, datetime):
        raise RuntimeError(f"query record has no datetime _time: {values!r}")

    tags: list[str] = []
    for column in config.tag_columns:
        value = values.get(column)
        if value is not None:
            tags.append(f"{escape_key(column)}={escape_tag_value(value)}")

    fields: list[str] = []
    tag_names = set(config.tag_columns)
    for column in sorted(values):
        if column in SYSTEM_COLUMNS or column in tag_names or column.startswith("_"):
            continue
        encoded = encode_field_value(values[column])
        if encoded is not None:
            fields.append(f"{escape_key(column)}={encoded}")
    if not fields:
        return None

    series = escape_measurement(measurement)
    if tags:
        series += "," + ",".join(tags)
    return f"{series} {','.join(fields)} {datetime_to_ns(record_time)}"


def read_token(environment_name: str, token_file: Path | None, required: bool = True) -> str:
    value = os.getenv(environment_name, "").strip()
    if value:
        return value
    if token_file is not None:
        try:
            value = token_file.read_text(encoding="utf-8-sig").strip()
        except OSError as exc:
            raise SystemExit(f"cannot read token file {token_file}: {exc}") from exc
        if value:
            return value
    if required:
        raise SystemExit(f"set {environment_name} or provide its token file")
    return ""


def load_checkpoint(path: Path, config: ArchiveConfig) -> datetime | None:
    if not path.exists():
        return None
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
        if document.get("version") != 1:
            raise CheckpointError("unsupported checkpoint version")
        if document.get("identity") != config.checkpoint_identity:
            raise CheckpointError(
                "checkpoint belongs to different source/destination settings; "
                "use a different --checkpoint path"
            )
        return parse_time(document["last_completed_stop"])
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise CheckpointError(f"cannot load checkpoint {path}: {exc}") from exc


def save_checkpoint(path: Path, config: ArchiveConfig, stop: datetime) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    document = {
        "version": 1,
        "last_completed_stop": format_time(stop),
        "updated_at": format_time(utc_now()),
        "identity": config.checkpoint_identity,
    }
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(document, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def write_lines(lines: list[str], config: ArchiveConfig) -> int:
    if not lines:
        return 0
    body = ("\n".join(lines) + "\n").encode("utf-8")
    if config.dry_run:
        return len(body)
    if config.gzip_body:
        body = gzip.compress(body, compresslevel=1)
    query = urllib.parse.urlencode(
        {
            "org": config.destination_org,
            "bucket": config.destination_bucket,
            "precision": "ns",
        }
    )
    endpoint = f"{config.destination_url.rstrip('/')}/api/v2/write?{query}"
    headers = {
        "Authorization": f"Token {config.destination_token}",
        "Content-Type": "text/plain; charset=utf-8",
        "Accept": "application/json",
    }
    if config.gzip_body:
        headers["Content-Encoding"] = "gzip"

    for attempt in range(config.retries + 1):
        request = urllib.request.Request(endpoint, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=config.timeout_seconds) as response:
                if response.status != 204:
                    raise RuntimeError(f"archive returned HTTP {response.status}")
            return len(body)
        except urllib.error.HTTPError as exc:
            detail = exc.read(1000).decode("utf-8", errors="replace")
            retryable = exc.code == 429 or exc.code >= 500
            if not retryable or attempt == config.retries:
                raise RuntimeError(f"archive HTTP {exc.code}: {detail}") from exc
            retry_after = exc.headers.get("Retry-After")
            delay = float(retry_after) if retry_after and retry_after.isdigit() else 0.0
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            if attempt == config.retries:
                raise RuntimeError(f"archive write failed: {exc}") from exc
            delay = 0.0
        delay = max(delay, min(30.0, 0.5 * (2**attempt)) + random.random() * 0.25)
        logging.warning("archive write retry %d/%d in %.2fs", attempt + 1, config.retries, delay)
        time.sleep(delay)
    raise AssertionError("write retry loop exited unexpectedly")


def archive_range(
    query_api: Any,
    config: ArchiveConfig,
    start: datetime,
    stop: datetime,
) -> tuple[int, int, str | None]:
    query = build_flux_query(config, start, stop)
    line_batch: list[str] = []
    point_count = 0
    transferred_bytes = 0
    sample_line: str | None = None
    records: Iterable[Any] = query_api.query_stream(query=query, org=config.source_org)
    for record in records:
        line = record_to_line(record, config)
        if line is None:
            continue
        if sample_line is None:
            sample_line = line
        line_batch.append(line)
        point_count += 1
        if len(line_batch) >= config.write_batch_size:
            transferred_bytes += write_lines(line_batch, config)
            line_batch.clear()
    if line_batch:
        transferred_bytes += write_lines(line_batch, config)
    return point_count, transferred_bytes, sample_line


def check_health(url: str, timeout_seconds: float) -> None:
    endpoint = f"{url.rstrip('/')}/health"
    try:
        with urllib.request.urlopen(endpoint, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (OSError, ValueError, urllib.error.URLError) as exc:
        raise RuntimeError(f"health check failed for {url}: {exc}") from exc
    if payload.get("status") != "pass":
        raise RuntimeError(f"InfluxDB is not healthy at {url}: {payload}")


def configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Downsample InfluxDB meas_data from :8086 to archive :8087"
    )
    parser.add_argument("--source-url", default="http://localhost:8086")
    parser.add_argument("--source-token-file", type=Path)
    parser.add_argument("--source-org", default="delta")
    parser.add_argument("--source-bucket", default="StoreHouseMeasData")
    parser.add_argument("--destination-url", default="http://localhost:8087")
    parser.add_argument("--destination-token-file", type=Path)
    parser.add_argument("--destination-org", default="delta")
    parser.add_argument("--destination-bucket", default="StoreHouseMeasArchive")
    parser.add_argument("--measurement", default="meas_data")
    parser.add_argument(
        "--tag-columns",
        default=",".join(DEFAULT_TAG_COLUMNS),
        help="comma-separated tag columns used by the source measurement",
    )
    parser.add_argument("--interval-seconds", type=int, default=5)
    parser.add_argument("--chunk-seconds", type=int, default=60)
    parser.add_argument("--overlap-seconds", type=int, default=10)
    parser.add_argument("--safety-delay-seconds", type=int, default=10)
    parser.add_argument("--poll-seconds", type=float, default=5.0)
    parser.add_argument("--write-batch-size", type=int, default=5000)
    parser.add_argument("--timeout-seconds", type=float, default=120.0)
    parser.add_argument("--retries", type=int, default=5)
    parser.add_argument(
        "--checkpoint",
        type=Path,
        default=Path(r"E:\InfluxDBArchive\archive_writer_checkpoint.json"),
    )
    start = parser.add_mutually_exclusive_group()
    start.add_argument("--start-time", type=parse_time, help="first source time in ISO-8601")
    start.add_argument("--start-now", action="store_true", help="start at current five-second boundary")
    parser.add_argument("--once", action="store_true", help="catch up to safe time and exit")
    parser.add_argument("--max-chunks", type=int, help="maximum chunks this process may handle")
    parser.add_argument("--dry-run", action="store_true", help="query and encode but do not write or checkpoint")
    parser.add_argument("--no-gzip", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def validate_args(args: argparse.Namespace) -> None:
    positive_integer_names = (
        "interval_seconds",
        "chunk_seconds",
        "write_batch_size",
        "timeout_seconds",
    )
    for name in positive_integer_names:
        if getattr(args, name) <= 0:
            raise SystemExit(f"--{name.replace('_', '-')} must be greater than zero")
    if args.chunk_seconds % args.interval_seconds:
        raise SystemExit("--chunk-seconds must be divisible by --interval-seconds")
    if args.overlap_seconds < 0 or args.overlap_seconds % args.interval_seconds:
        raise SystemExit("--overlap-seconds must be zero or divisible by --interval-seconds")
    if args.safety_delay_seconds < 0:
        raise SystemExit("--safety-delay-seconds must not be negative")
    if args.poll_seconds <= 0:
        raise SystemExit("--poll-seconds must be greater than zero")
    if args.retries < 0:
        raise SystemExit("--retries must not be negative")
    if args.max_chunks is not None and args.max_chunks <= 0:
        raise SystemExit("--max-chunks must be greater than zero")


def make_config(args: argparse.Namespace) -> ArchiveConfig:
    source_token = read_token(
        "INFLUX_ARCHIVE_SOURCE_TOKEN", args.source_token_file, required=True
    )
    destination_token = read_token(
        "INFLUX_ARCHIVE_DEST_TOKEN",
        args.destination_token_file,
        required=not args.dry_run,
    )
    tag_columns = tuple(column.strip() for column in args.tag_columns.split(",") if column.strip())
    if not tag_columns:
        raise SystemExit("--tag-columns must contain at least one column")
    return ArchiveConfig(
        source_url=args.source_url,
        source_token=source_token,
        source_org=args.source_org,
        source_bucket=args.source_bucket,
        destination_url=args.destination_url,
        destination_token=destination_token,
        destination_org=args.destination_org,
        destination_bucket=args.destination_bucket,
        measurement=args.measurement,
        tag_columns=tag_columns,
        interval_seconds=args.interval_seconds,
        chunk_seconds=args.chunk_seconds,
        overlap_seconds=args.overlap_seconds,
        safety_delay_seconds=args.safety_delay_seconds,
        poll_seconds=args.poll_seconds,
        write_batch_size=args.write_batch_size,
        timeout_seconds=args.timeout_seconds,
        retries=args.retries,
        gzip_body=not args.no_gzip,
        checkpoint_path=args.checkpoint,
        dry_run=args.dry_run,
    )


def run(args: argparse.Namespace) -> int:
    validate_args(args)
    config = make_config(args)
    checkpoint = load_checkpoint(config.checkpoint_path, config)
    if checkpoint is None:
        if args.start_time is not None:
            checkpoint = floor_time(args.start_time, config.interval_seconds)
        elif args.start_now:
            checkpoint = floor_time(utc_now(), config.interval_seconds)
        else:
            raise SystemExit(
                f"checkpoint does not exist: {config.checkpoint_path}; "
                "choose --start-now or --start-time"
            )
        logging.info("new checkpoint starts at %s", format_time(checkpoint))
    else:
        checkpoint = floor_time(checkpoint, config.interval_seconds)
        logging.info("resuming checkpoint %s", format_time(checkpoint))

    check_health(config.source_url, config.timeout_seconds)
    if not config.dry_run:
        check_health(config.destination_url, config.timeout_seconds)

    stop_requested = False

    def request_stop(signum: int, frame: Any) -> None:
        nonlocal stop_requested
        stop_requested = True
        logging.info("stop requested; finishing the current chunk")

    for signal_name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        if hasattr(signal, signal_name):
            signal.signal(getattr(signal, signal_name), request_stop)

    client = InfluxDBClient(
        url=config.source_url,
        token=config.source_token,
        org=config.source_org,
        timeout=int(config.timeout_seconds * 1000),
        enable_gzip=True,
    )
    query_api = client.query_api()
    handled_chunks = 0
    total_points = 0
    started = time.monotonic()
    try:
        while not stop_requested:
            safe_stop = floor_time(
                utc_now() - timedelta(seconds=config.safety_delay_seconds),
                config.interval_seconds,
            )
            available_seconds = (safe_stop - checkpoint).total_seconds()
            if available_seconds <= 0 or (
                available_seconds < config.chunk_seconds and not args.once
            ):
                if args.once or (
                    args.max_chunks is not None and handled_chunks >= args.max_chunks
                ):
                    break
                time.sleep(config.poll_seconds)
                continue

            chunk_stop = min(
                checkpoint + timedelta(seconds=config.chunk_seconds),
                safe_stop,
            )
            query_start = checkpoint - timedelta(seconds=config.overlap_seconds)
            query_start = floor_time(query_start, config.interval_seconds)
            logging.info(
                "archive range %s <= time < %s%s",
                format_time(query_start),
                format_time(chunk_stop),
                " (dry-run)" if config.dry_run else "",
            )
            points, transferred, sample_line = archive_range(
                query_api, config, query_start, chunk_stop
            )
            if config.dry_run:
                logging.info(
                    "dry-run encoded %d archive points (%d bytes before compression)",
                    points,
                    transferred,
                )
                if sample_line:
                    logging.info("sample: %s", sample_line[:1000])
            else:
                save_checkpoint(config.checkpoint_path, config, chunk_stop)
                logging.info(
                    "committed checkpoint %s: %d points, %d transferred bytes",
                    format_time(chunk_stop),
                    points,
                    transferred,
                )
            checkpoint = chunk_stop
            handled_chunks += 1
            total_points += points
            if args.max_chunks is not None and handled_chunks >= args.max_chunks:
                break
    finally:
        client.close()
    elapsed = max(time.monotonic() - started, 1e-9)
    logging.info(
        "stopped after %d chunks and %d archive points in %.2fs (%.0f points/s)",
        handled_chunks,
        total_points,
        elapsed,
        total_points / elapsed,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(args.verbose)
    lock_path = args.checkpoint.with_suffix(args.checkpoint.suffix + ".lock")
    try:
        with FileLock(lock_path):
            return run(args)
    except KeyboardInterrupt:
        logging.info("interrupted")
        return 130
    except Exception:
        logging.exception("archive writer failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
