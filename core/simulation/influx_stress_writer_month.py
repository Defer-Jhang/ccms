"""Standalone high-volume CCMS data generator.

The program writes data to:

1. Microsoft SQL Server
   Table:
       dbo.MeasSessionIndex

   One row per device per 3-hour QRCode cycle.

2. InfluxDB 2.x
   Measurement:
       meas_data

   Tags:
       storehouse_id
       pallet_position
       position_id
       return_code

   Fields:
       qrcode
       serialboard_id
       protectboard_id
       voltage
       temperature
       current
       ...

Consistency:
    MSSQL metadata for a cycle is committed first.
    Only after MSSQL succeeds will InfluxDB points for that cycle be written.
"""

from __future__ import annotations

import argparse
import csv
import ctypes
import gzip
import os
import random
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from concurrent.futures import (
    FIRST_COMPLETED,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    wait,
)

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyodbc


# ============================================================
# Constants
# ============================================================

LARGE_RUN = 1_000_000_000

SECONDS_PER_CYCLE = 3 * 60 * 60

NS_PER_SECOND = 1_000_000_000

TAIPEI_UTC_OFFSET_SECONDS = 8 * 60 * 60

DEFAULT_RETURN_CODE = "OK"

VOLTAGE_STEPS = tuple(
    value / 1000.0
    for value in range(10, 31)
)

TEMPERATURE_STEPS = (
    0.03,
    0.04,
    0.05,
)

DEFAULT_SERVERMAP = (
    Path(__file__).resolve().parents[1]
    / "db"
    / "ServerMap.csv"
)


# ============================================================
# Data classes
# ============================================================


@dataclass(frozen=True)
class DeviceMapping:
    """Store one device mapping loaded from ServerMap.csv."""

    storehouse_id: str
    pallet_id: str
    pallet_position: str
    serialboard_id: str
    protectboard_id: str
    position_id: str
    qrcode: str

    pallet_number: int

    tag_prefix: str
    field_prefix: str


@dataclass(frozen=True)
class Config:
    """Store InfluxDB and simulation runtime configuration."""

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

    return_code: str


@dataclass(frozen=True)
class MssqlConfig:
    """Store Microsoft SQL Server connection configuration."""

    server: str
    port: int
    database: str

    username: str | None
    password: str | None

    driver: str

    trusted_connection: bool
    trust_server_certificate: bool

    timeout: int
    retries: int


# ============================================================
# CPU monitor
# ============================================================


class SystemCpuMonitor:
    """Measure system-wide Windows CPU usage."""

    def __init__(self) -> None:
        try:
            if os.name != "nt":
                raise RuntimeError(
                    "--cpu-target currently requires Windows"
                )

            (
                self.previous_idle,
                self.previous_total,
            ) = self._read_times()

            self.previous_wall = time.perf_counter()

        except Exception as exc:
            raise RuntimeError(
                f"SystemCpuMonitor initialization failed: {exc}"
            ) from exc

    @staticmethod
    def _read_times() -> tuple[int, int]:
        """Read Windows CPU counters."""

        try:
            idle = ctypes.c_ulonglong()
            kernel = ctypes.c_ulonglong()
            user = ctypes.c_ulonglong()

            result = ctypes.windll.kernel32.GetSystemTimes(
                ctypes.byref(idle),
                ctypes.byref(kernel),
                ctypes.byref(user),
            )

            if not result:
                raise ctypes.WinError()

            return (
                idle.value,
                kernel.value + user.value,
            )

        except Exception as exc:
            raise RuntimeError(
                f"Failed to read Windows CPU times: {exc}"
            ) from exc

    def sample(self) -> tuple[float, float]:
        """Return system CPU usage percentage and sample interval."""

        try:
            idle, total = self._read_times()
            wall = time.perf_counter()

            total_delta = (
                total
                - self.previous_total
            )

            idle_delta = (
                idle
                - self.previous_idle
            )

            if total_delta <= 0:
                cpu_percent = 0.0

            else:
                cpu_percent = (
                    100.0
                    * (
                        total_delta
                        - idle_delta
                    )
                    / total_delta
                )

            interval = (
                wall
                - self.previous_wall
            )

            self.previous_idle = idle
            self.previous_total = total
            self.previous_wall = wall

            return (
                max(
                    0.0,
                    min(
                        cpu_percent,
                        100.0,
                    ),
                ),
                interval,
            )

        except Exception as exc:
            raise RuntimeError(
                f"Failed to sample CPU usage: {exc}"
            ) from exc


# ============================================================
# InfluxDB line protocol helpers
# ============================================================


def escape_tag(value: object) -> str:
    """Escape an InfluxDB line protocol tag value."""

    return (
        str(value)
        .replace("\\", "\\\\")
        .replace(" ", "\\ ")
        .replace(",", "\\,")
        .replace("=", "\\=")
    )


def escape_measurement(value: object) -> str:
    """Escape an InfluxDB line protocol measurement name."""

    return (
        str(value)
        .replace("\\", "\\\\")
        .replace(" ", "\\ ")
        .replace(",", "\\,")
    )


def escape_field_string(value: object) -> str:
    """Escape an InfluxDB line protocol string field value."""

    return (
        str(value)
        .replace("\\", "\\\\")
        .replace('"', '\\"')
    )


# ============================================================
# ServerMap
# ============================================================


def load_servermap(
    path: Path,
) -> tuple[DeviceMapping, ...]:
    """Load and validate the ServerMap.csv file."""

    required_columns = {
        "StoreHouseID",
        "PalletID",
        "SerialBoardID",
        "ProtectBoardID",
        "PalletPosition",
        "Position",
        "QRCODEID",
    }

    try:
        with path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as handle:

            reader = csv.DictReader(
                handle
            )

            missing_columns = (
                required_columns.difference(
                    reader.fieldnames or ()
                )
            )

            if missing_columns:
                raise RuntimeError(
                    "ServerMap is missing columns: "
                    + ", ".join(
                        sorted(
                            missing_columns
                        )
                    )
                )

            rows = list(
                reader
            )

    except Exception as exc:
        raise RuntimeError(
            f"Cannot read ServerMap "
            f"{path}: {exc}"
        ) from exc

    if not rows:
        raise RuntimeError(
            f"ServerMap contains no data rows: {path}"
        )

    try:
        rows.sort(
            key=lambda row: (
                int(
                    row["StoreHouseID"]
                ),
                {
                    "L": 0,
                    "R": 1,
                }.get(
                    row["PalletPosition"],
                    2,
                ),
                int(
                    row["Position"]
                ),
            )
        )

    except Exception as exc:
        raise RuntimeError(
            f"Failed to sort ServerMap: {exc}"
        ) from exc

    pallet_numbers: dict[
        tuple[str, ...],
        int,
    ] = {}

    identities: set[
        tuple[str, str, str]
    ] = set()

    devices: list[
        DeviceMapping
    ] = []

    try:
        for row in rows:

            identity = (
                row["StoreHouseID"],
                row["PalletPosition"],
                row["Position"],
            )

            if identity in identities:
                raise RuntimeError(
                    "Duplicate ServerMap topology: "
                    f"StoreHouseID={identity[0]}, "
                    f"PalletPosition={identity[1]}, "
                    f"Position={identity[2]}"
                )

            identities.add(
                identity
            )

            pallet_key = (
                row["StoreHouseID"],
                row["PalletID"],
                row["PalletPosition"],
                row["SerialBoardID"],
                row["ProtectBoardID"],
            )

            pallet_number = (
                pallet_numbers.setdefault(
                    pallet_key,
                    len(
                        pallet_numbers
                    ),
                )
            )

            # --------------------------------------------
            # InfluxDB Tags
            #
            # Only low-cardinality topology information
            # is stored as tags.
            # --------------------------------------------

            tag_prefix = (
                f"storehouse_id="
                f"{escape_tag(row['StoreHouseID'])},"
                f"pallet_position="
                f"{escape_tag(row['PalletPosition'])},"
                f"position_id="
                f"{escape_tag(row['Position'])},"
            )

            # --------------------------------------------
            # InfluxDB Fields
            #
            # SerialBoardID / ProtectBoardID are fields,
            # not tags.
            # --------------------------------------------

            field_prefix = (
                f'serialboard_id="'
                f'{escape_field_string(row["SerialBoardID"])}",'
                f'protectboard_id="'
                f'{escape_field_string(row["ProtectBoardID"])}",'
            )

            devices.append(
                DeviceMapping(
                    storehouse_id=(
                        row[
                            "StoreHouseID"
                        ]
                    ),
                    pallet_id=(
                        row[
                            "PalletID"
                        ]
                    ),
                    pallet_position=(
                        row[
                            "PalletPosition"
                        ]
                    ),
                    serialboard_id=(
                        row[
                            "SerialBoardID"
                        ]
                    ),
                    protectboard_id=(
                        row[
                            "ProtectBoardID"
                        ]
                    ),
                    position_id=(
                        row[
                            "Position"
                        ]
                    ),
                    qrcode=(
                        row[
                            "QRCODEID"
                        ]
                    ),
                    pallet_number=(
                        pallet_number
                    ),
                    tag_prefix=(
                        tag_prefix
                    ),
                    field_prefix=(
                        field_prefix
                    ),
                )
            )

    except Exception as exc:
        raise RuntimeError(
            f"Failed to build ServerMap mapping: {exc}"
        ) from exc

    return tuple(
        devices
    )


# ============================================================
# Deterministic data generation
# ============================================================


def deterministic_int(
    low: int,
    high: int,
    seed: int,
    *parts: int,
) -> int:
    """Return a deterministic pseudo-random integer."""

    value = seed

    for part in parts:
        value = (
            value
            * 6364136223846793005
            + part
            * 1442695040888963407
        ) % (2**64)

    return (
        low
        + value
        % (
            high
            - low
            + 1
        )
    )


def cumulative_steps(
    steps: tuple[float, ...],
    updates: int,
    offset: int,
) -> float:
    """Return the accumulated value from a cyclic step sequence."""

    cycles, remainder = divmod(
        updates,
        len(
            steps
        ),
    )

    return (
        cycles
        * sum(
            steps
        )
        + sum(
            steps[
                (
                    offset
                    + index
                )
                % len(
                    steps
                )
            ]
            for index in range(
                remainder
            )
        )
    )


def esp_values(
    second_index: int,
    pallet_number: int,
    cfg: Config,
) -> tuple[
    float,
    float,
    float,
    tuple[int, ...],
]:
    """Generate deterministic battery simulation values."""

    timestamp_seconds = (
        cfg.timestamp_start_ns
        // NS_PER_SECOND
        + second_index
    )

    (
        cycle_number,
        second_in_cycle,
    ) = divmod(
        timestamp_seconds
        + TAIPEI_UTC_OFFSET_SECONDS,
        SECONDS_PER_CYCLE,
    )

    updates = (
        second_in_cycle
        // 3
    )

    voltage_offset = (
        deterministic_int(
            0,
            len(
                VOLTAGE_STEPS
            )
            - 1,
            cfg.seed,
            cycle_number,
            pallet_number,
            1,
        )
    )

    temperature_offset = (
        deterministic_int(
            0,
            len(
                TEMPERATURE_STEPS
            )
            - 1,
            cfg.seed,
            cycle_number,
            pallet_number,
            2,
        )
    )

    voltage = min(
        3.1
        + cumulative_steps(
            VOLTAGE_STEPS,
            updates,
            voltage_offset,
        ),
        4.085,
    )

    temperature = min(
        23.0
        + cumulative_steps(
            TEMPERATURE_STEPS,
            updates,
            temperature_offset,
        ),
        24.5,
    )

    current = (
        deterministic_int(
            171,
            174,
            cfg.seed,
            cycle_number,
            second_in_cycle,
            pallet_number,
            3,
        )
        / 10.0
    )

    wire_voltage = tuple(
        deterministic_int(
            100,
            300,
            cfg.seed,
            cycle_number,
            second_in_cycle,
            pallet_number,
            channel,
        )
        for channel in range(
            5
        )
    )

    return (
        int(
            voltage
            * 1000
        )
        / 1000.0,

        (
            int(
                temperature
                * 10
                + 2731
            )
            - 2731
        )
        / 10.0,

        current,

        wire_voltage,
    )


# ============================================================
# Time / QRCode cycle functions
# ============================================================


def get_absolute_cycle(
    timestamp_seconds: int,
) -> int:
    """Return the Taipei-local 3-hour cycle number."""

    return (
        timestamp_seconds
        + TAIPEI_UTC_OFFSET_SECONDS
    ) // SECONDS_PER_CYCLE


def get_cycle_start_utc_seconds(
    absolute_cycle: int,
) -> int:
    """Return the UTC epoch start time of a Taipei-local cycle."""

    return (
        absolute_cycle
        * SECONDS_PER_CYCLE
        - TAIPEI_UTC_OFFSET_SECONDS
    )


def get_qrcode(
    absolute_cycle: int,
    device_index: int,
    cfg: Config,
) -> str:
    """Return the QRCode for one device within a cycle."""

    device = cfg.devices[
        device_index
    ]

    if (
        cfg.qrcode_mode
        == "servermap"
    ):
        return device.qrcode

    cycle_index = (
        absolute_cycle
        - cfg.history_start_cycle
    )

    qrcode_number = (
        cycle_index
        * len(
            cfg.devices
        )
        + device_index
        + 1
    )

    return (
        f"{cfg.qrcode_prefix}"
        f"{qrcode_number:0{cfg.qrcode_width}d}"
    )


def build_cycle_qrcodes(
    absolute_cycle: int,
    cfg: Config,
) -> tuple[str, ...]:
    """Generate all QRCode values for one cycle."""

    return tuple(
        get_qrcode(
            absolute_cycle,
            device_index,
            cfg,
        )
        for device_index
        in range(
            len(
                cfg.devices
            )
        )
    )


# ============================================================
# MSSQL connection
# ============================================================


def build_mssql_connection_string(
    cfg: MssqlConfig,
) -> str:
    """Build the SQL Server ODBC connection string."""

    parts = [
        (
            f"DRIVER="
            f"{{{cfg.driver}}}"
        ),
        (
            f"SERVER="
            f"{cfg.server},"
            f"{cfg.port}"
        ),
        (
            f"DATABASE="
            f"{cfg.database}"
        ),
        (
            "TrustServerCertificate="
            + (
                "yes"
                if cfg.trust_server_certificate
                else "no"
            )
        ),
        (
            f"Connection Timeout="
            f"{cfg.timeout}"
        ),
    ]

    if cfg.trusted_connection:

        parts.append(
            "Trusted_Connection=yes"
        )

    else:

        if not cfg.username:
            raise RuntimeError(
                "MSSQL username is required. "
                "Use --mssql-user or --mssql-trusted."
            )

        if cfg.password is None:
            raise RuntimeError(
                "MSSQL password is required."
            )

        parts.append(
            f"UID={cfg.username}"
        )

        parts.append(
            f"PWD={cfg.password}"
        )

    return (
        ";".join(
            parts
        )
        + ";"
    )


def connect_mssql(
    cfg: MssqlConfig,
) -> pyodbc.Connection:
    """Open a Microsoft SQL Server connection."""

    connection_string = (
        build_mssql_connection_string(
            cfg
        )
    )

    last_error: Exception | None = None

    for attempt in range(
        cfg.retries
        + 1
    ):
        try:
            return pyodbc.connect(
                connection_string,
                autocommit=False,
            )

        except Exception as exc:
            last_error = exc

            if (
                attempt
                >= cfg.retries
            ):
                break

            delay = min(
                10.0,
                0.5
                * (
                    2
                    ** attempt
                ),
            )

            print(
                f"MSSQL connection retry "
                f"{attempt + 1}/"
                f"{cfg.retries} "
                f"after {delay:.1f}s",
                flush=True,
            )

            time.sleep(
                delay
            )

    raise RuntimeError(
        f"MSSQL connection failed: "
        f"{last_error}"
    )


# ============================================================
# MSSQL schema
# ============================================================


def ensure_mssql_schema(
    connection: pyodbc.Connection,
) -> None:
    """Create the MSSQL metadata table and indexes when missing."""

    sql = """
IF OBJECT_ID(N'dbo.MeasSessionIndex', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.MeasSessionIndex
    (
        SessionIndexID BIGINT IDENTITY(1,1) NOT NULL,

        QRCode VARCHAR(100) NOT NULL,

        StartTime DATETIME2(0) NOT NULL,

        Status VARCHAR(20) NOT NULL,

        PalletID VARCHAR(100) NULL,

        SerialBoardID VARCHAR(100) NULL,

        ProtectBoardID VARCHAR(100) NULL,

        StoreHouseID INT NOT NULL,

        PalletPosition VARCHAR(10) NOT NULL,

        PositionID INT NOT NULL,

        CreatedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_MeasSessionIndex_CreatedAt
            DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_MeasSessionIndex
            PRIMARY KEY CLUSTERED
            (
                SessionIndexID
            )
    );
END;


IF COL_LENGTH(N'dbo.MeasSessionIndex', N'PalletID') IS NULL
BEGIN
    ALTER TABLE dbo.MeasSessionIndex
        ADD PalletID VARCHAR(100) NULL;
END;


IF COL_LENGTH(N'dbo.MeasSessionIndex', N'Status') IS NULL
BEGIN
    ALTER TABLE dbo.MeasSessionIndex
        ADD Status VARCHAR(20) NULL;
END;


IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name =
        N'UX_MeasSessionIndex_QRCode_StartTime'
      AND object_id =
        OBJECT_ID(
            N'dbo.MeasSessionIndex'
        )
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX
        UX_MeasSessionIndex_QRCode_StartTime

    ON dbo.MeasSessionIndex
    (
        QRCode,
        StartTime
    )

    INCLUDE
    (
        Status,
        SerialBoardID,
        ProtectBoardID,
        StoreHouseID,
        PalletPosition,
        PositionID
    );
END;


IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name =
        N'IX_MeasSessionIndex_DeviceTime'
      AND object_id =
        OBJECT_ID(
            N'dbo.MeasSessionIndex'
        )
)
BEGIN
    CREATE NONCLUSTERED INDEX
        IX_MeasSessionIndex_DeviceTime

    ON dbo.MeasSessionIndex
    (
        StoreHouseID,
        PalletPosition,
        PositionID,
        StartTime
    )

    INCLUDE
    (
        QRCode,
        Status,
        SerialBoardID,
        ProtectBoardID
    );
END;


IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name =
        N'IX_MeasSessionIndex_SerialBoardTime'
      AND object_id =
        OBJECT_ID(
            N'dbo.MeasSessionIndex'
        )
)
BEGIN
    CREATE NONCLUSTERED INDEX
        IX_MeasSessionIndex_SerialBoardTime

    ON dbo.MeasSessionIndex
    (
        SerialBoardID,
        StartTime
    )

    INCLUDE
    (
        QRCode,
        Status,
        ProtectBoardID,
        StoreHouseID,
        PalletPosition,
        PositionID
    );
END;


IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name =
        N'IX_MeasSessionIndex_ProtectBoardTime'
      AND object_id =
        OBJECT_ID(
            N'dbo.MeasSessionIndex'
        )
)
BEGIN
    CREATE NONCLUSTERED INDEX
        IX_MeasSessionIndex_ProtectBoardTime

    ON dbo.MeasSessionIndex
    (
        ProtectBoardID,
        StartTime
    )

    INCLUDE
    (
        QRCode,
        Status,
        SerialBoardID,
        StoreHouseID,
        PalletPosition,
        PositionID
    );
END;
"""

    cursor = None

    try:
        cursor = (
            connection.cursor()
        )

        cursor.execute(
            sql
        )

        connection.commit()

    except Exception as exc:

        connection.rollback()

        raise RuntimeError(
            f"Failed to create MSSQL schema: {exc}"
        ) from exc

    finally:

        if cursor is not None:
            try:
                cursor.close()

            except Exception:
                pass


# ============================================================
# MSSQL cycle rows
# ============================================================


def build_cycle_index_rows(
    absolute_cycle: int,
    cfg: Config,
) -> list[tuple]:
    """Build one MSSQL metadata row for every device in one cycle."""

    try:
        cycle_start_seconds = (
            get_cycle_start_utc_seconds(
                absolute_cycle
            )
        )

        # SQL Server DATETIME2 has no timezone information.
        # Store UTC time as a timezone-naive datetime.
        cycle_start = (
            datetime.fromtimestamp(
                cycle_start_seconds,
                tz=timezone.utc,
            )
            .replace(
                tzinfo=None
            )
        )

        qrcodes = (
            build_cycle_qrcodes(
                absolute_cycle,
                cfg,
            )
        )

        rows: list[
            tuple
        ] = []

        for (
            device_index,
            device,
        ) in enumerate(
            cfg.devices
        ):

            rows.append(
                (
                    qrcodes[
                        device_index
                    ],
                    cycle_start,
                    cfg.return_code,
                    device.pallet_id,
                    device.serialboard_id,
                    device.protectboard_id,
                    int(
                        device.storehouse_id
                    ),
                    device.pallet_position,
                    int(
                        device.position_id
                    ),
                )
            )

        return rows

    except Exception as exc:
        raise RuntimeError(
            f"Failed to build MSSQL cycle rows: {exc}"
        ) from exc


# ============================================================
# MSSQL cycle insert
# ============================================================


def insert_cycle_index_mssql(
    connection: pyodbc.Connection,
    absolute_cycle: int,
    cfg: Config,
    retries: int = 3,
) -> int:
    """Bulk insert one cycle of QRCode metadata into MSSQL.

    Existing rows with the same QRCode and StartTime are skipped.
    """

    rows = build_cycle_index_rows(
        absolute_cycle,
        cfg,
    )

    last_error: Exception | None = None

    for attempt in range(
        retries + 1
    ):
        cursor = None

        try:
            cursor = connection.cursor()

            # ------------------------------------------------
            # Create temporary staging table
            # ------------------------------------------------

            cursor.execute(
                """
SET NOCOUNT ON;

IF OBJECT_ID(
    'tempdb..#MeasSessionStage'
) IS NOT NULL
BEGIN
    DROP TABLE #MeasSessionStage;
END;

CREATE TABLE #MeasSessionStage
(
    QRCode VARCHAR(100) NOT NULL,
    StartTime DATETIME2(0) NOT NULL,
    Status VARCHAR(20) NOT NULL,
    PalletID VARCHAR(100) NULL,
    SerialBoardID VARCHAR(100) NULL,
    ProtectBoardID VARCHAR(100) NULL,
    StoreHouseID INT NOT NULL,
    PalletPosition VARCHAR(10) NOT NULL,
    PositionID INT NOT NULL
);
"""
            )

            # ------------------------------------------------
            # Insert staging rows
            #
            # Only ~2016 rows per cycle, so disabling
            # fast_executemany improves compatibility with
            # SQL Server local temporary tables.
            # ------------------------------------------------

            cursor.fast_executemany = False

            cursor.executemany(
                """
INSERT INTO #MeasSessionStage
(
    QRCode,
    StartTime,
    Status,
    PalletID,
    SerialBoardID,
    ProtectBoardID,
    StoreHouseID,
    PalletPosition,
    PositionID
)
VALUES
(
    ?, ?, ?, ?, ?, ?, ?, ?, ?
);
""",
                rows,
            )

            # ------------------------------------------------
            # Merge staging rows into final table
            # ------------------------------------------------

            cursor.execute(
                """
SET NOCOUNT ON;

INSERT INTO dbo.MeasSessionIndex
(
    QRCode,
    StartTime,
    Status,
    PalletID,
    SerialBoardID,
    ProtectBoardID,
    StoreHouseID,
    PalletPosition,
    PositionID
)
SELECT
    S.QRCode,
    S.StartTime,
    S.Status,
    S.PalletID,
    S.SerialBoardID,
    S.ProtectBoardID,
    S.StoreHouseID,
    S.PalletPosition,
    S.PositionID
FROM #MeasSessionStage AS S
WHERE NOT EXISTS
(
    SELECT 1
    FROM dbo.MeasSessionIndex AS T
    WHERE
        T.QRCode = S.QRCode
        AND T.StartTime = S.StartTime
);

SELECT CAST(@@ROWCOUNT AS BIGINT) AS InsertedRows;
"""
            )

            # ------------------------------------------------
            # Move to the result set containing SELECT
            # ------------------------------------------------

            result = None

            while True:

                if cursor.description is not None:
                    result = cursor.fetchone()
                    break

                if not cursor.nextset():
                    break

            if result is None:
                raise RuntimeError(
                    "MSSQL INSERT succeeded but inserted row count "
                    "could not be retrieved."
                )

            inserted_count = int(
                result[0]
            )

            connection.commit()

            return inserted_count

        except Exception as exc:
            last_error = exc

            try:
                connection.rollback()

            except Exception:
                pass

            if attempt >= retries:
                break

            delay = min(
                10.0,
                0.5 * (2 ** attempt),
            )

            print(
                f"\nMSSQL cycle insert retry "
                f"{attempt + 1}/{retries} "
                f"after {delay:.1f}s | {exc}",
                file=sys.stderr,
                flush=True,
            )

            time.sleep(delay)

        finally:

            if cursor is not None:
                try:
                    cursor.close()

                except Exception:
                    pass

    raise RuntimeError(
        "MSSQL cycle insert failed "
        f"(cycle={absolute_cycle}): "
        f"{last_error}"
    )


# ============================================================
# InfluxDB batch generation
# ============================================================


def make_batch(
    start: int,
    count: int,
    cfg: Config,
) -> bytes:
    """Generate one chronological InfluxDB line-protocol batch."""

    try:
        lines: list[str] = []

        append_line = (
            lines.append
        )

        measurement = (
            escape_measurement(
                cfg.measurement
            )
        )

        cached_value_key: (
            tuple[int, int]
            | None
        ) = None

        cached_measurement_fields = ""

        cached_qrcode_cycle: (
            int
            | None
        ) = None

        cached_qrcodes: (
            tuple[str, ...]
            | None
        ) = None

        device_count = len(
            cfg.devices
        )

        history_start_seconds = (
            cfg.timestamp_start_ns
            // NS_PER_SECOND
        )

        for point_index in range(
            start,
            start
            + count,
        ):

            (
                second_index,
                device_index,
            ) = divmod(
                point_index,
                device_count,
            )

            device = (
                cfg.devices[
                    device_index
                ]
            )

            # --------------------------------------------
            # Cache measurement values per pallet/second.
            # --------------------------------------------

            value_key = (
                second_index,
                device.pallet_number,
            )

            if (
                value_key
                != cached_value_key
            ):

                (
                    voltage,
                    temperature,
                    current,
                    wire_voltage,
                ) = esp_values(
                    second_index,
                    device.pallet_number,
                    cfg,
                )

                cached_measurement_fields = (
                    f"voltage="
                    f"{voltage:.3f},"

                    f"temperature="
                    f"{temperature:.1f},"

                    f"current="
                    f"{current:.1f},"

                    'error_code="OK",'

                    'wire_voltage_status="OK",'

                    "fetstate=1i,"

                    "fuse=0i,"

                    'afe="0x12",'

                    "wifistdisconn=0i,"

                    "wifiltdisconn=0i,"

                    "socketstdisconn=0i,"

                    "socketltdisconn=0i,"

                    "heartbeattxcount=10i,"

                    "heartbeatlosscount=0i,"

                    "heartbeatlastrttms=32i,"

                    "heartbeattrttmaxms=40i,"

                    "heatbeattimeoutflag=0i,"

                    "wifireconnlastms=0i,"

                    "wifireconnavgms=0i,"

                    "wifireconntimes=0i,"

                    "socketreconnlastms=0i,"

                    "socketreconnavgms=0i,"

                    "socketreconnmaxms=0i,"

                    "socketreconntimes=0i,"

                    'error_code_curr="OK",'

                    f"wire_voltage_1="
                    f"{wire_voltage[0]}i,"

                    f"wire_voltage_2="
                    f"{wire_voltage[1]}i,"

                    f"wire_voltage_3="
                    f"{wire_voltage[2]}i,"

                    f"wire_voltage_4="
                    f"{wire_voltage[3]}i,"

                    f"wire_voltage_5="
                    f"{wire_voltage[4]}i"
                )

                cached_value_key = (
                    value_key
                )

            # --------------------------------------------
            # Resolve QRCode for current 3-hour cycle.
            # --------------------------------------------

            timestamp_seconds = (
                history_start_seconds
                + second_index
            )

            absolute_cycle = (
                get_absolute_cycle(
                    timestamp_seconds
                )
            )

            if (
                absolute_cycle
                != cached_qrcode_cycle
            ):

                cached_qrcodes = (
                    build_cycle_qrcodes(
                        absolute_cycle,
                        cfg,
                    )
                )

                cached_qrcode_cycle = (
                    absolute_cycle
                )

            if cached_qrcodes is None:
                raise RuntimeError(
                    "QRCode cache was not initialized"
                )

            qrcode = (
                cached_qrcodes[
                    device_index
                ]
            )

            # --------------------------------------------
            # InfluxDB tags
            # --------------------------------------------

            tags = (
                f"{device.tag_prefix}"
                f"return_code="
                f"{escape_tag(cfg.return_code)}"
            )

            # --------------------------------------------
            # InfluxDB fields
            # --------------------------------------------

            identity_fields = (
                f'qrcode="'
                f'{escape_field_string(qrcode)}",'

                f"{device.field_prefix}"
            )

            timestamp_ns = (
                cfg.timestamp_start_ns
                + second_index
                * NS_PER_SECOND
            )

            line = (
                f"{measurement},"
                f"{tags} "
                f"{identity_fields}"
                f"{cached_measurement_fields} "
                f"{timestamp_ns}"
            )

            append_line(
                line
            )

        return (
            "\n".join(
                lines
            )
            + "\n"
        ).encode(
            "utf-8"
        )

    except Exception as exc:
        raise RuntimeError(
            "Batch generation failed "
            f"(start={start}, "
            f"count={count}): "
            f"{exc}"
        ) from exc


# ============================================================
# InfluxDB write
# ============================================================


def write_batch(
    start: int,
    count: int,
    cfg: Config,
) -> tuple[int, int]:
    """Generate and write one batch to InfluxDB."""

    try:
        body = (
            make_batch(
                start,
                count,
                cfg,
            )
        )

        if cfg.gzip_body:

            body = (
                gzip.compress(
                    body,
                    compresslevel=1,
                )
            )

        query = (
            urllib.parse.urlencode(
                {
                    "org":
                        cfg.org,

                    "bucket":
                        cfg.bucket,

                    "precision":
                        "ns",
                }
            )
        )

        endpoint = (
            f"{cfg.url.rstrip('/')}"
            f"/api/v2/write?"
            f"{query}"
        )

        headers = {
            "Authorization":
                f"Token {cfg.token}",

            "Content-Type":
                "text/plain; "
                "charset=utf-8",

            "Accept":
                "application/json",
        }

        if cfg.gzip_body:

            headers[
                "Content-Encoding"
            ] = "gzip"

        last_error: Exception | None = None

        for attempt in range(
            cfg.retries
            + 1
        ):

            request = (
                urllib.request.Request(
                    endpoint,
                    data=body,
                    headers=headers,
                    method="POST",
                )
            )

            try:
                with urllib.request.urlopen(
                    request,
                    timeout=cfg.timeout,
                ) as response:

                    if (
                        response.status
                        != 204
                    ):
                        raise RuntimeError(
                            "Unexpected "
                            "InfluxDB HTTP status "
                            f"{response.status}"
                        )

                return (
                    count,
                    len(
                        body
                    ),
                )

            except urllib.error.HTTPError as exc:

                last_error = exc

                retryable = (
                    exc.code
                    == 429
                    or exc.code
                    >= 500
                )

                detail = (
                    exc.read(
                        1000
                    )
                    .decode(
                        "utf-8",
                        errors="replace",
                    )
                )

                if (
                    not retryable
                    or attempt
                    >= cfg.retries
                ):

                    raise RuntimeError(
                        f"InfluxDB HTTP "
                        f"{exc.code}: "
                        f"{detail}"
                    ) from exc

            except (
                urllib.error.URLError,
                TimeoutError,
            ) as exc:

                last_error = exc

                if (
                    attempt
                    >= cfg.retries
                ):
                    raise RuntimeError(
                        f"InfluxDB write failed: "
                        f"{exc}"
                    ) from exc

            delay = (
                min(
                    30.0,
                    0.5
                    * (
                        2
                        ** attempt
                    ),
                )
                + random.random()
                * 0.25
            )

            time.sleep(
                delay
            )

        raise RuntimeError(
            f"InfluxDB retry loop failed: "
            f"{last_error}"
        )

    except Exception as exc:
        raise RuntimeError(
            "InfluxDB batch write failed "
            f"(start={start}, "
            f"count={count}): "
            f"{exc}"
        ) from exc


# ============================================================
# Cycle segmentation
# ============================================================


def get_cycle_segment_stop(
    current_index: int,
    final_stop: int,
    cfg: Config,
) -> tuple[int, int]:
    """Return the current cycle and its ending point index."""

    device_count = (
        len(
            cfg.devices
        )
    )

    second_index = (
        current_index
        // device_count
    )

    history_start_seconds = (
        cfg.timestamp_start_ns
        // NS_PER_SECOND
    )

    timestamp_seconds = (
        history_start_seconds
        + second_index
    )

    absolute_cycle = (
        get_absolute_cycle(
            timestamp_seconds
        )
    )

    cycle_start_seconds = (
        get_cycle_start_utc_seconds(
            absolute_cycle
        )
    )

    cycle_end_seconds = (
        cycle_start_seconds
        + SECONDS_PER_CYCLE
    )

    seconds_until_cycle_end = (
        cycle_end_seconds
        - history_start_seconds
    )

    cycle_end_point_index = (
        seconds_until_cycle_end
        * device_count
    )

    segment_stop = min(
        final_stop,
        cycle_end_point_index,
    )

    if (
        segment_stop
        <= current_index
    ):
        raise RuntimeError(
            "Invalid cycle segmentation: "
            f"current={current_index}, "
            f"stop={segment_stop}"
        )

    return (
        absolute_cycle,
        segment_stop,
    )


# ============================================================
# Timestamp helpers
# ============================================================


def parse_timestamp(
    value: str,
) -> datetime:
    """Parse an ISO-8601 timestamp and return UTC."""

    try:
        parsed = (
            datetime.fromisoformat(
                value.replace(
                    "Z",
                    "+00:00",
                )
            )
        )

        if (
            parsed.tzinfo
            is None
        ):

            parsed = (
                parsed.replace(
                    tzinfo=timezone.utc
                )
            )

        return (
            parsed
            .astimezone(
                timezone.utc
            )
            .replace(
                microsecond=0
            )
        )

    except Exception as exc:
        raise RuntimeError(
            f"Invalid timestamp "
            f"'{value}': {exc}"
        ) from exc


def years_before(
    value: datetime,
    years: int,
) -> datetime:
    """Return the same date and time a number of years earlier."""

    try:
        return (
            value.replace(
                year=(
                    value.year
                    - years
                )
            )
        )

    except ValueError:

        return (
            value.replace(
                year=(
                    value.year
                    - years
                ),
                day=28,
            )
        )


# ============================================================
# Arguments
# ============================================================


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = (
        argparse.ArgumentParser(
            description=(
                "Generate CCMS data "
                "for MSSQL and "
                "InfluxDB 2.x"
            )
        )
    )

    # --------------------------------------------------------
    # History range
    # --------------------------------------------------------

    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help=(
            "Maximum number of "
            "InfluxDB points to write."
        ),
    )

    parser.add_argument(
        "--start-index",
        type=int,
        default=0,
        help=(
            "Point index used to "
            "resume a previous run."
        ),
    )

    parser.add_argument(
        "--years",
        type=int,
        default=2,
        help=(
            "Generate this many "
            "calendar years of history."
        ),
    )

    parser.add_argument(
        "--start-time",
        default=None,
        help=(
            "Inclusive history start (ISO-8601). Use +08:00 for Taipei; "
            "timestamps without an offset are UTC."
        ),
    )

    parser.add_argument(
        "--end-time",
        default=None,
        help=(
            "Exclusive history end (ISO-8601). Use +08:00 for Taipei; "
            "timestamps without an offset are UTC. "
            "Default is current UTC time."
        ),
    )

    parser.add_argument(
        "--end-extension-days",
        type=int,
        default=0,
        help="Extend explicit --end-time by this many whole days.",
    )

    parser.add_argument(
        "--qrcode-start-time",
        default=None,
        help=(
            "Original run's history start (ISO-8601), preserving cycle QRCode "
            "numbering when resuming with a later --start-time. "
            "Defaults to --start-time."
        ),
    )

    # --------------------------------------------------------
    # Performance
    # --------------------------------------------------------

    parser.add_argument(
        "--batch-size",
        type=int,
        default=50_000,
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=4,
    )

    parser.add_argument(
        "--worker-mode",
        choices=(
            "process",
            "thread",
        ),
        default="process",
    )

    parser.add_argument(
        "--cpu-target",
        type=float,
        default=None,
    )

    # --------------------------------------------------------
    # InfluxDB
    # --------------------------------------------------------

    parser.add_argument(
        "--url",
        default=os.getenv(
            "INFLUXDB_URL",
            "http://localhost:8086",
        ),
    )

    parser.add_argument(
        "--token",
        default=os.getenv(
            "INFLUXDB_TOKEN",
            "1asCuXLjSMF2sTgYx-ukZ3zagQ-PdBS3dsFWwdoAbtctbmwlTRz0ERqXV0rXsJlASuG7lD7xX01vje-Ot1btzQ=="
        ),
    )

    parser.add_argument(
        "--org",
        default=os.getenv(
            "INFLUXDB_ORG",
            "delta",
        ),
    )

    parser.add_argument(
        "--bucket",
        default=os.getenv(
            "INFLUXDB_BUCKET",
            "StoreHouseMeasData",
        ),
    )

    parser.add_argument(
        "--measurement",
        default=(
            "meas_data"
        ),
    )

    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
    )

    parser.add_argument(
        "--retries",
        type=int,
        default=5,
    )

    parser.add_argument(
        "--no-gzip",
        action="store_true",
    )

    # --------------------------------------------------------
    # MSSQL
    # --------------------------------------------------------

    parser.add_argument(
        "--mssql-server",
        default=os.getenv(
            "MSSQL_SERVER",
            "localhost",
        ),
    )

    parser.add_argument(
        "--mssql-port",
        type=int,
        default=int(
            os.getenv(
                "MSSQL_PORT",
                "1433",
            )
        ),
    )

    parser.add_argument(
        "--mssql-database",
        default=os.getenv(
            "MSSQL_DATABASE",
            "BALPS",
        ),
    )

    parser.add_argument(
        "--mssql-user",
        default=os.getenv(
            "MSSQL_USER",
            "test"
        ),
    )

    parser.add_argument(
        "--mssql-password",
        default=os.getenv(
            "MSSQL_PASSWORD",
            "11111111"
        ),
    )

    parser.add_argument(
        "--mssql-driver",
        default=os.getenv(
            "MSSQL_DRIVER",
            "ODBC Driver 17 for SQL Server",
        ),
    )

    parser.add_argument(
        "--mssql-trusted",
        action="store_true",
        help=(
            "Use Windows integrated "
            "authentication."
        ),
    )

    parser.add_argument(
        "--mssql-trust-cert",
        action="store_true",
        help=(
            "Trust the SQL Server "
            "TLS certificate."
        ),
    )

    parser.add_argument(
        "--mssql-timeout",
        type=int,
        default=30,
    )

    parser.add_argument(
        "--mssql-retries",
        type=int,
        default=3,
    )

    # --------------------------------------------------------
    # Mapping / simulation
    # --------------------------------------------------------

    parser.add_argument(
        "--servermap",
        type=Path,
        default=DEFAULT_SERVERMAP,
    )

    parser.add_argument(
        "--qrcode-mode",
        choices=(
            "cycle",
            "servermap",
        ),
        default="cycle",
    )

    parser.add_argument(
        "--qrcode-width",
        type=int,
        default=None,
    )

    parser.add_argument(
        "--return-code",
        default=(
            DEFAULT_RETURN_CODE
        ),
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=20250818,
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
    )

    parser.add_argument(
        "--confirm-large-run",
        action="store_true",
    )

    return (
        parser.parse_args()
    )


# ============================================================
# Validation
# ============================================================


def validate_arguments(
    args: argparse.Namespace,
    count: int,
    available_points: int,
) -> None:
    """Validate program arguments."""

    if (
        args.batch_size
        <= 0
    ):
        raise RuntimeError(
            "--batch-size must be greater than zero"
        )

    if (
        args.workers
        <= 0
    ):
        raise RuntimeError(
            "--workers must be greater than zero"
        )

    if (
        args.years
        <= 0
    ):
        raise RuntimeError(
            "--years must be greater than zero"
        )

    if count <= 0:
        raise RuntimeError(
            "--count must be greater than zero"
        )

    if (
        args.start_index
        < 0
    ):
        raise RuntimeError(
            "--start-index cannot be negative"
        )

    if (
        args.start_index
        + count
        > available_points
    ):
        raise RuntimeError(
            "Requested point range exceeds "
            "the configured history range."
        )

    if (
        args.qrcode_width
        is not None
        and args.qrcode_width
        <= 0
    ):
        raise RuntimeError(
            "--qrcode-width must be greater than zero"
        )

    if (
        args.cpu_target
        is not None
        and not (
            1.0
            <= args.cpu_target
            <= 100.0
        )
    ):
        raise RuntimeError(
            "--cpu-target must be between 1 and 100"
        )

    if (
        count
        >= LARGE_RUN
        and not args.confirm_large_run
        and not args.dry_run
    ):
        raise RuntimeError(
            "Runs with >= 1 billion points require "
            "--confirm-large-run."
        )

    if (
        not args.dry_run
        and not args.token
    ):
        raise RuntimeError(
            "InfluxDB token is required. "
            "Set INFLUXDB_TOKEN or use --token."
        )

    if (
        not args.dry_run
        and not args.mssql_trusted
        and not args.mssql_user
    ):
        raise RuntimeError(
            "MSSQL authentication is required. "
            "Use --mssql-user / --mssql-password "
            "or --mssql-trusted."
        )


# ============================================================
# Main
# ============================================================


def main() -> int:
    """Run the complete MSSQL and InfluxDB data generator."""

    mssql_connection: (
        pyodbc.Connection
        | None
    ) = None

    try:
        args = (
            parse_args()
        )

        # ----------------------------------------------------
        # ServerMap
        # ----------------------------------------------------

        print(
            "Loading ServerMap...",
            flush=True,
        )

        devices = (
            load_servermap(
                args.servermap
            )
        )

        device_count = (
            len(
                devices
            )
        )

        print(
            f"Loaded "
            f"{device_count:,} "
            f"device mappings.",
            flush=True,
        )

        # ----------------------------------------------------
        # Date range
        # ----------------------------------------------------

        if args.end_extension_days < 0:
            raise RuntimeError("--end-extension-days cannot be negative")
        if args.end_extension_days and not args.end_time:
            raise RuntimeError("--end-extension-days requires --end-time")

        if args.end_time:

            end_time = (
                parse_timestamp(
                    args.end_time
                )
            )

        else:

            end_time = (
                datetime.now(
                    timezone.utc
                )
                .replace(
                    microsecond=0
                )
            )

        end_time += timedelta(days=args.end_extension_days)

        if args.start_time:

            start_time = (
                parse_timestamp(
                    args.start_time
                )
            )

        else:

            start_time = (
                years_before(
                    end_time,
                    args.years,
                )
            )

        if (
            start_time
            >= end_time
        ):
            raise RuntimeError(
                "--start-time must be "
                "earlier than --end-time."
            )

        history_seconds = int(
            (
                end_time
                - start_time
            ).total_seconds()
        )

        available_points = (
            history_seconds
            * device_count
        )

        if (
            args.count
            is None
        ):

            count = (
                available_points
                - args.start_index
            )

        else:

            count = (
                args.count
            )

        validate_arguments(
            args,
            count,
            available_points,
        )

        # ----------------------------------------------------
        # QRCode base format
        # ----------------------------------------------------

        qrcode_match = (
            re.fullmatch(
                r"(.*?)(\d+)",
                devices[0].qrcode,
            )
        )

        if (
            qrcode_match
            is None
        ):
            raise RuntimeError(
                "The first QRCODEID in ServerMap "
                "must end with a numeric suffix. "
                f"Current value: "
                f"{devices[0].qrcode}"
            )

        (
            qrcode_prefix,
            qrcode_suffix,
        ) = (
            qrcode_match.groups()
        )

        qrcode_width = (
            args.qrcode_width
            if (
                args.qrcode_width
                is not None
            )
            else len(
                qrcode_suffix
            )
        )

        start_timestamp_seconds = int(
            start_time.timestamp()
        )

        qrcode_start_time = (
            parse_timestamp(args.qrcode_start_time)
            if args.qrcode_start_time else start_time
        )
        if qrcode_start_time > start_time:
            raise RuntimeError("--qrcode-start-time must not be later than --start-time")

        # ----------------------------------------------------
        # Runtime config
        # ----------------------------------------------------

        cfg = Config(
            url=args.url,
            token=(
                args.token
                or "dry-run"
            ),
            org=args.org,
            bucket=args.bucket,
            measurement=args.measurement,
            timeout=args.timeout,
            retries=args.retries,
            gzip_body=(
                not args.no_gzip
            ),
            timestamp_start_ns=(
                start_timestamp_seconds
                * NS_PER_SECOND
            ),
            seed=args.seed,
            devices=devices,
            qrcode_mode=(
                args.qrcode_mode
            ),
            qrcode_prefix=(
                qrcode_prefix
            ),
            qrcode_width=(
                qrcode_width
            ),
            history_start_cycle=(
                get_absolute_cycle(
                    int(qrcode_start_time.timestamp())
                )
            ),
            return_code=(
                args.return_code
            ),
        )

        print(
            f"History range : "
            f"{start_time.isoformat()} "
            f"-> "
            f"{end_time.isoformat()}"
        )

        print(
            f"Devices       : "
            f"{device_count:,}"
        )

        taipei = timezone(timedelta(seconds=TAIPEI_UTC_OFFSET_SECONDS))
        print(
            f"Taipei range  : {start_time.astimezone(taipei).isoformat()} "
            f"-> {end_time.astimezone(taipei).isoformat()} (end exclusive)"
        )
        print(f"History seconds: {history_seconds:,}")
        print(f"QRCode origin : {qrcode_start_time.isoformat()}")

        print(
            f"Points/sec    : "
            f"{device_count:,}"
        )

        print(
            f"Total points  : "
            f"{count:,}"
        )

        print(
            f"QRCode mode   : "
            f"{args.qrcode_mode}"
        )

        # ====================================================
        # Dry run
        # ====================================================

        if args.dry_run:

            sample_count = min(
                args.batch_size,
                count,
            )

            started = (
                time.perf_counter()
            )

            payload = (
                make_batch(
                    args.start_index,
                    sample_count,
                    cfg,
                )
            )

            elapsed = (
                time.perf_counter()
                - started
            )

            first_second_index = (
                args.start_index
                // device_count
            )

            first_timestamp = (
                start_timestamp_seconds
                + first_second_index
            )

            first_cycle = (
                get_absolute_cycle(
                    first_timestamp
                )
            )

            sql_rows = (
                build_cycle_index_rows(
                    first_cycle,
                    cfg,
                )
            )

            print()
            print(
                "========== DRY RUN =========="
            )

            print(
                f"Generated "
                f"{sample_count:,} "
                f"InfluxDB points "
                f"in {elapsed:.3f}s"
            )

            print()

            print(
                "First InfluxDB line:"
            )

            print(
                payload
                .splitlines()[0]
                .decode(
                    "utf-8"
                )
            )

            print()

            print(
                "First MSSQL row:"
            )

            print(
                sql_rows[0]
            )

            print()
            print(
                "No database was modified."
            )

            return 0

        # ====================================================
        # MSSQL
        # ====================================================

        mssql_cfg = (
            MssqlConfig(
                server=(
                    args.mssql_server
                ),
                port=(
                    args.mssql_port
                ),
                database=(
                    args.mssql_database
                ),
                username=(
                    args.mssql_user
                ),
                password=(
                    args.mssql_password
                ),
                driver=(
                    args.mssql_driver
                ),
                trusted_connection=(
                    args.mssql_trusted
                ),
                trust_server_certificate=(
                    args.mssql_trust_cert
                ),
                timeout=(
                    args.mssql_timeout
                ),
                retries=(
                    args.mssql_retries
                ),
            )
        )

        print()
        print(
            "Connecting to MSSQL...",
            flush=True,
        )

        mssql_connection = (
            connect_mssql(
                mssql_cfg
            )
        )

        print(
            "MSSQL connected.",
            flush=True,
        )

        print(
            "Checking MSSQL schema...",
            flush=True,
        )

        ensure_mssql_schema(
            mssql_connection
        )

        print(
            "MSSQL schema ready.",
            flush=True,
        )

        # ====================================================
        # Executor
        # ====================================================

        executor_class = (
            ProcessPoolExecutor
            if (
                args.worker_mode
                == "process"
            )
            else ThreadPoolExecutor
        )

        cpu_monitor = (
            SystemCpuMonitor()
            if (
                args.cpu_target
                is not None
            )
            else None
        )

        first_point = (
            args.start_index
        )

        final_stop = (
            first_point
            + count
        )

        current_index = (
            first_point
        )

        completed = 0

        transferred = 0

        started = (
            time.perf_counter()
        )

        print()
        print(
            "Starting data generation..."
        )

        print(
            f"InfluxDB      : "
            f"{cfg.url}"
        )

        print(
            f"Bucket        : "
            f"{cfg.bucket}"
        )

        print(
            f"Measurement   : "
            f"{cfg.measurement}"
        )

        print(
            f"MSSQL Server  : "
            f"{args.mssql_server}:"
            f"{args.mssql_port}"
        )

        print(
            f"MSSQL Database: "
            f"{args.mssql_database}"
        )

        print(
            f"Batch Size    : "
            f"{args.batch_size:,}"
        )

        print(
            f"Workers       : "
            f"{args.workers}"
        )

        print()

        # ====================================================
        # Write cycle by cycle
        # ====================================================

        with executor_class(
            max_workers=(
                args.workers
            )
        ) as executor:

            while (
                current_index
                < final_stop
            ):

                (
                    absolute_cycle,
                    cycle_stop,
                ) = (
                    get_cycle_segment_stop(
                        current_index,
                        final_stop,
                        cfg,
                    )
                )

                cycle_start_seconds = (
                    get_cycle_start_utc_seconds(
                        absolute_cycle
                    )
                )

                cycle_start_time = (
                    datetime.fromtimestamp(
                        cycle_start_seconds,
                        tz=timezone.utc,
                    )
                )

                cycle_end_time = (
                    datetime.fromtimestamp(
                        cycle_start_seconds
                        + SECONDS_PER_CYCLE,
                        tz=timezone.utc,
                    )
                )

                print()
                print(
                    "----------------------------------------"
                )

                print(
                    f"Cycle        : "
                    f"{absolute_cycle}"
                )

                print(
                    f"Cycle UTC    : "
                    f"{cycle_start_time.isoformat()} "
                    f"-> "
                    f"{cycle_end_time.isoformat()}"
                )

                # ============================================
                # Step 1: MSSQL
                # ============================================

                print(
                    "Writing MSSQL index...",
                    flush=True,
                )

                inserted_rows = (
                    insert_cycle_index_mssql(
                        mssql_connection,
                        absolute_cycle,
                        cfg,
                        retries=(
                            args.mssql_retries
                        ),
                    )
                )

                print(
                    f"MSSQL index ready: "
                    f"{inserted_rows:,} "
                    f"new rows"
                )

                # ============================================
                # Step 2: InfluxDB
                # ============================================

                next_index = (
                    current_index
                )

                pending = set()

                pending_limit = max(
                    args.workers,
                    args.workers
                    * 2,
                )

                while (
                    next_index
                    < cycle_stop
                    or pending
                ):

                    while (
                        next_index
                        < cycle_stop
                        and len(
                            pending
                        )
                        < pending_limit
                    ):

                        batch_count = min(
                            args.batch_size,
                            cycle_stop
                            - next_index,
                        )

                        future = (
                            executor.submit(
                                write_batch,
                                next_index,
                                batch_count,
                                cfg,
                            )
                        )

                        pending.add(
                            future
                        )

                        next_index += (
                            batch_count
                        )

                    if not pending:
                        continue

                    (
                        done,
                        pending,
                    ) = wait(
                        pending,
                        return_when=(
                            FIRST_COMPLETED
                        ),
                    )

                    for future in done:

                        (
                            written_points,
                            byte_count,
                        ) = (
                            future.result()
                        )

                        completed += (
                            written_points
                        )

                        transferred += (
                            byte_count
                        )

                    # ----------------------------------------
                    # Optional CPU throttling
                    # ----------------------------------------

                    cpu_text = ""

                    if (
                        cpu_monitor
                        is not None
                    ):

                        (
                            current_cpu,
                            sample_interval,
                        ) = (
                            cpu_monitor.sample()
                        )

                        cpu_text = (
                            f" | CPU "
                            f"{current_cpu:.1f}%/"
                            f"{args.cpu_target:.0f}%"
                        )

                        if (
                            current_cpu
                            > args.cpu_target
                        ):

                            throttle_delay = (
                                sample_interval
                                * (
                                    current_cpu
                                    / args.cpu_target
                                    - 1.0
                                )
                            )

                            time.sleep(
                                min(
                                    max(
                                        throttle_delay,
                                        0.01,
                                    ),
                                    2.0,
                                )
                            )

                    elapsed = max(
                        time.perf_counter()
                        - started,
                        1e-9,
                    )

                    rate = (
                        completed
                        / elapsed
                    )

                    bandwidth = (
                        transferred
                        / elapsed
                        / 1_048_576
                    )

                    print(
                        f"\r"
                        f"{completed:,}/"
                        f"{count:,} "
                        f"("
                        f"{completed / count:.2%}"
                        f") "
                        f"| "
                        f"{rate:,.0f} "
                        f"points/s "
                        f"| "
                        f"{bandwidth:.1f} "
                        f"MiB/s"
                        f"{cpu_text}",
                        end="",
                        flush=True,
                    )

                # Current cycle has completed.
                current_index = (
                    cycle_stop
                )

        # ====================================================
        # Complete
        # ====================================================

        elapsed = max(
            time.perf_counter()
            - started,
            1e-9,
        )

        print()
        print()
        print(
            "========== COMPLETE =========="
        )

        print(
            f"Influx points : "
            f"{completed:,}"
        )

        print(
            f"Elapsed       : "
            f"{elapsed:.2f}s"
        )

        print(
            f"Average rate  : "
            f"{completed / elapsed:,.0f} "
            f"points/s"
        )

        print(
            f"Transferred   : "
            f"{transferred / 1_048_576:.1f} "
            f"MiB"
        )

        return 0

    except KeyboardInterrupt:

        print(
            "\nExecution interrupted by user.",
            file=sys.stderr,
        )

        return 130

    except Exception as exc:

        print()
        print(
            f"ERROR: {exc}",
            file=sys.stderr,
        )

        return 1

    finally:

        if (
            mssql_connection
            is not None
        ):

            try:
                mssql_connection.close()

                print(
                    "MSSQL connection closed."
                )

            except Exception:
                pass


# ============================================================
# Entry point
# ============================================================


if __name__ == "__main__":
    raise SystemExit(
        main()
    )
