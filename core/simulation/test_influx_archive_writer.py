from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import influx_archive_writer as writer


class FakeRecord:
    def __init__(self, values):
        self.values = values

    def get_time(self):
        return self.values.get("_time")


def make_config(checkpoint_path: Path) -> writer.ArchiveConfig:
    return writer.ArchiveConfig(
        source_url="http://localhost:8086",
        source_token="source",
        source_org="delta",
        source_bucket="StoreHouseMeasData",
        destination_url="http://localhost:8087",
        destination_token="destination",
        destination_org="delta",
        destination_bucket="StoreHouseMeasArchive",
        measurement="meas_data",
        tag_columns=writer.DEFAULT_TAG_COLUMNS,
        interval_seconds=5,
        chunk_seconds=60,
        overlap_seconds=10,
        safety_delay_seconds=10,
        poll_seconds=5,
        write_batch_size=5000,
        timeout_seconds=120,
        retries=5,
        gzip_body=True,
        checkpoint_path=checkpoint_path,
        dry_run=False,
    )


class ArchiveWriterTests(unittest.TestCase):
    def test_floor_time_uses_five_second_boundary(self):
        value = datetime(2026, 8, 24, 10, 1, 13, 456789, tzinfo=timezone.utc)
        self.assertEqual(
            writer.floor_time(value, 5),
            datetime(2026, 8, 24, 10, 1, 10, tzinfo=timezone.utc),
        )

    def test_record_to_line_preserves_tags_fields_and_timestamp(self):
        config = make_config(Path("checkpoint.json"))
        record = FakeRecord(
            {
                "result": "_result",
                "table": 0,
                "_measurement": "meas_data",
                "_time": datetime(2026, 8, 24, 10, 1, 15, tzinfo=timezone.utc),
                "storehouse_id": "1",
                "pallet_position": "L",
                "serialboard_id": "SB 1",
                "protectboard_id": "PB,1",
                "position_id": "0",
                "qrcode": "03O800222N0001",
                "return_code": 0,
                "voltage": 4.085,
                "temperature": 24.5,
                "fetstate": 1,
                "error_code": 'A"B',
            }
        )
        line = writer.record_to_line(record, config)
        self.assertIsNotNone(line)
        assert line is not None
        self.assertIn("serialboard_id=SB\\ 1", line)
        self.assertIn("protectboard_id=PB\\,1", line)
        self.assertIn("return_code=0", line)
        self.assertIn("fetstate=1i", line)
        self.assertIn('error_code=\"A\\\"B\"', line)
        self.assertTrue(line.endswith("1787565675000000000"))

    def test_checkpoint_round_trip_and_identity_validation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "checkpoint.json"
            config = make_config(path)
            stop = datetime(2026, 8, 24, 10, 1, 15, tzinfo=timezone.utc)
            writer.save_checkpoint(path, config, stop)
            self.assertEqual(writer.load_checkpoint(path, config), stop)

            document = json.loads(path.read_text(encoding="utf-8"))
            document["identity"]["destination_bucket"] = "other"
            path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaises(writer.CheckpointError):
                writer.load_checkpoint(path, config)

    def test_flux_query_downsamples_before_pivot(self):
        config = make_config(Path("checkpoint.json"))
        start = datetime(2026, 8, 24, 10, 0, tzinfo=timezone.utc)
        stop = datetime(2026, 8, 24, 10, 1, tzinfo=timezone.utc)
        query = writer.build_flux_query(config, start, stop)
        self.assertIn('from(bucket: "StoreHouseMeasData")', query)
        self.assertIn("aggregateWindow(every: 5s, fn: last", query)
        self.assertLess(query.index("aggregateWindow"), query.index("pivot"))


if __name__ == "__main__":
    unittest.main()
