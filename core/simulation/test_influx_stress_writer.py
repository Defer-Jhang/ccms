"""Offline checks for time ranges and resumed deterministic data."""

import contextlib
import io
import sys
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
try:
    import pyodbc
except ModuleNotFoundError:
    # These tests never connect to SQL Server.
    with patch.dict(sys.modules, {"pyodbc": Mock()}):
        import influx_stress_writer as writer
else:
    import influx_stress_writer as writer


class TimeRangeTests(unittest.TestCase):
    def run_preview(self, *arguments):
        output = io.StringIO()
        with (
            patch.object(sys, "argv", ["writer", "--dry-run", "--batch-size", "2", *arguments]),
            patch.object(writer, "make_batch", wraps=writer.make_batch) as batch,
            patch.object(writer, "connect_mssql") as connect,
            contextlib.redirect_stdout(output),
            contextlib.redirect_stderr(output),
        ):
            result = writer.main()
        connect.assert_not_called()
        return result, output.getvalue(), batch

    def test_resume_preserves_data_and_qrcodes(self):
        result, output, batch = self.run_preview(
            "--start-time", "2025-06-17 00:00:00.000+08:00",
            "--end-time", "2025-06-18 00:00:00+08:00",
            "--end-extension-days", "1",
            "--qrcode-start-time", "2023-06-01T00:00:00+08:00",
        )
        self.assertEqual(result, 0, output)
        self.assertIn("2025-06-19T00:00:00+08:00 (end exclusive)", output)
        cfg = batch.call_args.args[2]
        self.assertIn(f"Total points  : {172800 * len(cfg.devices):,}", output)
        original_ns = int(writer.parse_timestamp("2023-06-01T00:00:00+08:00").timestamp()) * writer.NS_PER_SECOND
        original = replace(cfg, timestamp_start_ns=original_ns)
        offset = (cfg.timestamp_start_ns - original_ns) // writer.NS_PER_SECOND * len(cfg.devices)
        self.assertEqual(writer.make_batch(0, 2, cfg), writer.make_batch(offset, 2, original))
        final_index = 172800 * len(cfg.devices) - 1
        last_line = writer.make_batch(final_index, 1, cfg).splitlines()[0]
        self.assertEqual(int(last_line.rsplit(b" ", 1)[1]), cfg.timestamp_start_ns + 172799 * writer.NS_PER_SECOND)

    def test_invalid_ranges(self):
        for arguments in (
            ["--end-extension-days", "1"],
            ["--end-extension-days", "-1"],
            ["--start-time", "2025-06-17", "--end-time", "2025-06-16"],
            ["--start-time", "2025-06-17", "--end-time", "2025-06-18", "--qrcode-start-time", "2025-06-18"],
        ):
            with self.subTest(arguments=arguments):
                result, output, batch = self.run_preview(*arguments)
                self.assertEqual(result, 1, output)
                batch.assert_not_called()

    def test_utc_default_unchanged(self):
        self.assertEqual(
            writer.parse_timestamp("2025-06-16 16:00:00"),
            writer.parse_timestamp("2025-06-17 00:00:00.000+08:00"),
        )


if __name__ == "__main__":
    unittest.main()
