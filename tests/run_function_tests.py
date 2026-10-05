"""Run CCMS function tests with compact, human-readable case names."""

from __future__ import annotations

import sys
import logging
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]

# Test selection list.
# [] means run every discovered test. Add a short name or a test function name
# to run only those cases, for example:
# ENABLED_CASES = ["W1001->W2005->W2004", "C03 W2004 OK/NG/Water"]
# Keep every case listed here so the run scope is visible at a glance. Remove
# any entry when a focused run is needed; an empty list still means all cases.
ENABLED_CASES = [
    'MAP-01 mapping completeness',
    'MAP-02 QRCode identity',
    'MAP-03 pair moves Storehouse',
    'MAP-04 duplicate serial',
    'MAP-05 arbitrary pairs',
    'C01-4 MPD heartbeat 60s',
    'C01-3 both tasks ready',
    'C01-1/C01-2 W0001/W0002 XML',
    'C01-1 time boundary/invalid',
    'C01-5 ESP reconnect recovery',
    'C01-5 invalid recovery checkpoint',
    'C01-5 recovery validation NG',
    'C02-1 key mismatch reject',
    'C02-1 15-step CC/REST',
    'C02-1 W2002 recipe',
    'C02-2 W2003 REST/CC/END',
    'C03 W2004 OK/NG/Water',
    'C04 W2004 NoResponse',
    'C05 W2005 OK/NG',
    'C06 2-rack/2-round',
    'C07 wire/delay thresholds',
    'C07-1..13 thresholds',
    'Protect known empty value',
    'Protect config mismatch cases',
    'Protect duplicate XML key',
    'Protect duplicate values',
    'Protect complete keys',
    'Protect XML mismatch cases',
    'RecipeStep 2 mismatch',
    'Protect key order',
    'unknown message rejected',
    'W1001 30s NG->Water',
    'W1001->W2005->W2004',
    'W2004 stale status',
    'E2E W2002 ACK',
    'E2E DATA->W2004',
    'E2E ALARM->W1001',
    'E2E NG->W2005->W2004',
    'E2E W1001 NG->Water',
]


class CompactTextTestResult(unittest.TextTestResult):
    """Print the short test description instead of the long Python method id."""

    def getDescription(self, test):
        return str(test)


class CompactTextTestRunner(unittest.TextTestRunner):
    resultclass = CompactTextTestResult


def iter_tests(suite):
    for test in suite:
        if isinstance(test, unittest.TestSuite):
            yield from iter_tests(test)
        else:
            yield test


def select_tests(suite):
    if not ENABLED_CASES:
        return suite
    wanted = set(ENABLED_CASES)
    selected = unittest.TestSuite()
    for test in iter_tests(suite):
        short_name = str(test)
        function_name = getattr(test, "_testMethodName", "")
        if short_name in wanted or function_name in wanted:
            selected.addTest(test)
    return selected


def main() -> int:
    # Expected-rejection cases intentionally exercise logging paths. Keep the
    # result stream limited to short test names and assertion failures.
    logging.disable(logging.CRITICAL)
    suite = unittest.defaultTestLoader.discover(
        start_dir=str(REPO_ROOT / "tests"),
        pattern="test*.py",
        top_level_dir=str(REPO_ROOT),
    )
    suite = select_tests(suite)
    result = CompactTextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
