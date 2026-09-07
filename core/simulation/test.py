from datetime import datetime, timezone
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


class InfluxFieldTest:
    """
    Test InfluxDB field behavior.

    This class verifies:
    1. Adding a new field to an existing measurement.
    2. Writing a different data type to an existing field.
    """

    def __init__(self, url, token, org, bucket):
        """
        Initialize InfluxDB connection parameters.

        Args:
            url: InfluxDB server URL.
            token: InfluxDB access token.
            org: InfluxDB organization name.
            bucket: InfluxDB bucket name.

        Returns:
            None
        """
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket

        self.client = None
        self.write_api = None

    def connect(self):
        """
        Connect to InfluxDB.

        Args:
            None

        Returns:
            None
        """
        try:
            self.client = InfluxDBClient(
                url=self.url,
                token=self.token,
                org=self.org
            )

            self.write_api = self.client.write_api(
                write_options=SYNCHRONOUS
            )

            print("[INFO] Connected to InfluxDB.")

        except Exception as ex:
            print(f"[ERROR] InfluxDB connection failed: {ex}")
            raise

    def test_initial_write(self):
        """
        Write the initial measurement fields.

        Args:
            None

        Returns:
            None
        """
        try:
            point = (
                Point("field_test")
                .tag("device", "battery_01")
                .field("voltage", 3.7)
                .field("temperature", 25.0)
                .time(datetime.now(timezone.utc), WritePrecision.NS)
            )

            self.write_api.write(
                bucket=self.bucket,
                org=self.org,
                record=point
            )

            print("[PASS] Initial fields written successfully.")
            print("       voltage=float")
            print("       temperature=float")

        except Exception as ex:
            print(f"[FAIL] Initial write failed: {ex}")

    def test_add_new_field(self):
        """
        Add a new field to the existing measurement.

        Args:
            None

        Returns:
            None
        """
        try:
            point = (
                Point("field_test")
                .tag("device", "battery_01")
                .field("voltage", 3.8)
                .field("temperature", 26.0)
                .field("current", 10.5)
                .time(datetime.now(timezone.utc), WritePrecision.NS)
            )

            self.write_api.write(
                bucket=self.bucket,
                org=self.org,
                record=point
            )

            print("[PASS] New field added successfully.")
            print("       current=float")

        except Exception as ex:
            print(f"[FAIL] Adding new field failed: {ex}")

    def test_field_type_conflict(self):
        """
        Change an existing field type to trigger a type conflict.

        Args:
            None

        Returns:
            None
        """
        try:
            point = (
                Point("field_test")
                .tag("device", "battery_01")
                .field("voltage", "3.9")
                .field("temperature", 27.0)
                .time(datetime.now(timezone.utc), WritePrecision.NS)
            )

            self.write_api.write(
                bucket=self.bucket,
                org=self.org,
                record=point
            )

            print("[WARNING] Type-conflict write unexpectedly succeeded.")

        except Exception as ex:
            print("[EXPECTED FAIL] Field type conflict occurred.")
            print(f"                {ex}")

    def test_int_to_float_conflict(self):
        """
        Test integer and float field type behavior.

        Args:
            None

        Returns:
            None
        """
        try:
            point = (
                Point("field_type_int_test")
                .tag("device", "battery_01")
                .field("status_value", 100)
                .time(datetime.now(timezone.utc), WritePrecision.NS)
            )

            self.write_api.write(
                bucket=self.bucket,
                org=self.org,
                record=point
            )

            print("[PASS] Integer field written.")
            print("       status_value=integer")

            point = (
                Point("field_type_int_test")
                .tag("device", "battery_01")
                .field("status_value", 100.5)
                .time(datetime.now(timezone.utc), WritePrecision.NS)
            )

            self.write_api.write(
                bucket=self.bucket,
                org=self.org,
                record=point
            )

            print("[WARNING] int -> float write unexpectedly succeeded.")

        except Exception as ex:
            print("[EXPECTED FAIL] int -> float type conflict occurred.")
            print(f"                {ex}")

    def close(self):
        """
        Close InfluxDB connection.

        Args:
            None

        Returns:
            None
        """
        try:
            if self.client:
                self.client.close()

            print("[INFO] InfluxDB connection closed.")

        except Exception as ex:
            print(f"[ERROR] Failed to close InfluxDB connection: {ex}")


if __name__ == "__main__":

    URL = "http://localhost:8086"
    TOKEN = "1asCuXLjSMF2sTgYx-ukZ3zagQ-PdBS3dsFWwdoAbtctbmwlTRz0ERqXV0rXsJlASuG7lD7xX01vje-Ot1btzQ=="
    ORG = "delta"
    BUCKET = "test_bucket"

    tester = InfluxFieldTest(
        url=URL,
        token=TOKEN,
        org=ORG,
        bucket=BUCKET
    )

    try:
        tester.connect()

        print("\n========== TEST 1 ==========")
        print("Initial write")
        tester.test_initial_write()

        print("\n========== TEST 2 ==========")
        print("Add new field")
        tester.test_add_new_field()

        print("\n========== TEST 3 ==========")
        print("Change float field to string")
        tester.test_field_type_conflict()

        print("\n========== TEST 4 ==========")
        print("Change integer field to float")
        tester.test_int_to_float_conflict()

    except Exception as ex:
        print(f"[FATAL] Test stopped: {ex}")

    finally:
        tester.close()