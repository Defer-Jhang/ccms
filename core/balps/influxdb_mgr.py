import pandas as pd

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import ASYNCHRONOUS, SYNCHRONOUS
from datetime import datetime
import pytz
from functools import partial
from multiprocessing import Pool
import numpy as np
from balps.log_mgr import logging_api
import os
import sys

"""
InfluxDB Operation
"""


class influxdb_api:
    def __init__(self, bucket="StoreHouseMeasData"):
        """
        Setup influx db configuration

        Args:
            bucket: Store multiple measurement tables
            meas: Measurement table name

        Returns:
            None

        Example:
            >>> influxdb_obj = influxdb_api("StoreHouseMeasData")
        """
        # Setup influxdb configuration
        self.url = "http://localhost:8086"
        self.token = "1asCuXLjSMF2sTgYx-ukZ3zagQ-PdBS3dsFWwdoAbtctbmwlTRz0ERqXV0rXsJlASuG7lD7xX01vje-Ot1btzQ=="  # noqa: S105
        self.org = "delta"
        self.bucket = bucket
        self.logs = "logs"
        self.msg = "xml_msg"
        self.json = "json_msg"
        self.perf = "perf"
        self.ccs_logs = "ccs_logs"
        self.logger = logging_api(filename=os.path.join("logs", f"{os.path.splitext(os.path.basename(sys.argv[0]))[0]}"), backup_count=7)
        self.logging = self.logger.get_logger()  # Create influxdb and mssql object

    def health_check(self):
        """
        Health check

        Args:
            None

        Returns:
            True or False

        Example:
            >>> influxdb_obj = influxdb_api("StoreHouseMeasData")
            >>> influxdb_obj.health_check()
        """
        try:
            if self.influx_client.health().status == "pass":
                return True
        except Exception:
            self.logging.error(f"health_check failed\n{self.logger.get_slim_error_log()}")
            self.write_log_influxdb("ERROR", f"health_check failed\n{self.logger.get_slim_error_log()}", "system")
            return False

    def create_connect(self):
        """
        Create influx db connection

        Args:
            None

        Returns:
            None

        Example:
            >>> influxdb_obj = influxdb_api("StoreHouseMeasData")
            >>> influxdb_obj.create_connect()
        """
        try:
            # Create influx db connection
            self.influx_client = InfluxDBClient(url=self.url, token=self.token, org=self.org, timeout=600000)
            # Create query and write api for influx db
            self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)
            self.query_api = self.influx_client.query_api()
            self.create_bucket(self.bucket)
        except Exception as e:
            self.logging.error(f"create_connect failed\n{self.logger.get_slim_error_log()}")
            self.write_log_influxdb("ERROR", f"create_connect failed\n{self.logger.get_slim_error_log()}", "system")
            raise e

    def close_connect(self):
        """
        Close influx db connection

        Args:
            None

        Returns:
            None

        Example:
            >>> influxdb_obj = influxdb_api("StoreHouseMeasData")
            >>> influxdb_obj.close_connect()
        """
        # Close connection
        try:
            self.influx_client.close()
        except Exception:
            self.logging.error(f"close_connect failed\n{self.logger.get_slim_error_log()}")
            self.write_log_influxdb("ERROR", f"close_connect failed\n{self.logger.get_slim_error_log()}", "system")

    def delete_bucket(self, bucket_name):
        """
        Delete influx db bucket

        Args:
            bucket_name: Created bucket name

        Returns:
            None

        Example:
            >>> influxdb_obj = influxdb_api("StoreHouseMeasData")
            >>> influxdb_obj.create_connect()
            >>> influxdb_obj.delete_bucket("test11")
        """
        try:
            # Create bucket
            bucket_api = self.influx_client.buckets_api()
            # Check if the bucket exists
            bucket = bucket_api.find_bucket_by_name(bucket_name)
            if bucket is not None:
                bucket_api.delete_bucket(bucket)

        except Exception:
            self.logging.error(f"delete_bucket failed\n{self.logger.get_slim_error_log()}")
            self.write_log_influxdb("ERROR", f"delete_bucket failed\n{self.logger.get_slim_error_log()}", "system")

    def create_bucket(self, bucket_name):
        """
        Create influx db bucket

        Args:
            bucket_name: Created bucket name

        Returns:
            None

        Example:
            >>> influxdb_obj = influxdb_api("StoreHouseMeasData")
            >>> influxdb_obj.create_connect()
            >>> influxdb_obj.create_bucket("test11")
        """
        try:
            # Create bucket
            bucket_api = self.influx_client.buckets_api()
            # Check if the bucket exists
            if bucket_api.find_bucket_by_name(bucket_name) is None:
                bucket_api.create_bucket(bucket_name=bucket_name, org=self.org)

        except Exception as e:
            self.logging.error(f"create_bucket failed\n{self.logger.get_slim_error_log()}")
            self.write_log_influxdb("ERROR", f"create_bucket failed\n{self.logger.get_slim_error_log()}", "system")
            raise e

    def write_pd_influxdb_many_record(self, df, meas, tags):
        """
        Write pandas table to influx db

        Args:
            df: Input pandas table
            meas: Written measurement object

        Returns:
            None

        Example:
            >>> influxdb_obj = influxdb_api("StoreHouseMeasData")
            >>> influxdb_obj.create_connect()
            >>> influxdb_obj.create_bucket("test11")
        """
        try:
            self.tags = tags
            self.df = df
            self.measurement = meas

            self.create_bucket(self.bucket)
            # Write pandas data to influxdb in parallel
            num_processes = 4
            chunks = np.array_split(df, num_processes)
            with Pool(num_processes) as pool:
                process_func = partial(influxdb_api.process_chunk, meas=meas, tags=tags)
                results = pool.map(process_func, chunks)
            points = [point for sublist in results for point in sublist]
            self.write_api.write(self.bucket, record=points)
        except Exception:
            self.logging.error(f"write_pd_influxdb_many_record failed\n{self.logger.get_slim_error_log()}")
            self.write_log_influxdb("ERROR", f"write_pd_influxdb_many_record failed\n{self.logger.get_slim_error_log()}", "system")

    def dataframe_to_point(self, tz="Asia/Taipei"):
        """
        Convert dataframe to influxdb point

        Args:
            None

        Returns:
            None

        Example:
            >>> influxdb_obj = influxdb_api("StoreHouseMeasData")
            >>> influxdb_obj.create_connect()
            >>> influxdb_obj.dataframe_to_point()
        """
        try:
            tz = pytz.timezone(tz)
            points = []
            # Convert dataframe to point
            for _, row in self.df.iterrows():
                timestamp = pd.to_datetime(row["datetime"]).tz_localize(tz).tz_convert(pytz.utc)
                point = Point(self.measurement).time(timestamp)
                # Add tags and fields
                for col in self.df.columns:
                    if col in self.tags:
                        if not pd.isna(row[col]):
                            point.tag(col, row[col])
                    elif col != "datetime":
                        point.field(col, row[col])
                points.append(point)
            return points
        except Exception:
            self.logging.error(f"dataframe_to_point failed\n{self.logger.get_slim_error_log()}")
            self.write_log_influxdb("ERROR", f"dataframe_to_point failed\n{self.logger.get_slim_error_log()}", "system")

    def write_pd_influxdb(self, df, meas, tags):
        """
        Write pandas table to influx db

        Args:
            df: Input pandas table
            meas: Written measurement object
            tags: Set index of the table

        Returns:
            None

        Example:
            >>> influxdb_obj = influxdb_api("StoreHouseMeasData")
            >>> influxdb_obj.create_connect()
            >>> influxdb_obj.write_pd_influxdb(df, meas, tags)
        """
        try:
            self.tags = tags
            self.df = df
            self.measurement = meas
            # Create bucket (table)
            self.create_bucket(self.bucket)
            point = self.dataframe_to_point()
            self.write_api.write(self.bucket, record=point)
        except Exception:
            self.logging.error(f"write_pd_influxdb failed\n{self.logger.get_slim_error_log()}")
            self.write_log_influxdb("ERROR", f"write_pd_influxdb failed\n{self.logger.get_slim_error_log()}", "system")

    def reformat_json_horizontal(self, data, exclude_keys=None):
        result_lines = []
        if exclude_keys is None:
            exclude_keys = {}
        for key, val in data.items():
            if key in exclude_keys:
                continue
            if isinstance(val, dict) and all(k.isdigit() for k in val.keys()):
                step_keys = sorted(val.keys(), key=int)
                values = [str(val.get(k, "")) for k in step_keys]
                if len(values) == 1:
                    result_lines.append(f"{key:<22}: {values[0]}")
                else:
                    result_lines.append(f"{key:<22}: {', '.join(values)}")

        return "\n".join(result_lines)

    def format_df_mixed(self, df):
        single_fields = {"datetime", "storehouse_id", "serialboard_id", "slot_sts", "protectboard_id", "current"}
        lines = []
        for col in df.columns:
            if col in single_fields:
                val = str(df.iloc[0][col])
                lines.append(f"{col:<18}: {val}")
            else:
                values = ", ".join(str(v) for v in df[col].tolist())
                lines.append(f"{col:<18}: {values}")
        return "\n".join(lines)

    def write_perf_influxdb(self, item, data, sbid="A0001", shid="1"):
        """
        Write performance data to influxdb

        Args:
            item: test item (e.g. HeartbeatRspTime)
            data: performance data
            sbid: Serial board ID

        Returns:
            None

        Example:
            >>> influxdb_obj = influxdb_api("StoreHouseMeasData")
            >>> influxdb_obj.create_connect()
            >>> influxdb_obj.write_perf_influxdb("HeartbeatRspTime", 0.25, "A0001")
        """
        try:
            self.create_bucket(self.bucket)
            point = Point(self.perf).tag("item", item).tag("serialboard_id", sbid).tag("storehouse_id", shid).field("data", data).time(datetime.utcnow(), WritePrecision.NS)
            self.write_api.write(self.bucket, record=point)
        except Exception:
            self.logging.error(f"write_perf_influxdb failed\n{self.logger.get_slim_error_log()}")
            self.write_log_influxdb("ERROR", f"write_perf_influxdb failed\n{self.logger.get_slim_error_log()}", "system")

    def write_log_influxdb(self, level, message, sbid="A0001"):
        """
        Write log to influxdb

        Args:
            level: Log levels are a categorization system based on hierarchy (e.g. WARN, INFO, DEBUG, ERROR)
            message: Log information

        Returns:
            None

        Example:
            >>> influxdb_obj = influxdb_api("StoreHouseMeasData")
            >>> influxdb_obj.create_connect()
            >>> influxdb_obj.write_log_influxdb("ERROR", "Write Panads Table Failed")
        """
        try:
            self.create_bucket(self.bucket)
            point = Point(self.logs).tag("level", level).tag("serialboard_id", sbid).field("message", message).time(datetime.utcnow(), WritePrecision.NS)
            self.write_api.write(self.bucket, record=point)
        except Exception:
            self.logging.error(f"write_log_influxdb failed\n{self.logger.get_slim_error_log()}")
            # This method is the terminal error-log sink; calling itself here
            # would recurse indefinitely when InfluxDB is unavailable.

    def write_ccs_logs_influxdb(self, level, message, sbid="A0001"):
            """
            Write ccs_logs to influxdb
    
            Args:
                level: ccs_logs levels are a categorization system based on hierarchy (e.g. WARN, INFO, DEBUG, ERROR)
                message: ccs_logs information
    
            Returns:
                None
    
            Example:
                >>> influxdb_obj = influxdb_api("StoreHouseMeasData")
                >>> influxdb_obj.create_connect()
                >>> influxdb_obj.write_ccs_logs_influxdb("ERROR", "Write Panads Table Failed")
            """
            try:
                self.create_bucket(self.bucket)
                point = Point(self.ccs_logs).tag("level", level).tag("serialboard_id", sbid).field("message", message).time(datetime.utcnow(), WritePrecision.NS)
                self.write_api.write(self.bucket, record=point)
            except Exception:
                self.logging.error(f"write_ccs_logs_influxdb failed\n{self.logger.get_slim_error_log()}")
                self.write_log_influxdb("ERROR", f"write_ccs_logs_influxdb failed\n{self.logger.get_slim_error_log()}", "system")

    def write_msg_influxdb(self, level, message, sbid="A0001", storehouse_id="0"):
        """
        Write xml message detail to influx db

        Args:
            level: Message levels are a categorization system based on hierarchy (e.g. WARN, INFO, DEBUG, ERROR)
            message: Log information

        Returns:
            None

        Example:
            >>> influxdb_obj = influxdb_api("StoreHouseMeasData")
            >>> influxdb_obj.create_connect()
            >>> influxdb_obj.write_msg_influxdb("DEBUG", xml_string)
        """
        try:
            self.create_bucket(self.bucket)
            point = Point(self.msg).tag("level", level).tag("serialboard_id", sbid).tag("storehouse_id", storehouse_id).field("message", message).time(datetime.utcnow(), WritePrecision.NS)
            self.write_api.write(self.bucket, record=point)
        except Exception:
            self.logging.error(f"write_msg_influxdb failed\n{self.logger.get_slim_error_log()}")
            self.write_log_influxdb("ERROR", f"write_msg_influxdb failed\n{self.logger.get_slim_error_log()}", "system")

    def write_json_influxdb(self, level, message, type="request", sbid="ALL", req_name="ALL"):
        """
        Write json message detail to influx db

        Args:
            level: Message levels are a categorization system based on hierarchy (e.g. WARN, INFO, DEBUG, ERROR)
            message: Log information

        Returns:
            None

        Example:
            >>> influxdb_obj = influxdb_api("StoreHouseMeasData")
            >>> influxdb_obj.create_connect()
            >>> influxdb_obj.write_json_influxdb("DEBUG", json_string)
        """
        try:
            self.create_bucket(self.bucket)
            point = (
                Point(self.json).tag("level", level).tag("serialboard_id", sbid).tag("type", type).tag("request_name", req_name).field("message", message).time(datetime.utcnow(), WritePrecision.NS)
            )
            self.write_api.write(self.bucket, record=point)
        except Exception:
            self.logging.error(f"write_json_influxdb failed\n{self.logger.get_slim_error_log()}")
            self.write_log_influxdb("ERROR", f"write_json_influxdb failed\n{self.logger.get_slim_error_log()}", "system")

    def read_pd_influxdb(self, start_time, end_time, bucket, meas, fields):
        """
        Read data from influx db to pandas table

        Args:
            start_time: Start time for reading data
            end_time: End time for reading data
            bucket: Bucket name
            meas: Measurement object
            field: Set query fields

        Returns:
            pd: Query results

        Example:
            >>> influxdb_obj = influxdb_api("StoreHouseMeasData")
            >>> influxdb_obj.create_connect()
            >>> influxdb_obj.read_pd_influxdb("2024-10-25T00:00:00Z", "2024-10-25T01:00:00Z", "CellMeas", "20241001", ["voltage", "temperature"])
        """
        try:
            query_base = f'''
            from(bucket: "{bucket}")
            |> range(start: {start_time}, stop: {end_time})
            |> filter(fn: (r) => r._measurement == "{meas}")
            '''

            # Set filter fields
            if fields:
                fields_filter = " or ".join([f'r._field == "{field}"' for field in fields])
                query_filter = f"|> filter(fn: (r) => {fields_filter})"
            else:
                query_filter = ""

            pivot_part = """
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            """

            query = query_base + query_filter + pivot_part

            return self.query_api.query_data_frame(query=query, org=self.org)
        except Exception:
            self.logging.error(f"read_pd_influxdb failed\n{self.logger.get_slim_error_log()}")
            self.write_log_influxdb("ERROR", f"read_pd_influxdb failed\n{self.logger.get_slim_error_log()}", "system")

