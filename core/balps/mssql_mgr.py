from io import StringIO
import pandas as pd
from sqlalchemy import create_engine, text
import urllib
import os
import sys
import json
from datetime import datetime
import time

"""
MSSQL Operation
"""

DATABASE_CONFIG = {
    "server": "LOCALHOST",
    "database": "BALPS",
    "driver": "{SQL Server}",
    "driver_pd": "ODBC+Driver+17+for+SQL+Server",
}


class mssql_api:
    def __init__(self, ip=DATABASE_CONFIG["server"], db=DATABASE_CONFIG["database"]):
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from .log_mgr import logging_api

        """
        Setup mssql configuration

        Args:
            ip: Ip address
            db: Database name

        Returns:
            None

        Example:
            >>> mssql_obj = mssql_api("localhost", "BALPS")
        """
        try:
            self.logger = logging_api(filename=os.path.join("logs", f"{os.path.splitext(os.path.basename(sys.argv[0]))[0]}"), backup_count=7)
            self.logging = self.logger.get_logger()  # Create influxdb and mssql object
            self.DB_DRIVER = DATABASE_CONFIG["driver"]
            self.DB_DRIVER_PD = DATABASE_CONFIG["driver_pd"]
            self.DB_SERVER = ip
            self.DB_DATABASE = db
            self.meas_map_pd = None
        except Exception:
            self.logging.error(f"Mssql initialization failed\n{self.logger.get_slim_error_log()}")

    def wait_mssql_ready(self):
        while True:
            try:
                self.check_mssql_alive(text("""SELECT 1"""))
                return
            except Exception:
                self.logging.warning("⏳ Waiting for MSSQL...)")
            time.sleep(3)

    def create_db_connect_pd(self):
        """
        Create influx db connection for engine module

        Args:
            None

        Returns:
            None

        Example:
            >>> mssql_obj = mssql_api("localhost", "BALPS")
            >>> mssql_obj.create_db_connect_pd()
        """
        try:
            params = urllib.parse.quote_plus(
                f"DRIVER={self.DB_DRIVER_PD};SERVER={self.DB_SERVER};DATABASE={self.DB_DATABASE};Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"
            )
            conn_string = f"mssql+pyodbc:///?odbc_connect={params}"
            self.engine = create_engine(conn_string, pool_pre_ping=True)
        except Exception:
            self.logging.error(f"create_db_connect_pd failed\n{self.logger.get_slim_error_log()}")

    def close_db_connect_pd(self):
        """
        Close influx db connection for engine module

        Args:
            None

        Returns:
            None

        Example:
            >>> mssql_obj = mssql_api("localhost", "BALPS")
            >>> mssql_obj.close_db_connect_pd()
        """
        try:
            self.engine.dispose()
        except Exception:
            self.logging.error(f"close_db_connect_pd failed\n{self.logger.get_slim_error_log()}")

    def write_db_pd(self, out_pd, table_name):
        """
        Write pandas table to mssql

        Args:
            out_pd: Output pandas table
            table_name: Output table name

        Returns:
            None

        Example:
            >>> mssql_obj = mssql_api("localhost", "BALPS")
            >>> mssql_obj.write_db_pd(pd, "StoreHouseCellMeas")
        """
        try:
            out_pd.to_sql(table_name, con=self.engine, if_exists="append", index=False)
        except Exception:
            self.logging.error(f"write_db_pd failed\n{self.logger.get_slim_error_log()}")

    def check_mssql_alive(self, query):
        """
        Check mssql alive

        Args:
            query: Sql command

        Returns:
            Query result for pandas table

        Example:
            >>> mssql_obj = mssql_api("localhost", "BALPS")
            >>> mssql_obj.check_mssql_alive(query)
        """
        try:
            with self.engine.connect() as connection:
                return pd.read_sql(query, connection)
        except Exception as e:
            raise e

    def query_db_pd(self, query):
        """
        Query mssql and write data to pandas table

        Args:
            query: Sql command

        Returns:
            Query result for pandas table

        Example:
            >>> mssql_obj = mssql_api("localhost", "BALPS")
            >>> mssql_obj.query_db_pd(query)
        """
        try:
            with self.engine.connect() as connection:
                return pd.read_sql(query, connection)
        except Exception as e:
            self.logging.error(f"query_db_pd failed\n{self.logger.get_slim_error_log()}")
            raise e

    def update_db_pd(self, sql_cmd, var):
        """
        Update sql table

        Args:
            sql_cmd: Sql command

        Returns:
            None

        Example:
            >>> mssql_obj = mssql_api("localhost", "BALPS")
            >>> mssql_obj.update_db_pd(sql_cmd, var)

            >>> sql_cmd = text("
            >>> UPDATE task
            >>> SET pid = :pid, last_exec_time = :last_execution
            >>> WHERE task_uuid = :task_uuid
            >>> ")
            >>> var = {
            >>>     'pid': pid,
            >>>     'last_execution': last_execution,
            >>>     'task_uuid': task_uuid
            >>> }
        }
        """
        try:
            with self.engine.connect() as connection:
                connection.execute(sql_cmd, var)
                connection.commit()
        except Exception:
            self.logging.error(f"update_db_pd failed\n{self.logger.get_slim_error_log()}")

    def create_db_table(self, sql_cmd_list):
        """
        Create sql table

        Args:
            sql_cmd_list: Sql command list

        Returns:
            None

        Example:
            >>> mssql_obj = mssql_api("localhost", "BALPS")
            >>> mssql_obj.create_db_pd(sql_cmd_list)

            >>> sql_cmd_list = ["
            >>> IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ServerMap')
            >>> CREATE TABLE [dbo].[ServerMap](
            >>> [StoreHouseID] [int] NULL,
            >>> [PalletID] [varchar](20) NULL,
            >>> [SerialBoardID] [varchar](20) NULL,
            >>> [PalletPosition] [varchar](1) NULL,
            >>> [Position] [int] NULL,
            >>> [QRCODEID] [varchar](20) NULL
            >>> ) ON [PRIMARY]"]
        """
        try:
            with self.engine.connect() as connection:
                for query in sql_cmd_list:
                    connection.execute(text(query))
                connection.commit()
        except Exception:
            self.logging.error(f"create_db_table failed\n{self.logger.get_slim_error_log()}")

    def delete_db_table(self, sql_cmd, params=None):
        """
        Delete sql table

        Args:
            sql_cmd: Sql command

        Returns:
            None

        Example:
            >>> mssql_obj = mssql_api("localhost", "BALPS")
            >>> mssql_obj.create_db_pd(sql_cmd)

            >>> sql_cmd = ["
            >>> IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Watchdog]') AND type in (N'U'))
            >>> DROP TABLE [dbo].[Watchdog]
            >>> IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ServerMap]') AND type in (N'U'))
            >>> DROP TABLE [dbo].ServerMap
            >>> IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Task]') AND type in (N'U'))
            >>> DROP TABLE [dbo].Task"]
        """
        try:
            with self.engine.connect() as connection:
                connection.execute(sql_cmd, params or {})
                connection.commit()
        except Exception:
            self.logging.error(f"delete_db_table failed\n{self.logger.get_slim_error_log()}")

    def clear_task_table(self, is_all_clear=False, sb_id=""):
        """
        Clear task table

        Args:
            None

        Returns:
            None

        Example:
            >>> mssql_obj = mssql_api("localhost", "BALPS")
            >>> mssql_obj.clear_task_table()
        """
        try:
            with self.engine.connect() as connection:
                # connection.execute(text("DELETE FROM dbo.Task"))
                if is_all_clear:
                    # connection.execute(text("Update dbo.Task SET is_delete = 1 WHERE is_delete = 0"))
                    connection.execute(text("DELETE FROM dbo.Task"))
                else:
                    connection.execute(text("Update dbo.Task SET is_delete = 1 WHERE is_delete = 0 AND task_name= :task_name"), {"task_name": sb_id})
                connection.commit()
        except Exception:
            self.logging.error(f"clear_task_table failed\n{self.logger.get_slim_error_log()}")

    def create_rack_status(self, mssql_obj, rack_id, sbid):
        """
        Create rack status

        Args:
            rack_id: Rack ID
            sbid: SerialBoard ID

        Returns:
            None

        Example:
            >>> mssql_obj = mssql_api("127.0.0.1", 12345)
            >>> mssql_obj.create_rack_status(mssql_obj, rack_id, sbid)
        """
        try:
            json_data = json.dumps([{"rack_id": rack_id, "pallet_id": sbid, "pallet_status": 0, "last_update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")}], indent=4)

            mssql_obj.write_db_pd(pd.read_json(StringIO(json_data)), "RackStatus")
            self.logging.info(f"Create RackStatus [RackID {rack_id}] [SerialboardID: {sbid}]")
        except Exception:
            self.logging.error(f"create_task failed\n{self.logger.get_slim_error_log()}")

    def update_pallet_status(self, mssql_obj, shid, sbid, status):
        """
        Update pallet status

        Args:
            mssql_obj: MSSQL object
            sbid: SerialBoardID
            status: Pallet status (0: offline, 1: running, 2: error)

        Returns:
            None

        Example:
            >>> mssql_obj = mssql_api("127.0.0.1", 12345)
            >>> mssql_obj.update_pallet_status(mssql_obj, sbid, status)
        """
        try:
            sql_cmd = text("""
            UPDATE RackStatus
            SET pallet_status = :status, last_update_time = :last_update_time
            WHERE pallet_id = :sbid
            AND rack_id =:shid
            """)
            var = {"shid": shid, "sbid": sbid, "status": status, "last_update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")}
            mssql_obj.update_db_pd(sql_cmd, var)
            # self.logging.info(f"update_pallet_status successfull | sbid: {sbid}, status: {status}")
        except Exception:
            self.logging.error(f"update_pallet_status failed\n{self.logger.get_slim_error_log()}")

    def delete_rack_status(self, mssql_obj, rack_id=1, is_all_clear=False):
        """
        Create rack status

        Args:
            rack_id: Rack ID

        Returns:
            None

        Example:
            >>> mssql_obj = mssql_api("127.0.0.1", 12345)
            >>> mssql_obj.delete_rack_status(mssql_obj, rack_id)
        """
        try:
            if is_all_clear:
                sql_cmd = text("""
                DELETE FROM RackStatus
                """)
                mssql_obj.delete_db_table(sql_cmd)
                self.logging.info("Cleared all RackStatus records from MSSQL")
            else:
                sql_cmd = text("""
                DELETE FROM RackStatus
                WHERE rack_id = :rack_id
                """)
                var = {"rack_id": rack_id}
                mssql_obj.delete_db_table(sql_cmd, var)
                self.logging.info(f"Deleted RackStatus records for Rack {rack_id} from MSSQL")
        except Exception:
            self.logging.error(f"delete_rack_status failed\n{self.logger.get_slim_error_log()}")
