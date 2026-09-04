import pandas as pd
import os
import sys
from sqlalchemy import text

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import balps.mssql_mgr


class init_db:
    def __init__(self):
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from balps.log_mgr import logging_api

        """
        DB connection initizliation 

        Args:
            None

        Returns:
            None

        Example:
            >>> init_db_obj = init_db()
        """
        try:
            self.logger = logging_api(filename=os.path.join("logs", f"{os.path.splitext(os.path.basename(sys.argv[0]))[0]}"), backup_count=7)
            self.logging = self.logger.get_logger()  # Create influxdb and mssql object
            self.mssql_obj = balps.mssql_mgr.mssql_api()
            self.mssql_obj.create_db_connect_pd()
        except Exception:
            self.logging.error(f"DB initialization failed\n{self.logger.get_slim_error_log()}")

    def create_tables(self):
        """
        Create watchdog, task ,server map, rackstatus table

        Args:
            None

        Returns:
            None

        Example:
            >>> init_db_obj = init_db()
            >>> init_db_obj.create_tables()
        """
        try:
            queries = [
                """
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Watchdog')
                CREATE TABLE [dbo].[Watchdog](
                    [watchdog_id] [bigint] IDENTITY(1,1)  NOT NULL PRIMARY KEY CLUSTERED,
                    [datetime] [datetime2](7) NULL,
                    [app_name] [varchar](64) NULL,
                    [app_uuid] [varchar](64) NULL,
                    [launch_command] [varchar](64) NULL,
                    [last_exec_time] [datetime2](7) NULL,
                    [pid] [bigint] NULL,
                    [active] [bit] NULL,
                    [is_delete] [bit] NULL
                ) ON [PRIMARY]
                """,
                """
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Task')
                CREATE TABLE [dbo].[Task](
                    [task_id] [bigint] IDENTITY(1,1) NOT NULL PRIMARY KEY CLUSTERED,
                    [datetime] [datetime2](7) NULL,
                    [task_name] [varchar](64) NULL,
                    [task_uuid] [varchar](64) NULL,
                    [description] [varchar](256) NULL,
                    [last_exec_time] [datetime2](7) NULL,
                    [launch_command] [varchar](128) NULL,
                    [pid] [bigint] NULL,
                    [ttl] [bigint] NULL,
                    [active] [bit] NULL,
                    [is_delete] [bit] NULL,
                    [pallet_info] [varchar](max) NULL,
                    [step_info] [varchar](max) NULL,
                    [task_step] [smallint] NULL,
                    [task_ack] [bit] NULL
                ) ON [PRIMARY]
                """,
                """
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ServerMap')
                CREATE TABLE [dbo].[ServerMap](
                    [datetime] [datetime2](7) NULL,
                    [StoreHouseID] [int] NULL,
                    [PalletID] [varchar](20) NULL,
                    [SerialBoardID] [varchar](20) NULL,
                    [ProtectBoardID] [varchar](20) NULL,
                    [PalletPosition] [varchar](1) NULL,
                    [Position] [int] NULL,
                    [QRCODEID] [varchar](20) NULL
                ) ON [PRIMARY]
                """,
                """
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'CCSMap')
                CREATE TABLE [dbo].[CCSMap](
                    [StoreHouseID] [int] NULL,
                    [PalletID] [varchar](20) NULL,
                    [SerialBoardID] [varchar](20) NULL,
                    [ProtectBoardID] [varchar](20) NULL,
                    [PalletPosition] [varchar](1) NULL,
                    [Position] [int] NULL,
                    [QRCODEID] [varchar](20) NULL
                ) ON [PRIMARY]
                """,
                """
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'RackStatus')
                CREATE TABLE [dbo].[RackStatus](
                    [rack_status_id] [bigint] IDENTITY(1,1) NOT NULL,
                    [rack_id] [smallint] NULL,
                    [pallet_id] [nvarchar](50) NULL,
                    [pallet_status] [smallint] NOT NULL,
                    [last_update_time] [datetime2](7) NULL,
                CONSTRAINT [PK_RackStatus] PRIMARY KEY CLUSTERED 
                (
                    [rack_status_id] ASC
                )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
                ) ON [PRIMARY]
                """,
            ]

            self.mssql_obj.create_db_table(queries)
            self.logging.info("create_tables successfull")
        except Exception:
            self.logging.error(f"create_tables failed\n{self.logger.get_slim_error_log()}")

    def delete_tables(self):
        """
        Create watchdog, task and server map table

        Args:
            None

        Returns:
            None

        Example:
            >>> init_db_obj = init_db()
            >>> init_db_obj.delete_tables()
        """
        try:
            queries = [
                text("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Watchdog]') AND type in (N'U')) DROP TABLE [dbo].[Watchdog];"),
                text("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ServerMap]') AND type in (N'U')) DROP TABLE [dbo].[ServerMap];"),
                text("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[CCSMap]') AND type in (N'U')) DROP TABLE [dbo].[CCSMap];"),
                text("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Task]') AND type in (N'U')) DROP TABLE [dbo].[Task];"),
                text("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[RackStatus]') AND type in (N'U')) DROP TABLE [dbo].[RackStatus];"),
            ]

            for query in queries:
                self.mssql_obj.delete_db_table(query)
            self.logging.info("delete_tables successfull")
        except Exception:
            self.logging.error(f"delete_tables failed\n{self.logger.get_slim_error_log()}")

    def insert_csv_to_table(self, csv_path, table_name):
        """
        Write watchdog, task and server map table data

        Args:
            None

        Returns:
            None

        Example:
            >>> init_db_obj = init_db()
            >>> init_db_obj.insert_csv_to_table()
        """
        try:
            self.mssql_obj.write_db_pd(pd.read_csv(csv_path), table_name)
            self.logging.info(f"insert_csv_to_table successfull | {table_name}")
        except Exception:
            self.logging.error(f"insert_csv_to_table failed\n{self.logger.get_slim_error_log()}")


if __name__ == "__main__":
    # Initialization
    init_db_obj = init_db()
    # Delete table
    init_db_obj.delete_tables()
    # Create table
    init_db_obj.create_tables()

    # Insert watchdog data
    init_db_obj.insert_csv_to_table(csv_path="core\\db\\watchdog.csv", table_name="Watchdog")
    # Insert server map data
    init_db_obj.insert_csv_to_table(csv_path="core\\db\\servermap.csv", table_name="CCSMap")
