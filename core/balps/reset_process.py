from sqlalchemy import text
import os
import sys

"""
Reset Task and Watchdog PID
"""


class reset_process_api:
    def __init__(self, mssql_obj):
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        # from balps.mssql_mgr import mssql_api
        from balps.log_mgr import logging_api

        """
        Initialize db connection

        Args:
            None

        Returns:
            None

        Example:
            >>> reset_obj = reset_pid_api()
        """
        try:
            self.logger = logging_api(filename=os.path.join("logs", f"{os.path.splitext(os.path.basename(sys.argv[0]))[0]}"), backup_count=7)
            self.logging = self.logger.get_logger()  # Create influxdb and mssql object
            self.mssql_obj = mssql_obj  # mssql_api()
            # self.mssql_obj.create_db_connect_pd()
        except Exception:
            self.logging.error(f"Reset pid initialization failed\n{self.logger.get_slim_error_log()}")

    def watchdog_active_reset(self):
        """
        Reset watchdog process

        Args:
            None

        Returns:
            None

        Example:
            >>> reset_obj = reset_pid_api()
            >>> reset_obj.watchdog_active_reset()
        """
        try:
            sql_cmd = text("""
            UPDATE dbo.Watchdog SET active = 0 WHERE is_delete = 0
            """)
            self.mssql_obj.update_db_pd(sql_cmd, None)
        except Exception:
            self.logging.error(f"watchdog_active_reset failed\n{self.logger.get_slim_error_log()}")
