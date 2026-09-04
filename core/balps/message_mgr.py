import os
import sys

"""
Message Manager
    
"""


class MessageManager:
    def __init__(self, host, port, task_mgr_obj):
        from .influxdb_mgr import influxdb_api
        from .mssql_mgr import mssql_api
        from .message_api import message_api
        from .log_mgr import logging_api

        """
        Init message manager

        Args:
            host: connect ip (e.g. '127.0.0.1')
            port: connect port (e.g. 12345)
            task_mgr_obj: Task manager object

        Returns:
            None

        Example:
            >>> msg_obj = message_api(host, port, task_mgr_obj)
        """
        try:
            self.logger = logging_api(filename=os.path.join("logs", f"{os.path.splitext(os.path.basename(sys.argv[0]))[0]}"), backup_count=7)
            self.logging = self.logger.get_logger()  # Create influxdb and mssql object
            self.influxdb_obj = influxdb_api()
            self.influxdb_obj.create_connect()
            self.mssql_obj = mssql_api()
            self.mssql_obj.create_db_connect_pd()
            self.message_obj = message_api(host, port, self.influxdb_obj, self.mssql_obj, task_mgr_obj)
        except Exception as e:
            self.message_obj.stop()
            self.logging.error(f"{e}")

    def run(self):
        """
        Run message manager

        Args:
            None
        Returns:
            None

        Example:
            >>> msg_obj = message_api(host, port, influxdb_obj)
            >>> msg_obj.run()
        """
        try:
            self.message_obj.run()
        except Exception:
            self.message_obj.stop()
            self.influxdb_obj.write_log_influxdb("ERROR", f"run failed\n{self.logger.get_slim_error_log()}", "system")

    def handle_task_operation(self):
        """
        Handle task operation

        Args:
            None
        Returns:
            None

        Example:
            >>> msg_obj = message_api(host, port, influxdb_obj)
            >>> msg_obj.handle_task_operation()
        """
        try:
            self.message_obj.handle_task_operation()
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"handle_task_operation failed\n{self.logger.get_slim_error_log()}", "system")
