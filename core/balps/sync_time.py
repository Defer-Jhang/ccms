import subprocess  # noqa: S404
from datetime import datetime
import os
import sys
from balps.log_mgr import logging_api
import ctypes

"""
Sync Sytem Time

"""


class sync_time_api:
    def __init__(self, influxdb_obj):
        """
        Init sync system time

        Args:
            influxdb_obj: Used to write system logs

        Returns:
            None

        Example:
            >>> sync_time_obj = sync_time_api(influxdb_obj)
        """
        self.influxdb_obj = influxdb_obj

    def run(self, datetime_string):
        """
        Run sync system time

        Args:
            datetime_string: Updated system time

        Returns:
            None

        Example:
            >>> sync_time_obj = sync_time_api(influxdb_obj)
            >>> sync_time_obj.run(datetime_string)
        """
        try:
            self.logger = logging_api(filename=os.path.join("logs", f"{os.path.splitext(os.path.basename(sys.argv[0]))[0]}"), backup_count=7)
            self.logging = self.logger.get_logger()  # Create influxdb and mssql object
            setup_datetime = datetime.strptime(datetime_string, "%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
            # powershell_command = (
            #     "$ErrorActionPreference='Stop'; "
            #     f"Set-Date -Date ([datetime]::ParseExact('{setup_datetime}','yyyy-MM-dd HH:mm:ss',$null))"
            # )
            # self.run_as_admin_powershell(powershell_command)
            powershell_command = (
                "$ErrorActionPreference='Stop'; "
                f"Set-Date -Date ([datetime]::ParseExact("
                f"'{setup_datetime}','yyyy-MM-dd HH:mm:ss',$null)) "
                "| Out-Null"
            )

            self.run_as_admin_powershell(powershell_command)
            if self.verify_time_sync(datetime_string):
                return True
            else:
                return False
        except Exception:
            return False

    def verify_time_sync(self, verified_datetime):
        """
        Verify sync system time

        Args:
            verified_datetime: Updated system time

        Returns:
            None

        Example:
            >>> sync_time_obj = sync_time_api(influxdb_obj)
            >>> sync_time_obj.verify_time_sync(verified_datetime)
        """
        try:
            current_time = datetime.now()
            verified_datetime = datetime.strptime(verified_datetime, "%Y%m%d%H%M%S")
            # Allow time difference of 10 seconds
            if abs((current_time - verified_datetime).total_seconds()) <= 10:
                return True
            return False
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"verify_time_sync failed\n{self.logger.get_slim_error_log()}", "system")
            return False

    def run_as_admin_powershell(self, command):
        """
        Use admin to update system time

        Args:
           command: Sync system time command

        Returns:
            None

        Example:
            >>> sync_time_obj = sync_time_api(influxdb_obj)
            >>> sync_time_obj.run_as_admin_powershell(command)
        """
        try:
            ctypes.windll.shell32.ShellExecuteW(
                None, "runas", "powershell.exe",
                f'-NoProfile -WindowStyle Hidden -Command "{command}"',
                None, 0
            )  # noqa: S603, S607
        except Exception as e:
            self.influxdb_obj.write_log_influxdb("ERROR", f"run_as_admin_powershell failed\n{self.logger.get_slim_error_log()}", "system")
            raise e
