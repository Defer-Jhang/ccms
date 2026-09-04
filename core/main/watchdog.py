from datetime import datetime
import subprocess  # noqa: S404
import time
import threading
from sqlalchemy import text
import pandas as pd
import psutil
import os
import sys
import logging
import traceback

# size_gb = 30
# num_elements = (size_gb * 1024**3) // 8

# arr = np.ones(num_elements, dtype=np.float64)

"""
Run WatchDog Process
"""


class appManager:
    def __init__(self):
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from balps.log_mgr import logging_api
        from balps.reset_process import reset_process_api
        from balps.mssql_mgr import mssql_api
        from balps.influxdb_mgr import influxdb_api

        """
        Create influx db and mssql connection
        Initialize app activity list and ttl (time to live)

        Args:
            None

        Returns:
            None

        Example:
            >>> app_manager_obj = appManager()
        """
        try:
            self.logger = logging_api(filename=os.path.join("logs", f"{os.path.splitext(os.path.basename(sys.argv[0]))[0]}"), backup_count=7)
            self.logging = self.logger.get_logger()  # Create influxdb and mssql object
            self.mssql_obj = mssql_api()
            self.influxdb_obj = influxdb_api()
            self.influxdb_obj.create_connect()
            self.mssql_obj.create_db_connect_pd()
            self.mssql_obj.wait_mssql_ready()
            # Reset Application State of Watchdogs and apps
            reset_process_api(self.mssql_obj).watchdog_active_reset()
            self.active_app = pd.DataFrame()
            self.ttl_unit = 1  # unit second
        except Exception:
            self.logging.error(f"Watchdog initialization failed\n{self.logger.get_slim_error_log()}")

    def init_influxdb_connection(self):
        """
        Create influx db and mssql connection
        Initialize app activity list and ttl (time to live)

        Args:
            None

        Returns:
            None

        Example:
            >>> app_manager_obj = appManager()
            >>> app_manager_obj.init_influxdb_connection()
        """
        try:
            self.influxdb_obj.create_connect()
            return True
        except Exception:
            self.logging.error(f"init_influxdb_connection failed\n{self.logger.get_slim_error_log()}")
            return False

    def fetch_app(self):
        """
        Get application list

        Args:
            None

        Returns:
            Application list of pandas table

        Example:
            >>> app_manager_obj = appManager()
            >>> app_list = app_manager_obj.fetch_app()
        """
        try:
            query = """SELECT * FROM watchdog WHERE (is_delete = 0 AND active = 0) OR (is_delete = 1 AND active = 1) ORDER BY watchdog_id ASC"""
            app_list = self.mssql_obj.query_db_pd(query)
            return app_list[(app_list["is_delete"] == 0) & (app_list["active"] == 0)], app_list[(app_list["is_delete"] == 1) & (app_list["active"] == 1)]
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"fetch_app failed\n{self.logger.get_slim_error_log()}", "system")

    def run_app(self, app):
        """
        Execute application (python {app_name}.py)

        Args:
            app(pandas series object): Appliation info

        Returns:
            None

        Example:
            >>> app_manager_obj = appManager()
            >>> app_manager_obj.run_app()
        """
        try:
            self.app = app
            process = subprocess.Popen(f"{app['launch_command']}", shell=True)  # noqa: S602
            self.app["pid"] = process.pid
            self.app["last_exec_time"] = datetime.now()
            # Record the last exection time
            self.update_app_active(self.app["app_uuid"], self.app["app_name"], self.app["pid"], self.app["last_exec_time"])
            self.logging.info(f"Run App {self.app['app_name'].rstrip()} | {self.app['pid']} | {self.app['last_exec_time']}")
            self.influxdb_obj.write_log_influxdb("INFO", f"Run App {self.app['app_name'].rstrip()} | {self.app['pid']} | {self.app['last_exec_time']}", "all")
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"Run app {self.app['app_name']}\n{self.logger.get_slim_error_log()}", "system")

    def update_app_active(self, app_uuid, app_name, pid, last_exec_time):
        """
        Update watchdog state for pid, active and last execution time

        Args:
            app: App info

        Returns:
            None

        Example:
            >>> app_manager_obj = appManager()
            >>> app_manager_obj.update_app_active(app_uuid, app_name, pid, last_exec_time)
        """
        try:
            sql_cmd = text("""
            UPDATE Watchdog
            SET pid = :pid, last_exec_time = :last_exec, active = :active
            WHERE app_uuid = :app_uuid
            """)
            var = {"pid": pid, "last_exec": last_exec_time, "active": 1, "app_uuid": app_uuid}
            self.mssql_obj.update_db_pd(sql_cmd, var)
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"App [{app_name}]\n{self.logger.get_slim_error_log()}", "system")

    def update_app_inactive(self, app_uuid, app_name):
        """
        Update app state for inactive

        Args:
            uuid: App uuid

        Returns:
            None

        Example:
            >>> app_manager_obj = appManager()
            >>> app_manager_obj.update_app_inactive(app_uuid, app_name)
        """
        try:
            sql_cmd = text("""
            UPDATE app
            SET active = :inactive
            WHERE app_uuid = :app_uuid
            """)
            var = {"inactive": 0, "app_uuid": app_uuid}
            self.mssql_obj.update_db_pd(sql_cmd, var)
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"App [{app_name}]\n{self.logger.get_slim_error_log()}", "system")

    def is_process_running(self, pid, app_name):
        """
        Check if the process is running

        Args:
            pid: The process id of the running app
            app_name: App Name

        Returns:
            App process status

        Example:
            >>> app_manager_obj = appManager()
            >>> app_manager_obj.is_process_running(pid, app_name)
        """
        try:
            p = psutil.Process(pid)
            return p.is_running() and p.status() != psutil.STATUS_ZOMBIE
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"is_process_running failed [{app_name}]\n{self.logger.get_slim_error_log()}", "system")
            return False

    def schedule_app(self):
        """
        Application Scheduler (Background Execution Application)

        Args:
            None

        Returns:
            None

        Example:
            >>> app_manager_obj = appManager()
            >>> app_manager_obj.schedule_app()
        """
        while True:
            try:
                inactive_apps, active_apps = self.fetch_app()
                for _, app in inactive_apps.iterrows():
                    threading.Thread(target=self.run_app, args=(app,)).start()
                    time.sleep(2)
                for _, app in active_apps.iterrows():
                    if app["is_delete"] == 1:
                        parent = psutil.Process(app["pid"])
                        children = parent.children(recursive=True)
                        # Terminate child process
                        for child in children:
                            if self.is_process_running(child.pid, app["app_name"]):
                                child.terminate()
                        parent.terminate()
                        # Set the app as inactive
                        self.update_app_inactive(app["app_uuid"], app["app_name"])
                    elif not self.is_process_running(app["pid"]):
                        # Set the app as inactive
                        self.update_app_inactive(app["app_uuid"], app["app_name"])
                # Scan frequency is 1 second
                time.sleep(1)
            except Exception:
                self.influxdb_obj.write_log_influxdb("ERROR", f"Schedule app {self.app['app_name']}\n{self.logger.get_slim_error_log()}", "system")


if __name__ == "__main__":
    try:
        # Run and Manage Applications
        app_manager_obj = appManager()
        app_thread = threading.Thread(target=app_manager_obj.schedule_app)
        app_thread.start()
        # app_manager_obj.init_influxdb_connection()
        app_thread.join()
    except Exception:
        exc_type, exc_value, exc_tb = sys.exc_info()
        tb_summary = traceback.extract_tb(exc_tb)
        last_call = tb_summary[-1]

        err_msg = f"{exc_type.__name__}: {exc_value}"
        logging.error(f"Execute WatchDog Failed\nFile {last_call.filename}, line {last_call.lineno}, in {last_call.name}: {err_msg}")
