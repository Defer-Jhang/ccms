from datetime import datetime
import subprocess  # noqa: S404
import time
import threading
from sqlalchemy import text
import pandas as pd
import psutil
import re
import json
import uuid
from io import StringIO
import os
import sys
import shlex

"""
Task Manager
  Manage the operation and monitoring of multiple tasks
  Tasks are independently flow control programs, such as serial algorithms and device data monitoring.
"""


class TaskManager:
    def __init__(self, host):
        from .mssql_mgr import mssql_api
        from .influxdb_mgr import influxdb_api
        from .log_mgr import logging_api

        """
        Initialize db connection and task manager setting

        Args:
            None

        Returns:
            None

        Example:
            >>> task_obj = TaskManager()
        """
        try:
            self.logger = logging_api(filename=os.path.join("logs", f"{os.path.splitext(os.path.basename(sys.argv[0]))[0]}"), backup_count=7)
            self.logging = self.logger.get_logger()  # Create influxdb and mssql object
            self.ip = host
            self.mssql_obj = mssql_api()
            self.influxdb_obj = influxdb_api()
            self.mssql_obj.create_db_connect_pd()
            self.influxdb_obj.create_connect()
            self.ttl_unit = 1  # unit second
            # Clear all tasks
            self.mssql_obj.clear_task_table(is_all_clear=True)
            self.mssql_obj.delete_rack_status(self.mssql_obj, is_all_clear=True)
        except Exception:
            self.logging.error(f"Task manager initialization failed\n{self.logger.get_slim_error_log()}")

    def fetch_active_tasks(self):
        """
        Get a list of running tasks

        Args:
            None

        Returns:
            None

        Example:
            >>> task_obj = TaskManager()
            >>> task_obj.fetch_tasks()
        """
        try:
            query = """SELECT * FROM task WHERE is_delete = 0 AND active = 1"""
            task_list = self.mssql_obj.query_db_pd(query)
            if task_list is None:
                task_list = pd.DataFrame()
            return task_list
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"fetch_tasks failed\n{self.logger.get_slim_error_log()}", "system")
            return pd.DataFrame()

    def fetch_inactive_tasks(self):
        """
        Get a list of non running tasks

        Args:
            None

        Returns:
            None

        Example:
            >>> task_obj = TaskManager()
            >>> task_obj.fetch_inactive_tasks()
        """
        try:
            query = """SELECT * FROM task WHERE is_delete = 0 AND active = 0"""
            task_list = self.mssql_obj.query_db_pd(query)
            if task_list is None:
                task_list = pd.DataFrame()
            return task_list
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"fetch_inactive_tasks failed\n{self.logger.get_slim_error_log()}", "system")
            return pd.DataFrame()

    def fetch_active_delete_tasks(self):
        """
        Get a list of currently running tasks that are marked for deletion

        Args:
            None

        Returns:
            None

        Example:
            >>> task_obj = TaskManager()
            >>> task_obj.fetch_active_delete_tasks()
        """
        try:
            query = """SELECT * FROM task WHERE is_delete = 1 AND active = 1"""
            task_list = self.mssql_obj.query_db_pd(query)
            if task_list is None:
                task_list = pd.DataFrame()
            return task_list
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"fetch_active_delete_tasks failed\n{self.logger.get_slim_error_log()}", "system")
            return pd.DataFrame()

    def run_task(self, task):
        """
        Run tasks for list

        Args:
            task: Task info

        Returns:
            None

        Example:
            >>> task_obj = TaskManager()
            >>> task_obj.run_task(task)
        """
        try:
            args = shlex.split(
                task["launch_command"],
                posix=False,
            )

            # Add FlowControl.py path.
            args[0] = os.path.join(
                "core",
                "fc",
                args[0],
            )

            # Add FlowControl.json path.
            cfg_index = args.index("-cfg") + 1
            args[cfg_index] = os.path.join(
                "core",
                "fc",
                args[cfg_index],
            )

            # Build command.
            cmd = [sys.executable] + args

            # Execute Task
            process = subprocess.Popen(cmd)

            # Python process PID
            pid = process.pid

            last_exec_time = datetime.now()
            task_uuid = task["task_uuid"]
            task_name = task["task_name"]

            # Update the Task Status to Active
            self.update_task_active(
                task_uuid,
                task_name,
                pid,
                last_exec_time,
            )

            self.influxdb_obj.write_log_influxdb(
                "INFO",
                f"Run Task {task_name.rstrip()} | PID {pid}",
                task_name,
            )

            self.logging.info(
                f"Run Task {task_name.rstrip()} | PID {pid}"
            )

        except Exception:
            self.influxdb_obj.write_log_influxdb(
                "ERROR",
                f"run_task failed [{task['task_name']}]\n"
                f"{self.logger.get_slim_error_log()}",
                "system",
            )

    # def run_task(self, task):
    #     """
    #     Run tasks for list

    #     Args:
    #         task: Task info

    #     Returns:
    #         None

    #     Example:
    #         >>> task_obj = TaskManager()
    #         >>> task_obj.run_task(task)
    #     """
    #     try:
    #         # Add File Path
    #         fc_path = r"core\\fc"
    #         new_arg = re.sub(r"(\w+\.py)", fc_path + r"\\\1", re.sub(r"(\w+\.json)", fc_path + r"\\\1", task["launch_command"]))
    #         # Execute Task
    #         process = subprocess.Popen(f"python {new_arg}", shell=True)  # noqa: S602
    #         pid = process.pid
    #         last_exec_time = datetime.now()
    #         task_uuid = task["task_uuid"]
    #         task_name = task["task_name"]
    #         # Update the Task Status to Active
    #         self.update_task_active(task_uuid, task_name, pid, last_exec_time)
    #         self.influxdb_obj.write_log_influxdb("INFO", f"Run Task {task['task_name'].rstrip()} | {pid} | {last_exec_time}", task_name)
    #         self.logging.info(f"Run Task {task['task_name'].rstrip()} | {pid} | {last_exec_time}")
    #         # time.sleep(0.5)
    #     except Exception:
    #         self.influxdb_obj.write_log_influxdb("ERROR", f"run_task failed [{task['task_name']}]\n{self.logger.get_slim_error_log()}", "system")

    def check_task_ready(self, left_sbid, right_sbid, check_interval=0.1, max_attempts=1000):
        """
        Check task ready

        Args:
            left_sbid: Left SerialBoardID
            right_sbid: Right SerialBoardID
            check_interval: Check startup status interval
            max_attempts: Maximum number of attempts

        Returns:
            None

        Example:
            >>> client = xml_socket_api("127.0.0.1", 12345)
            >>> client.check_task_ready(left_sbid, right_sbid)
        """
        try:
            query = f"""
                    SELECT COUNT(*) 
                    FROM [dbo].[Task] 
                    WHERE [pid] > 0 
                    AND [task_name] IN ('{left_sbid}', '{right_sbid}')
                    AND [is_delete] = 0
                    """  # noqa: S608
            attempts = 0
            while attempts < max_attempts:
                result = self.mssql_obj.query_db_pd(query)
                # Check If the Left and Right SerialBoard of the StoreHouse are Ready for the Task
                if not result.empty and result.iloc[0, 0] == 2:
                    return True
                attempts += 1
                time.sleep(check_interval)
            return False
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"check_task_ready failed\n{self.logger.get_slim_error_log()}", "system")

    def update_task_step(self, left_sbid, right_sbid, sb_step, task_step):
        """
        Update task step

        Args:
            left_sbid: Left SerialBoardID
            right_sbid: Right SerialBoardID
            sb_step: Pallet Step XML
            task_step: Changed Charging Step

        Returns:
            None

        Example:
            >>> client = xml_socket_api("127.0.0.1", 12345)
            >>> client.update_task_step(left_sbid, right_sbid, sb_step, task_step)
        """
        try:
            sql_cmd = text("""
                UPDATE task
                SET task_step = :task_step, step_info = :sb_step, last_exec_time = CURRENT_TIMESTAMP
                WHERE is_delete = '0' AND task_name = :sbid
            """)
            left_params = {"task_step": task_step, "sb_step": sb_step, "sbid": left_sbid}
            right_params = {"task_step": task_step, "sb_step": sb_step, "sbid": right_sbid}
            self.mssql_obj.update_db_pd(sql_cmd, left_params)
            self.mssql_obj.update_db_pd(sql_cmd, right_params)
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"update_task_step failed\n{self.logger.get_slim_error_log()}", "system")

    def check_task_exists(self, left_sbid, right_sbid, shid):
        """
        Check if a task exists

        Args:
            left_sbid: Left SerialBoardID
            right_sbid: Right SerialBoardID
            shid: StoreHouseID

        Returns:
            bool: True if neither task exists; otherwise, False.

        Example:
            >>> client = xml_socket_api("127.0.0.1", 12345)
            >>> client.check_task_exists(left_sbid, right_sbid, shid)
        """
        try:
            query = f"""
            SELECT DISTINCT task_name
            FROM [BALPS].[dbo].[Task]
            WHERE is_delete = 0
            AND task_name IN ('{left_sbid}', '{right_sbid}')
            """  # noqa: S608

            result_df = self.mssql_obj.query_db_pd(query)

            existing_tasks = {
                str(task_name).rstrip()
                for task_name in result_df["task_name"].tolist()
            }

            existing_left_sbid = (
                str(left_sbid).rstrip()
                if str(left_sbid).rstrip() in existing_tasks
                else None
            )

            existing_right_sbid = (
                str(right_sbid).rstrip()
                if str(right_sbid).rstrip() in existing_tasks
                else None
            )

            existing_sbids = [
                sbid
                for sbid in (existing_left_sbid, existing_right_sbid)
                if sbid is not None
            ]

            # Either left_sbid or right_sbid already exists.
            if existing_sbids:
                existing_sbid_log = " | ".join(
                    f"'{sbid}'" for sbid in existing_sbids
                )

                self.influxdb_obj.write_log_influxdb(
                    "WARNING",
                    f"Task already exists | {existing_sbid_log}",
                    "system",
                )

            if existing_left_sbid or existing_right_sbid:
                return True
            else:
                return False
        except Exception as ex:
            self.influxdb_obj.write_log_influxdb("ERROR", f"check_task_exists failed\n{self.logger.get_slim_error_log()}", shid)
            raise ex

    def create_task(self, left_pallet, right_pallet, left_sbid, right_sbid, shid, left_pbid, right_pbid):
        """
        Create new task

        Args:
            left_sbid: Left SerialBoardID
            right_sbid: Right SerialBoardID
            shid: StoreHouseID

        Returns:
            None

        Example:
            >>> client = xml_socket_api("127.0.0.1", 12345)
            >>> client.create_task(left_sbid, right_sbid, shid)
        """
        try:
            
            l_pb_port = int(left_pbid[4:7])
            r_pb_port = int(right_pbid[4:7])

            json_data = json.dumps(
                [
                    {
                        "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                        "task_name": left_sbid,
                        "task_uuid": str(uuid.uuid4()).replace("-", "").upper(),
                        "description": f"measurement serialboard {left_sbid}",
                        "launch_command": f"FlowControl.py -cfg FlowControl.json -shid {shid} -sbid {left_sbid} -pbid {left_pbid} -ep {20000 + l_pb_port} -sip {self.ip}",
                        "pid": 0,
                        "ttl": 11400,
                        "pallet_info": left_pallet,
                        "task_step": 0,
                        "active": 0,
                        "is_delete": 0,
                    },
                    {
                        "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                        "task_name": right_sbid,
                        "task_uuid": str(uuid.uuid4()).replace("-", "").upper(),
                        "description": f"measurement serialboard {right_sbid}",
                        "launch_command": f"FlowControl.py -cfg FlowControl.json -shid {shid} -sbid {right_sbid} -pbid {right_pbid} -ep {20000 + r_pb_port} -sip {self.ip}",
                        "pid": 0,
                        "ttl": 11400,
                        "pallet_info": right_pallet,
                        "task_step": 0,
                        "active": 0,
                        "is_delete": 0,
                    },
                ],
                indent=4,
            )

            self.mssql_obj.write_db_pd(pd.read_json(StringIO(json_data)), "Task")
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"create_task failed\n{self.logger.get_slim_error_log()}", "system")

    def delete_task(self, sb_id):
        """
        Delete task

        Args:
            sbid_list: StoreHouseID corresponding to the left and right SerialBoardID

        Returns:
            None

        Example:
            >>> client = xml_socket_api("127.0.0.1", 12345)
            >>> client.update_task_step(sbid_list)
        """
        try:
            sql_cmd = text(f"""
                UPDATE task
                SET is_delete = '1'
                WHERE is_delete = '0' AND task_name = '{sb_id}';
            """)  # noqa: S608
            self.mssql_obj.update_db_pd(sql_cmd, None)
            self.influxdb_obj.write_log_influxdb("INFO", f"Delete Task {sb_id}", sb_id)
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"delete_task failed\n{self.logger.get_slim_error_log()}", sb_id)

    def update_task_active(self, task_uuid, task_name, pid, last_exec_time):
        """
        Update task state for pid, active and last execution time

        Args:
            task: Task info

        Returns:
            None

        Example:
            >>> task_obj = TaskManager()
            >>> task_obj.update_task(task)
        """
        try:
            sql_cmd = text("""
            UPDATE task
            SET pid = :pid, last_exec_time = :last_exec, active = :active
            WHERE task_uuid = :task_uuid
            """)
            var = {"pid": pid, "last_exec": last_exec_time, "active": 1, "task_uuid": task_uuid}
            self.mssql_obj.update_db_pd(sql_cmd, var)
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"Task [{task_name}]\n{self.logger.get_slim_error_log()}", "system")

    def update_task_inactive(self, task_uuid, task_name):
        """
        Update task state for inactive

        Args:
            uuid: Task uuid

        Returns:
            None

        Example:
            >>> task_obj = TaskManager()
            >>> task_obj.update_task(uuid)
        """
        try:
            sql_cmd = text("""
            UPDATE task
            SET active = :inactive
            WHERE task_uuid = :task_uuid
            """)
            var = {"inactive": 0, "task_uuid": task_uuid}
            self.mssql_obj.update_db_pd(sql_cmd, var)
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"Task [{task_name}]\n{self.logger.get_slim_error_log()}", "system")

    def is_process_running(self, pid, task_name):
        """
        Check if the process is running

        Args:
            pid: The process id of the running task
            task_name: Task Name

        Returns:
            Task process status

        Example:
            >>> task_obj = TaskManager()
            >>> task_obj.update_pid(task, pid)
        """
        try:
            p = psutil.Process(pid)
            return p.is_running() and p.status() != psutil.STATUS_ZOMBIE
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"is_process_running failed [{task_name}]\n{self.logger.get_slim_error_log()}", "system")
            return False

    def schedule_tasks(self):  # noqa: C901
        """
        Create a thread to run tasks in the inactive list and close the task list that meets the end conditions

        Args:
            None

        Returns:
            None

        Example:
            >>> task_obj = TaskManager()
            >>> task_obj.schedule_tasks()
        """
        while True:
            try:
                tasks = self.fetch_inactive_tasks()
                if tasks is not None and not tasks.empty:
                    for _, task in tasks.iterrows():
                        threading.Thread(target=self.run_task, args=(task,)).start()
                        time.sleep(0.1)
                tasks = self.fetch_active_tasks()
                if tasks is not None and not tasks.empty:
                    for _, task in tasks.iterrows():
                        try:
                            current_time = datetime.now()
                            # The task has exceeded its survival time
                            if (current_time - task["last_exec_time"]).seconds / self.ttl_unit > task["ttl"]:
                                self.influxdb_obj.write_log_influxdb("INFO", f"Remove Task [{task['task_name']}]", task["task_name"])
                                self.logging.info(f"Remove Task [{task['task_name']}]")
                                # Check task alive task is running
                                if pd.notna(task["pid"]) and self.is_process_running(task["pid"], task["task_name"]):
                                    self.update_task_inactive(task["task_uuid"], task["task_name"])
                                    self.delete_task(task["task_name"])
                                    parent = psutil.Process(task["pid"])
                                    children = parent.children(recursive=True)
                                    # Terminate child process
                                    for child in children:
                                        if self.is_process_running(child.pid, task["task_name"]):
                                            child.terminate()
                                    parent.terminate()
                                    self.influxdb_obj.write_log_influxdb("INFO", f"killed {task['task_uuid']} | {task['task_name']}", "all")
                        except psutil.NoSuchProcess:
                            pass
                        except Exception:
                            self.influxdb_obj.write_log_influxdb("ERROR", f"schedule_tasks failed [{task['task_name']}]\n{self.logger.get_slim_error_log()}", task["task_name"])
                # Removed tasks should be ended directly
                tasks = self.fetch_active_delete_tasks()
                if tasks is not None and not tasks.empty:
                    for _, task in tasks.iterrows():
                        try:
                            self.influxdb_obj.write_log_influxdb("INFO", f"Remove Task [{task['task_name']} PID: {task["pid"]}]", task["task_name"])
                            self.logging.info(f"Remove Task [{task['task_name']} PID: {task["pid"]}]")
                            self.update_task_inactive(task["task_uuid"], task["task_name"])
                            parent = psutil.Process(task["pid"])
                            children = parent.children(recursive=True)
                            # Terminate child process
                            for child in children:
                                if self.is_process_running(child.pid, task["task_name"]):
                                    child.terminate()
                            parent.terminate()
                        except Exception:
                            self.influxdb_obj.write_log_influxdb("ERROR", f"schedule_tasks failed [{task['task_name']}]\n{self.logger.get_slim_error_log()}", task["task_name"])
                        # Set the task as inactive
                # Scan frequency is 1 second
                time.sleep(1)
            except Exception:
                self.influxdb_obj.write_log_influxdb("ERROR", f"schedule_tasks failed [{task['task_name']}]\n{self.logger.get_slim_error_log()}", "system")
