import threading
import os
import sys
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from balps import task_mgr
from balps import message_mgr
from balps.log_mgr import logging_api

if __name__ == "__main__":
    try:
        logger = logging_api(filename=os.path.join("logs", f"{os.path.splitext(os.path.basename(sys.argv[0]))[0]}"), backup_count=7)
        logging = logger.get_logger()
        # Load Input Config
        with open("core\\main\\config.json") as json_file:
            config_data = json.load(json_file)
            # Create Task Manager Thread
            task_manager_obj = task_mgr.TaskManager(config_data["ccms_ip"])
            task_thread = threading.Thread(target=task_manager_obj.schedule_tasks)
            # Create Message Manager Thread
            msg_manager_obj = message_mgr.MessageManager(config_data["ccs_ip"], config_data["ccs_port"], task_manager_obj)
            message_thread = threading.Thread(target=msg_manager_obj.run)
            # task_operation_thread = threading.Thread(target=msg_manager_obj.handle_task_operation, daemon=True)
            # task_operation_thread.start()

            task_thread.start() #schedule_tasks
            message_thread.start()
            task_thread.join() #schedule_tasks
            message_thread.join()
            # task_operation_thread.join()
    except Exception:
        logging.error(f"CCMS start failed\n{logger.get_slim_error_log()}")
