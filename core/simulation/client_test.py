from remote_mode_api import RemoteBatteryModeAPI
import time
import logging


logging.basicConfig(level=logging.INFO)

# Server IP and port
uri = "PYRO:battery_mode_api@192.168.127.100:8888"

remote_api = RemoteBatteryModeAPI(uri)

# 1. Connect device
res = remote_api.connect_devices()
logging.info(res)

# 2. Run each mode
for i in range(100):
    logging.info("====== Loop", i, "======")
    remote_api.mode_battery_exit()

    logging.info("Power on")
    remote_api.mode_battery_entry()
    time.sleep(10)

    logging.info("CC mode")
    remote_api.mode_cc_charge()

    logging.info("Rest Mode")
    remote_api.mode_idle()

    logging.info("Power off")
    remote_api.mode_battery_exit()
