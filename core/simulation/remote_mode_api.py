import Pyro5.api


class RemoteBatteryModeAPI:
    def __init__(self, uri):
        self.remote = Pyro5.api.Proxy(uri)

    def connect_devices(self):
        if not (self.connect("87001", "192.168.1.100")):
            return False
        if not (self.connect("PWR", "COM19")):
            return False
        if not (self.connect("CS", "COM20")):
            return False
        if not (self.connect("ELOAD", "192.168.1.101")):
            return False
        return True

    def connect(self, instrument_name, interface_param):
        return self.remote.connect(instrument_name, interface_param)

    def disconnect(self, instrument_name):
        return self.remote.disconnect(instrument_name)

    def mode_battery_entry(self):
        return self.remote.mode_battery_entry()

    def mode_cc_charge(self, duration=0):
        return self.remote.mode_cc_charge(duration)

    def mode_idle(self, duration=0):
        return self.remote.mode_idle(duration)

    def mode_battery_exit(self):
        return self.remote.mode_battery_exit()