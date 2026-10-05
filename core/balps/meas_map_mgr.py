import xml.etree.ElementTree as ET  # noqa: S405
import pandas as pd
import copy
import json
import os
import sys
from balps.log_mgr import logging_api
from sqlalchemy import text
from datetime import datetime

"""
Query Measurement Mapping Table Information
"""
class TaskInfo:
    """
    Manage task information data.
    """

    def __init__(self):
        """
        Initialize task information with default values.
        """
        try:
            self.data = {
                "MessageName": {"0": ""},
                "TransactionID": {"0": ""},
                "TrxID": {"0": ""},
                "LineID": {"0": ""},
                "StoreHouseID": {"0": 0},
                "SerialBoardID": {"0": ""},
                "Step": {"0": 0},
                "ControlMode": {"0": ""},
                "PalletID": {"0": ""},
                "PalletPosition": {"0": ""},
                "Position": {"0": 0},
                "QRCodeID": {"0": ""},
                "ProtectBoardID": {"0": ""},
                "StopStep": {"0": ""},
                "ReturnCode": {"0": ""},
                "ReturnMessage": {"0": ""}
            }

        except Exception as e:
            raise RuntimeError(f"Failed to initialize TaskInfo: {e}")

    def get_data(self):
        """
        Return all task information.
        """
        try:
            return self.data
        except Exception as e:
            raise RuntimeError(f"Failed to get task information: {e}")

    def get(self, key, index="0"):
        """
        Get a task information value.

        Args:
            key: Task information key.
            index: Task information index.

        Returns:
            Task information value.
        """
        try:
            return self.data[key][index]
        except Exception as e:
            raise RuntimeError(f"Failed to get task information: {e}")

    def set(self, key, value, index="0"):
        """
        Set a task information value.

        Args:
            key: Task information key.
            value: New value.
            index: Task information index.
        """
        try:
            self.data[key][index] = value
        except Exception as e:
            raise RuntimeError(f"Failed to set task information: {e}")

    def reset(self):
        """
        Reset all task information.
        """
        try:
            self.__init__()
        except Exception as e:
            raise RuntimeError(f"Failed to reset task information: {e}")
    
    def convert_all_to_string(self, data):
        """
        Recursively convert all values to strings.

        Args:
            data: Input dictionary, list, or value.

        Returns:
            Converted data with string values.
        """
        try:
            if isinstance(data, dict):
                return {
                    key: self.convert_all_to_string(value)
                    for key, value in data.items()
                }

            if isinstance(data, list):
                return [
                    self.convert_all_to_string(value)
                    for value in data
                ]

            if data is None:
                return ""

            if isinstance(data, str):
                return data

            return str(data)

        except Exception as e:
            raise RuntimeError(f"Failed to convert data to string: {e}")

class meas_map_api:
    def __init__(self, influxdb_obj, protect_params=None, sb_id="ccms", pb_id=""):
        """
        Setup measurement mapping table

        Args:
            None

        Returns:
            None

        Example:
            >>> meas_map_obj = meas_map_api()
        """
        self.meas_map_pd = pd.DataFrame()
        self.config_map = pd.DataFrame()
        self.task_map = {}
        self.serial_board_list = {}
        self.influxdb_obj = influxdb_obj
        self.sb_id = sb_id
        self.pb_id = pb_id
        if protect_params is None:
            config_path = os.path.join(os.path.dirname(__file__), "..", "fc", "FlowControl.json")
            with open(config_path, encoding="utf-8") as json_file:
                protect_params = json.load(json_file)["meas"][0]["protect_params"]
        self.protect_params = protect_params
        # Create task information object
        self.task_info = TaskInfo()
        self.logger = logging_api(filename=os.path.join("logs", f"{os.path.splitext(os.path.basename(sys.argv[0]))[0]}"), backup_count=7)
        self.logging = self.logger.get_logger()  # Create influxdb and mssql object

    def get_meas_map(self):
        """
        Get measurement mapping table

        Args:
            None

        Returns:
            Measurement mapping table

        Example:
            >>> meas_map_obj = meas_map_api()
            >>> meas_map = meas_map_obj.get_meas_map()
        """
        return copy.deepcopy(self.meas_map_pd)

    def get_config_map(self):
        """
        Get config mapping table

        Args:
            None

        Returns:
            Config mapping table

        Example:
            >>> meas_map_obj = meas_map_api()
            >>> meas_map = meas_map_obj.get_config_map()
        """
        return copy.deepcopy(self.config_map)

    def get_task_info(self):
        """
        Get task information

        Args:
            None

        Returns:
            Task information

        Example:
            >>> meas_map_obj = meas_map_api()
            >>> task_info = meas_map_obj.get_task_info()
        """
        return copy.deepcopy(self.task_info)

    def query_meas_map_by_id(self, storehouse_id):
        """
        Get measurement mapping table by store house id

        Args:
            storehouse_id: Store house id for meas map

        Returns:
            Meas Map Table

        Example:
            >>> meas_map_obj = meas_map_api()
            >>> meas_map = meas_map_obj.query_meas_map_by_id(storehouse_id)
        """
        try:
            return self.meas_map_pd[(self.meas_map_pd["StoreHouseID"] == int(storehouse_id))]
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"Failed to query meas map using RackID [{storehouse_id}]\n{self.logger.get_slim_error_log()}", "system")
            self.logging.error(f"Failed to query meas map using RackID [{storehouse_id}]\n{self.logger.get_slim_error_log()}")

    def query_meas_map_by_serialboard(self, serialboard_id, position_id):
        """
        Get store house id, pallet id, and qrcode id

        Args:
            serialboard_id: Sensing serial board id
            position_id: Sensing position id

        Returns:
            Filtered meas map includes storehouse id pallet id and qrcode

        Example:
            >>> meas_map_obj = meas_map_api()
            >>> meas_map = meas_map_obj.query_meas_map_by_id(mssql)
        """
        try:
            filtered_meas_map = self.meas_map_pd[(self.meas_map_pd["SerialBoardID"] == serialboard_id) & (self.meas_map_pd["Position"] == position_id)]
            if not filtered_meas_map.empty:
                return {"StoreHouseID": filtered_meas_map.iloc[0]["StoreHouseID"], "PalletID": filtered_meas_map.iloc[0]["PalletID"], "QRCodeID": filtered_meas_map.iloc[0]["QRCodeID"]}
            else:
                return None
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"Failed to query meas map using SerialBoardID [{serialboard_id}] and PositionID[{position_id}]\n{self.logger.get_slim_error_log()}", "system")
            self.logging.error(f"Failed to query meas map using SerialBoardID [{serialboard_id}] and PositionID[{position_id}]\n{self.logger.get_slim_error_log()}")
            raise

    def get_serial_board_id_list(self, storehouse_id):
        """
        Get left and right serial board ids according to the store house id

        Args:
            storehouse_id: Store house id for meas map

        Returns:
            Left and right serial board id

        Example:
            >>> meas_map_obj = meas_map_api()
            >>> serial_board_id = meas_map_obj.get_serial_board_id_list(storehouse_id)
        """
        try:
            return self.serial_board_list[storehouse_id]
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"Failed to get SerialBoardID list\n{self.logger.get_slim_error_log()}", "system")
            self.logging.error(f"Failed to get SerialBoardID list\n{self.logger.get_slim_error_log()}")

    def parse_serial_board_id(self, root):
        """
        Parse the left and right serial board ids of each store house

        Args:
            root: Xml request

        Returns:
            Left and right serial board ids

        Example:
            >>> meas_map_obj = meas_map_api()
            >>> serial_board_id_list = meas_map_obj.parse_serial_board_id(root)
        """
        try:
            pallet_info = []
            for pallet in root.findall(".//Pallet"):
                serial_board_id = pallet.find("SerialBoardID").text
                pallet_info.append(serial_board_id)

            return pallet_info
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"Failed to parse SerialBoardID\n{self.logger.get_slim_error_log()}", "system")
            self.logging.error(f"Failed to parse SerialBoardID\n{self.logger.get_slim_error_log()}")


    def update_device_info(self, mssql_obj, xml_string):
        """
        Update device information

        Args:
            mssql_obj: MSSQL object
            xml_string: Xml request string

        Returns:
            None

        Example:
            >>> meas_map_obj = meas_map_api()
            >>> meas_map_obj.update_device_info(mssql_obj, xml_string)
        """
        try:
            root = ET.fromstring(xml_string)  # noqa: S314

            storehouse_id = int(root.find(".//StoreHouseID").text)

            pallet_info = []
            for pallet in root.findall(".//Pallet"):
                pallet_id = pallet.find("PalletID").text
                serial_board_id = pallet.find("SerialBoardID").text
                protect_board_id = pallet.find("DRCID").text
                pallet_position = pallet.find("PalletPosition").text
                for qrcode in pallet.findall(".//QRCode"):
                    data = {
                        "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                        "StoreHouseID": storehouse_id,
                        "PalletID": pallet_id,
                        "SerialBoardID": serial_board_id,
                        "ProtectBoardID": protect_board_id,
                        "PalletPosition": pallet_position,
                        "Position": int(qrcode.find("Position").text),
                        "QRCodeID": qrcode.find("QRCODEID").text,
                    }
                    pallet_info.append(data)

            sql = text("DELETE FROM [dbo].[ServerMap] WHERE StoreHouseID = :StoreHouseID")
            var = {"StoreHouseID": storehouse_id}
            mssql_obj.delete_db_table(sql, var)
            mssql_obj.write_db_pd(pd.DataFrame(pallet_info), "ServerMap")
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"Update ServerMap failed\n{self.logger.get_slim_error_log()}", "system")
            self.logging.error(f"Update ServerMap failed\n{self.logger.get_slim_error_log()}")

    def get_required_text(self, parent, tag):
        value = parent.findtext(tag)

        if value is None or not value.strip():
            return ""
        else:
            #raise ValueError(f"Required XML tag '{tag}' is missing or empty.")
            return value.strip()
    
    def get_required_int(self, parent, tag):
        value = self.get_required_text(parent, tag)
        try:
            return int(value)
        except ValueError:
            raise ValueError(f"XML tag '{tag}' is not a valid integer: '{value}'.")

    # def parse_protect_params(self, recipe_step):
    #     """Parse and scale recipe protection parameters using FlowControl.json."""
    #     xml_tags = {element.tag.lower(): element.tag for element in recipe_step}
    #     parsed_params = {}
    #     for key in self.protect_params:
    #         xml_tag = xml_tags.get(key.lower(), key)
    #         value = self.get_required_text(recipe_step, xml_tag)
    #         multiplier = self.protect_params[key]
    #         if multiplier == "":
    #             self.influxdb_obj.write_log_influxdb("ERROR", f"Protect key is empty | [{value}]", self.sb_id)
    #         elif value == "":
    #             parsed_params[key] = str(value)
    #         else:
    #             parsed_params[key] = str(int(float(value) * multiplier))
    #     return parsed_params
    
    def parse_protect_params(self, recipe_step):
        """Parse and validate recipe protection parameters using FlowControl.json."""
        try:
            parsed_params = {}

            # XML keys that are not protection parameters.
            excluded_xml_keys = {
                # "Step",
                # "Control_Mode",
            }

            # ---------------------------------------------------------
            # 1. Get XML keys with original case and reject duplicates.
            # ---------------------------------------------------------
            xml_elements = [
                element
                for element in recipe_step
                if element.tag not in excluded_xml_keys
            ]
            xml_key_list = [element.tag for element in xml_elements]
            duplicate_xml_keys = sorted({
                key
                for key in xml_key_list
                if xml_key_list.count(key) > 1
            })

            if duplicate_xml_keys:
                duplicate_messages = []
                for key in duplicate_xml_keys:
                    values = [
                        (element.text or "").strip()
                        for element in xml_elements
                        if element.tag == key
                    ]
                    if len(set(values)) == 1:
                        duplicate_messages.append(
                            f"{key} repeated {len(values)} times with same value "
                            f"[{values[0]}]"
                        )
                    else:
                        duplicate_messages.append(
                            f"{key} repeated with different values {values}"
                        )

                raise KeyError(
                    "Duplicate XML protect parameter key | "
                    + " | ".join(duplicate_messages)
                )

            xml_keys = set(xml_key_list)

            # ---------------------------------------------------------
            # 2. Get protect parameter keys with original case.
            # ---------------------------------------------------------
            protect_keys = set(self.protect_params.keys())

            # ---------------------------------------------------------
            # 3. Validate empty key in protect_params.
            # ---------------------------------------------------------
            if "" in protect_keys:
                raise ValueError(
                    f"Invalid protect parameter: empty key | "
                    f"Value: [{self.protect_params['']}]"
                )

            # ---------------------------------------------------------
            # 4. Detect case mismatch.
            #
            # Example:
            # XML     : Cell_Min_Current
            # Protect : cell_Min_Current
            # ---------------------------------------------------------
            xml_lower_map = {
                key.lower(): key
                for key in xml_keys
            }

            protect_lower_map = {
                key.lower(): key
                for key in protect_keys
            }

            case_mismatch = []

            for lower_key in xml_lower_map.keys() & protect_lower_map.keys():

                xml_key = xml_lower_map[lower_key]
                protect_key = protect_lower_map[lower_key]

                if xml_key != protect_key:
                    case_mismatch.append(
                        (xml_key, protect_key)
                    )

            if case_mismatch:
                mismatch_messages = [
                    (
                        f"XML Key: [{xml_key}] | "
                        f"Protect Key: [{protect_key}]"
                    )
                    for xml_key, protect_key in case_mismatch
                ]

                raise KeyError(
                    "Key case mismatch | "
                    + " ; ".join(mismatch_messages)
                )

            # ---------------------------------------------------------
            # 5. Detect actual missing keys.
            # ---------------------------------------------------------

            # protect_params has key, but XML does not.
            missing_in_xml = protect_keys - xml_keys

            # XML has key, but protect_params does not.
            missing_in_protect = xml_keys - protect_keys

            if missing_in_xml or missing_in_protect:

                error_messages = []

                if missing_in_xml:
                    error_messages.append(
                        f"Missing in XML: {sorted(missing_in_xml)}"
                    )

                if missing_in_protect:
                    error_messages.append(
                        f"Missing in protect_params: "
                        f"{sorted(missing_in_protect)}"
                    )

                raise KeyError(
                    "Protect parameter key mismatch | "
                    + " | ".join(error_messages)
                )

            # ---------------------------------------------------------
            # 6. Parse protection parameters.
            # ---------------------------------------------------------
            for key in self.protect_params:

                value = self.get_required_text(
                    recipe_step,
                    key
                )

                multiplier = self.protect_params[key]

                # -----------------------------------------------------
                # XML empty value means protection is disabled.
                # -----------------------------------------------------
                if value is None or str(value).strip() == "":
                    parsed_params[key] = ""
                    continue

                # -----------------------------------------------------
                # No scaling required.
                # -----------------------------------------------------
                if multiplier == "":
                    parsed_params[key] = str(value)
                    continue

                # -----------------------------------------------------
                # 7. Validate multiplier is numeric.
                # -----------------------------------------------------
                try:
                    multiplier_value = float(multiplier)

                except (TypeError, ValueError):
                    raise ValueError(
                        f"Invalid protect multiplier | "
                        f"Key: [{key}] | "
                        f"Multiplier: [{self.protect_params[key]}]"
                    )

                # -----------------------------------------------------
                # 8. Validate XML value is numeric and apply scaling.
                # -----------------------------------------------------
                try:
                    parsed_params[key] = str(
                        int(float(value) * multiplier_value)
                    )

                except (TypeError, ValueError):
                    raise ValueError(
                        f"Invalid recipe parameter value | "
                        f"Key: [{key}] | "
                        f"Value: [{value}]"
                    )

            return parsed_params

        except Exception as ex:
            self.influxdb_obj.write_log_influxdb(
                "ERROR",
                f"Parse protect params failed | {ex}",
                self.sb_id
            )
            raise
    
    def parse_pallet_info(self, xml_string):
        """
        Parse pallet information

        Args:
            xml_string: Xml request string

        Returns:
            Device information

        Example:
            >>> meas_map_obj = meas_map_api()
            >>> device_info = meas_map_obj.parse_pallet_info(xml_string)
        """
        try:
            root = ET.fromstring(xml_string)  # noqa: S314

            storehouse_id = self.get_required_text(root, ".//StoreHouseID")
            pallet_info = []
            for pallet in root.findall(".//Pallet"):
                pallet_id = self.get_required_text(pallet, "PalletID")
                serial_board_id = self.get_required_text(pallet, "SerialBoardID")
                drc_id = self.get_required_text(pallet, "DRCID")
                pallet_position = self.get_required_text(pallet, "PalletPosition")
                for qrcode in pallet.findall(".//QRCode"):
                    qrcode_data = {
                        "StoreHouseID": storehouse_id,
                        "PalletID": pallet_id,
                        "SerialBoardID": serial_board_id,
                        "DRCID": drc_id,
                        "PalletPosition": pallet_position,
                        "Position": self.get_required_int(qrcode, "Position"),
                        "QRCodeID": self.get_required_text(qrcode, "QRCODEID"),
                    }
                    pallet_info.append(qrcode_data)
            return pd.DataFrame(pallet_info)
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"Parse pallet info failed\n{self.logger.get_slim_error_log()}", "system")
            self.logging.error(f"Parse pallet info failed\n{self.logger.get_slim_error_log()}")
            raise

    def parse_config_info(self, xml_string):
        """
        Parse config information

        Args:
            xml_string: Xml request string

        Returns:
            Config information

        Example:
            >>> meas_map_obj = meas_map_api()
            >>> config_info = meas_map_obj.parse_config_info(xml_string)
        """
        try:
            root = ET.fromstring(xml_string)  # noqa: S314

            recipe_info = []
            for pallet in root.findall(".//Pallet"):
                storehouse_id = self.get_required_text(root, ".//StoreHouseID")
                serial_board_id = self.get_required_text(pallet, "SerialBoardID")
                pallet_position = self.get_required_text(pallet, "PalletPosition")
                protect_board_id = self.get_required_text(pallet, "DRCID")

                for recipe_step in root.findall(".//RecipeStep"):
                    protect_params = self.parse_protect_params(recipe_step)
                    step_data = {
                        "StoreHouseID": storehouse_id,
                        "SerialBoardID": serial_board_id,
                        "PalletPosition": pallet_position,
                        "ProtectBoardID": protect_board_id,
                    }
                    step_data.update(protect_params)
                    recipe_info.append(step_data)
            return pd.DataFrame(recipe_info)
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"Parse W2002 Config failed\n{self.logger.get_slim_error_log()}", "system")
            self.logging.error(f"Parse W2002 Config failed\n{self.logger.get_slim_error_log()}")
            raise

    def get_xml_text(self, root, path, default=None):
        """
        Check and get xml text

        Args:
            root: Xml request
            path: Xml key

        Returns:
            Value of xml key

        Example:
            >>> meas_map_obj = meas_map_api()
            >>> meas_map_obj.get_xml_text(root, path)
        """
        try:
            element = root.find(path)
            if element is not None and element.text is not None:
                return element.text.strip()
            return default
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"Failed to get xml text[{path}]\n{self.logger.get_slim_error_log()}", "system")
            self.logging.error(f"Failed to get xml text[{path}]\n{self.logger.get_slim_error_log()}")

    def update_task_info_pbid(self, pb_id):
        """
        Update ProtectBoardID for task information

        Args:
            pb_id: ProtectBoardID string

        Returns:
            None

        Example:
            >>> meas_map_obj = meas_map_api()
            >>> meas_map_obj.update_task_info_pbid(pb_id)
        """
        try:
            self.task_info.set("ProtectBoardID", pb_id)
            # self.task_info["ProtectBoardID"] = {"0": pb_id}

        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"Failed to update task info\n{self.logger.get_slim_error_log()}", "system")
            self.logging.error(f"Failed to update task info\n{self.logger.get_slim_error_log()}")
            raise

    def update_task_info(self, xml_string):
        """
        Update task information

        Args:
            xml_string: Xml request string

        Returns:
            None

        Example:
            >>> meas_map_obj = meas_map_api()
            >>> meas_map_obj.update_task_info(xml_string)
        """
        try:
            root = ET.fromstring(xml_string)  # noqa: S314
            # Get the key corresponding to the value
            extract_keys = {
                "MessageName": "./HEADER/MESSAGENAME",
                "TransactionID": "./HEADER/TRANSACTIONID",
                "TrxID": "./BODY/TRX_ID",
                "LineID": "./BODY/LINE_ID",
                "StoreHouseID": "./BODY/StoreHouseID",
                "SerialBoardID": "./BODY/PalletInfo/Pallet/SerialBoardID",
                "PalletID": "./BODY/PalletInfo/Pallet/PalletID",
                "PalletPosition": "./BODY/PalletInfo/Pallet/PalletPosition",
                "Step": "./BODY/STEP",
            }

            # Set the corresponding value according to key
            for key, xpath in extract_keys.items():
                element = root.find(xpath)
                if element is not None and element.text is not None:
                    self.task_info.set(key, element.text.strip())
                    # self.task_info[key] = {"0": element.text.strip()}

            # Set the value of the last step of the recipe
            recipe_steps = root.findall("./BODY/RecipeInfo/RecipeStep")
            if recipe_steps:
                last_step = recipe_steps[-1].find("Step")
                self.task_info.set("StopStep", last_step.text if last_step is not None else "0")
                # self.task_info["StopStep"] = {"0": last_step.text if last_step is not None else "0"}

        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"Failed to update task info\n{self.logger.get_slim_error_log()}", "system")
            self.logging.error(f"Failed to update task info\n{self.logger.get_slim_error_log()}")
            raise

    def delete_serial_board_id_entry(self, storehouse_id):
        """
        Delete serial board ids for store house

        Args:
            storehouse_id: Store house id

        Returns:
            None

        Example:
            >>> meas_map_obj = meas_map_api()
            >>> meas_map_obj.delete_serial_board_id_entry(storehouse_id)
        """
        try:
            if storehouse_id in self.serial_board_list:
                del self.serial_board_list[storehouse_id]
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"Failed to delete SerialBoardIDs from Rack {storehouse_id}\n{self.logger.get_slim_error_log()}", "system")
            self.logging.error(f"Failed to delete SerialBoardIDs from Rack {storehouse_id}\n{self.logger.get_slim_error_log()}")

    def add_serial_board_id_entry(self, storehouse_id, entry):
        """
        Add serial board ids for store house

        Args:
            storehouse_id: Store house id
            entry: Serial board id list

        Returns:
            None

        Example:
            >>> meas_map_obj = meas_map_api()
            >>> meas_map_obj.add_serial_board_id_entry(storehouse_id, entry)
        """
        try:
            if storehouse_id not in self.serial_board_list:
                self.serial_board_list[storehouse_id] = []
                for data in entry:
                    self.serial_board_list[storehouse_id].append(data)
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"Failed to add SerialBoardID [{entry}] to Rack {storehouse_id}\n{self.logger.get_slim_error_log()}", "system")
            self.logging.error(f"Failed to add SerialBoardID [{entry}] to rack cache\n{self.logger.get_slim_error_log()}")

    def check_storehouse_exists(self, storehouse_id):
        """
        Check if the store house exists

        Args:
            storehouse_id: Store house id

        Returns:
            Exist or not

        Example:
            >>> meas_map_obj = meas_map_api()
            >>> status = meas_map_obj.check_storehouse_exists(storehouse_id)
        """
        try:
            if self.meas_map_pd.empty:
                return False
            if "StoreHouseID" in self.meas_map_pd.columns:
                return storehouse_id in self.meas_map_pd["StoreHouseID"].values
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"Failed to check rack {storehouse_id}\n{self.logger.get_slim_error_log()}", "system")
            self.logging.error(f"Failed to check rack {storehouse_id}\n{self.logger.get_slim_error_log()}")
            raise

    def add_storehouse_entry(self, storehouse_id, entry):
        """
        Add serial board ids for store house

        Args:
            storehouse_id: Store house id
            entry: Serial board id list

        Returns:
            None

        Example:
            >>> meas_map_obj = meas_map_api()
            >>> meas_map_obj.add_storehouse_entry(storehouse_id, entry)
        """
        try:
            if not self.check_storehouse_exists(storehouse_id):
                self.meas_map_pd = pd.concat([self.meas_map_pd, entry], ignore_index=True)
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"Failed to add rack {storehouse_id}\n{self.logger.get_slim_error_log()}", "system")
            self.logging.error(f"Failed to add rack {storehouse_id}\n{self.logger.get_slim_error_log()}")
            raise

    def delete_storehouse_entry(self, storehouse_id):
        """
        Delete serial board ids for store house

        Args:
            storehouse_id: Store house id

        Returns:
            None

        Example:
            >>> meas_map_obj = meas_map_api()
            >>> meas_map_obj.delete_storehouse_entry(storehouse_id)
        """
        try:
            if self.check_storehouse_exists(storehouse_id):
                self.meas_map_pd = self.meas_map_pd[self.meas_map_pd["StoreHouseID"] != storehouse_id]
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"Failed to delete rack {storehouse_id}\n{self.logger.get_slim_error_log()}", "system")
            self.logging.error(f"Failed to delete rack {storehouse_id}\n{self.logger.get_slim_error_log()}")
            raise

    def check_config_exists(self, serial_board_id):
        """
        Check if config exists

        Args:
            serial_board_id: Serial board id

        Returns:
            Exist or not

        Example:
            >>> meas_map_obj = meas_map_api()
            >>> status = meas_map_obj.check_config_exists(serial_board_id)
        """
        try:
            if self.config_map.empty:
                return False
            if "SerialBoardID" in self.config_map.columns:
                return serial_board_id in self.config_map["SerialBoardID"].values
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"Failed to check config [{serial_board_id}] exists\n{self.logger.get_slim_error_log()}", "system")
            self.logging.error(f"Failed to check config [{serial_board_id}] exists\n{self.logger.get_slim_error_log()}")
            raise

    def add_config_entry(self, serial_board_id, entry):
        """
        Add config for serial board

        Args:
            serial_board_id: Serial board id
            entry: Config

        Returns:
            None

        Example:
            >>> meas_map_obj = meas_map_api()
            >>> meas_map_obj.add_config_entry(serial_board_id)
        """
        try:
            if not self.check_config_exists(serial_board_id):
                self.config_map = pd.concat([self.config_map, entry], ignore_index=True)
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"Failed to add config [{serial_board_id}] to config map\n{self.logger.get_slim_error_log()}", "system")
            self.logging.error(f"Failed to add config [{serial_board_id}] to config map\n{self.logger.get_slim_error_log()}")
            raise

    def delete_config_entry(self, serial_board_id):
        """
        Delete config for serial board

        Args:
            serial_board_id: Serial board id

        Returns:
            None

        Example:
            >>> meas_map_obj = meas_map_api()
            >>> meas_map_obj.delete_config_entry(serial_board_id)
        """
        try:
            if self.check_config_exists(serial_board_id):
                self.config_map = self.config_map[self.config_map["SerialBoardID"] != serial_board_id]
        except Exception:
            self.influxdb_obj.write_log_influxdb("ERROR", f"Failed to delete config [{serial_board_id}] from config map\n{self.logger.get_slim_error_log()}", "system")
            self.logging.error(f"Failed to delete config [{serial_board_id}] from config map\n{self.logger.get_slim_error_log()}")
            raise
