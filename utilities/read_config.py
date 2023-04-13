import json


class read_config:

    def __init__(self):
        self.server_config_json = self.read_config("server_config_json")
        self.table_config_json = self.read_config("table_config")
        self.config_json = self.merge_configs()

    def read_config(self, file_names):
        try:
            print("#### Reading server config json ###")
            with open("/configs/" + file_name + ".json",'r') as config_file:
                config_file = json.loads(file)
                return config_file
        except Exception as e:
            print("#### Config file not found/ Error reading server config json from the path /configs/config_json.json")
            raise e

    def merge_configs(self):
        try:
            print("#### Merging configs ####")
            self.server_config_json.update(self.table_config_json)
            return self.server_config_json
        except Exception as e:
            print("#### Error merging the config files ####")
