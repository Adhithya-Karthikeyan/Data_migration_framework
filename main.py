from utilities.DataMigration_Utility import DataMigration_Utility
from utilities.read_config import read_config


class main:

    def __init__(self, read_source, write_destination):
        self.pre_process()
        self.initialise_vars()
        self.process()

    def pre_process(self):
        try:
            self.config_json = read_config().config_json()
        except Exception as e:
            print("#### Error reading server config json info ####")
            raise e

    def initialise_vars(self):
        self.read_creds = self.config_json[read_source]
        self.write_creds = self.config_json[write_destination]


        self.creds = {
                    "read":self.read_creds, \
                    "write":self.write_creds
                }

        self.postgres_table_details = {
                                    "table_name":"titanic_gender_submission", \
                                    "database_name":"sample_datasets"
                                }

    def process(self):
        
        DataMigration_Utility(
                                self.creds, \
                                self.postgres_table_details, \
                                "local",\
                                "titanic_gender_submission.csv", \
                                "/Users/adhithya/Documents/test_data/datasets/titanic/"
                )

main("postgres","postgres")