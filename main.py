from utilities.PushTablesToPostgres import PushTablesToPostgres


class main:

    def __init__(self):
        self.initialise_vars()
        self.process()

    def initialise_vars(self):


        self.creds = {
                    'user':"postgres", \
                    "password":"Godspeed.123"
                }

        self.postgres_table_details = {
                                    "table_name":"titanic_gender_submission", \
                                    "database_name":"sample_datasets"
                                }

    def process(self):
        PushTablesToPostgres(
                                self.creds, \
                                self.postgres_table_details, \
                                "local",\
                                "titanic_gender_submission.csv", \
                                "/Users/adhithya/Documents/test_data/datasets/titanic/"
                )

main()