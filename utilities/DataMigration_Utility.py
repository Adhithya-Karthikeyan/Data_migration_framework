import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.\
        config("spark.jars", '/Users/adhithya/test_projects/jar_files/postgresql-42.5.1.jar').\
        config("spark.executor.extraClassPath", "/Users/adhithya/test_projects/jar_files/postgresql-42.5.1.jar").\
        getOrCreate()


class DataMigration_Utility:
    
    def __init__(self, creds, postgres_table_details, read_mode = None, csv_file = None, local_path = None):
        self.read_creds = creds['read']
        self.write_creds = creds['write']

        self.postgres_table_details = postgres_table_details
        self.local_path = local_path
        self.csv_file = csv_file
        self.read_mode = read_mode
        self.process()
        
    def process(self):
        self.initialise_vars()
        
        if self.read_mode == 'local':
            df = self.read_from_local()
            
        elif self.read_mode == 'postgres':
            df = self.read_from_postgres()
            
        else:
            print("Please select valid read mode")
            
        self.write_to_postgres(df)
            
    def initialise_vars(self):

        
        # Fetching the database and table detauls
        self.table_name = self.postgres_table_details['table_name']
        database_name = self.postgres_table_details['database_name']

        # Fetching the creds to read from the DB
        self.r_username = self.read_creds['username']
        self.r_password = self.read_creds['password']
        self.r_url = self.read_creds['url']
        self.r_driver = self.read_creds['driver']
        self.r_jar_path = self.read_creds['jar_file_path']
        self.r_url = "{r_url}{database_name}".format(
                                                    r_url = self.r_url, \
                                                    database_name = database_name
                                                )

        # Fetching the creds to write to the DB
        self.w_username = self.read_creds['username']
        self.w_password = self.read_creds['password']
        self.w_url = self.read_creds['url']
        self.w_driver = self.read_creds['driver']
        self.w_jar_path = self.read_creds['jar_file_path']
        self.w_url = "{w_url}{database_name}".format(
                                                    r_url = self.w_url, \
                                                    database_name = database_name
                                                )

    def read_from_postgres(self):
        print("#### Reading tables from the DB ####")
        try:
            df = spark.read\
                        .format('jdbc')\
                        .options(url = self.r_url,\
                                dbtable =self.table_name,\
                                user = self.r_user,\
                                password = self.r_password,\
                                driver = self.r_driver)\
                        .load()
            print("#### Status --> Success ####")
            return df
        except Exception as err:
            raise err
            
    def read_from_local(self):
        print("#### Reading tables from local ####")
        csv_read_path = "{local_path}/{csv_file}".format(local_path = self.local_path, \
                                                        csv_file = self.csv_file)
        
        
        try:
            df = spark.read.csv(csv_read_path, header = True)
            print("#### Status --> Success ####")
            return df
        except Exception as err:
            raise err
            
            
    def write_to_postgres(self, df):
        print("#### Writing the table to DB ####")
        
        try:
            
            df.write.format('jdbc').\
                    options(
                                url = self.w_url,
                                driver = w_driver,\
                                dbtable = self.table_name,\
                                user = self.w_user,\
                                password = self.w_password
                           ).\
                    mode("overwrite").\
                    save()
            
            print("#### Status --> Success ####")
            
        except Exception as err:
            raise err

