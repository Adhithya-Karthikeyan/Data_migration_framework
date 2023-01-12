import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.\
        config("spark.jars", '/Users/adhithya/test_projects/jar_files/postgresql-42.5.1.jar').\
        config("spark.executor.extraClassPath", "/Users/adhithya/test_projects/jar_files/postgresql-42.5.1.jar").\
        getOrCreate()


class PushTablesToPostgres:
    
    def __init__(self, creds, postgres_table_details, read_mode = None, csv_file = None, local_path = None):
        self.user = creds['user']
        self.password = creds['password']
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
        
        self.table_name = self.postgres_table_details['table_name']
        database_name = self.postgres_table_details['database_name']
        self.url = "jdbc:postgresql://localhost:5432/{database_name}".format(database_name = database_name)

        
    def read_from_postgres(self):
        print("#### Reading tables from Postgres ####")
        try:
            df = spark.read\
                        .format('jdbc')\
                        .options(url = self.url,\
                                dbtable =self.table_name,\
                                user = self.user,\
                                password = self.password,\
                                driver = "org.postgresql.Driver")\
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
        print("#### Writing to Postgres ####")
        
        try:
            
            df.write.format('jdbc').\
                    options(
                                url = self.url,
                                driver = "org.postgresql.Driver",\
                                dbtable = self.table_name,\
                                user = self.user,\
                                password = self.password
                           ).\
                    mode("overwrite").\
                    save()
            
            print("#### Status --> Success ####")
            
        except Exception as err:
            raise err

