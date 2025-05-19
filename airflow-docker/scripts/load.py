import yaml
import logging
from sqlalchemy import create_engine
import pandas as pd


logger = logging.getLogger(__name__)
logging.basicConfig(level= logging.INFO)


class Load:

    def __init__(self):
        pass


    def read_db_credentials(self, yaml_file):

        """This method safely loads the yaml file checking if nothing is missing.
        
        Args:
            yaml_file(YML): takes yaml file holding credentials as a paramenter
            
        Return:
            Returns the credentials within this file
            
        Raise:
            Raises an error if the file fails to open or is not mathcing what is required"""


        with open(yaml_file, 'r') as file:
            try:
                config = yaml.safe_load(file)
                creds = config.get('postgres', {})
            
                required_keys = ['host', 'port', 'database', 'user', 'password']
                missing_keys = [k for k in required_keys if k not in creds]

                if missing_keys:
                    raise ValueError(f'Missing keys in YAML: {missing_keys}')
                
                return creds
            
            except yaml.YAMLError as e:
                logger.warning(f'Failed to open requested file: {e}')
                raise
                

            


    def db_engine(self, creds):

        """This function creates the engine to successfully connect to the local database.
        
        Args:
            creds(YML): takes the credentials as a parameter
            
        Return:
            Returns the engine"""
        
        engine = create_engine(
             f"{'postgresql'}+{'psycopg2'}://{creds['user']}"
            f":{creds['password']}@{creds['host']}"
            f":{creds['port']}/{creds['database']}"
        )

        return engine
    
    def upload_parquet_in_chunks(self, parquet_file, table_name, engine, chunksize=100_000):

        """This method converts the data to a dataframe, then iterates through each chunk of
        data converting to sql and moving it to the local database.
        
        Args:
            parquet_file(parquet): takes the data as a parameter
            table_name(string): sets the table name of the data
            engine: takes the engine function as parameter
            chunksize(int): sets each chunksize to 100000mb
            
        Return:
            Returns the data to pgadmin
            
        Raise:
            Raises each chunk uploaded, data successfully loaded or if the data failed to load"""

        try:
       
            df = pd.read_parquet(parquet_file)

            for i in range(0, len(df), chunksize):
                chunk = df.iloc[i:i + chunksize]

                chunk.to_sql(
                    name=table_name,
                    con=engine,
                    if_exists='append' if i > 0 else 'replace',
                    index=False
                )

            logger.info(f'Uploaded chunk {(i // chunksize) + 1}')

            logger.info(f"Data successfully loaded into table '{table_name}'")

        except Exception as e:
            logger.exception(f'Failed during chunked parquet upload: {e}')
            raise
        


