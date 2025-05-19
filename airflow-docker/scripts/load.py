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
        
        engine = create_engine(
             f"{'postgresql'}+{'psycopg2'}://{creds['user']}"
            f":{creds['password']}@{creds['host']}"
            f":{creds['port']}/{creds['database']}"
        )

        return engine
    
    def upload_parquet_in_chunks(self, parquet_file, table_name, engine, chunksize=100_000):

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
        


