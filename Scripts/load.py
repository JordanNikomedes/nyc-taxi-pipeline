import yaml
import logging
from sqlalchemy import create_engine


logger = logging.getLogger(__name__)
logging.basicConfig(level= logging.INFO)


class Load:

    def __init__(self):
        pass


    def read_db_credentials(self, yaml_file):


        with open(yaml_file, 'r') as file:
            try:
                cred = yaml.safe_load(file)
            
                required_keys = ['host', 'port', 'database', 'user', 'password']
                missing_keys = [k for k in required_keys if k not in yaml_file]

                if missing_keys:
                    raise ValueError(f'Missing keys in YAML: {missing_keys}')
                
                return cred
            
            except yaml.YAMLError as e:
                logger.warning(f'Failed to open requested file: {e}')
                raise
                

            


    def db_engine(self, cred):
        
        engine = create_engine(
             f"{'postgresql'}+{'psycopg2'}://{cred.get('user')}"
            f":{cred.get('password')}@{cred.get('host')}"
            f":{cred.get('port')}/{cred.get('database')}"
        )

        return engine
    
    def upload_to_db(self, table_name, engine, df):



        try:
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            logger.info(f'Data uploaded to table {table_name} successfully')
        
        except Exception as e:
            logger.exception(f'Failed to upload to database: {e}')


