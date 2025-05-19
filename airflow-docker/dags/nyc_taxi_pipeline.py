import sys
sys.path.append('/opt/airflow/scripts')

from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime

from ingest import Ingest 
from transform import Transform
from load import Load

import pandas as pd
import logging


logger = logging.getLogger(__name__)
logging.basicConfig(level= logging.INFO)



def ingest_callable():

    logger.info('Starting data ingestion...')
    
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet'
    local_file_path = '/opt/airflow/scripts/Data/Raw/data.parquet'

    ingest_instance = Ingest(url, local_file_path)
    ingest_instance.retrieve_and_load_data()

    logger.info('Data ingestion complete.')


def transform_callable():

    logger.info('Starting data transformation...')
    
    raw_file_path = '/opt/airflow/scripts/Data/Raw/data.parquet'

    transformer = Transform()
    df = transformer.load_raw_data(raw_file_path)
    cleaned_df = transformer.clean_data(df)
    transformer.save_processed_data(cleaned_df)

    logger.info('Data transformation complete.')


def load_callable(**kwargs):

    logger.info('Starting data loading...')
    
    
    yaml_path = '/opt/airflow/scripts/creds.yaml'
    cleaned_path = '/opt/airflow/scripts/Data/Processed/cleaned_data.parquet'

    loader = Load()
    
    creds = loader.read_db_credentials(yaml_path)
    engine = loader.db_engine(creds)

    loader.upload_parquet_in_chunks(
        parquet_file= cleaned_path,
        table_name = 'nyc_taxi_data',
        engine = engine,
        chunksize = 100_000

    )

    logger.info('Data successfully loaded')



default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

with DAG(
    'nyc_taxi_pipeline',
    default_args = default_args,
    schedule_interval = None,
    catchup = False,
    tags = ['nyc', 'etl', 'pipeline']
) as dag:
    

    ingest_task = PythonOperator(
        task_id = 'ingest_data',
        python_callable = ingest_callable

    )

    transform_task = PythonOperator(
        task_id = 'transform_data',
        python_callable = transform_callable
    )

    load_task = PythonOperator(
        task_id = 'load_data',
        python_callable = load_callable
    )

    ingest_task >> transform_task >> load_task