import pandas as pd
import pyarrow.parquet as pa
import logging
import os


logger = logging.getLogger(__name__)
logging.basicConfig(level= logging.INFO)



class Transform:

    def __init__(self):
        pass


    def load_raw_data(self, file):
        
        batch_dataframe = []
        parquet_file = pa.ParquetFile(file)

        for i, batch in enumerate(parquet_file.iter_batches(batch_size=350)):
            logger.info(f'Batch {i} loaded!')
            batch_dataframe.append(batch.to_pandas())

        return pd.concat(batch_dataframe, ignore_index=True)
    
    def clean_data(self, df):
        
        df.dropna(how= 'any', inplace= True)
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], errors= 'coerce')
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'], errors= 'coerce')
        df.drop(columns=['RatecodeID', 'store_and_fwd_flag'], axis= 1, inplace= True)

        return df
    
    def save_processed_data(self, cleaned_df):

        processed_dir = '/opt/airflow/scripts/Data/Processed'
        os.makedirs(processed_dir, exist_ok=True)

        output_path = os.path.join(processed_dir, 'cleaned_data.parquet')
        cleaned_df.to_parquet(output_path, engine= 'pyarrow', compression= 'snappy')
        
        logger.info(f'Cleaned data saved to {output_path}')





if __name__ == '__main__':

    raw_file_path = r'C:\Users\jorda\OneDrive\Documents\nyc-taxi-pipeline-1\Data\Raw\data.parquet'

    transformer = Transform()
    df = transformer.load_raw_data(raw_file_path)
    cleaned_df = transformer.clean_data(df)
    transformer.save_processed_data(cleaned_df)
            

