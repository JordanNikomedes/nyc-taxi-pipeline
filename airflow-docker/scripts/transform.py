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

        """This method loads the raw data in set batch sizes in an array, then concatenated to
        form a dataframe.
        
        Args:
            file(string): path to the data
            
        Returns: 
            a pandas dataframe
            
        Raises:
            raises information to terminal establishing each batch has loaded"""
        
        batch_dataframe = []
        parquet_file = pa.ParquetFile(file)

        for i, batch in enumerate(parquet_file.iter_batches(batch_size=350)): # sets size of batch
            logger.info(f'Batch {i} loaded!')
            batch_dataframe.append(batch.to_pandas())

        return pd.concat(batch_dataframe, ignore_index=True) # concats each batch to form one dataframe
    
    def clean_data(self, df):

        """This method drops all nulls and 2 columns and changes to datetime format.
        
        Args:
            df(dataframe): passes the raw data as a parameter
            
        Return:
            Returns the new dataframe after being cleaned
        """
        
        df.dropna(how= 'any', inplace= True)
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], errors= 'coerce')
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'], errors= 'coerce')
        df.drop(columns=['RatecodeID', 'store_and_fwd_flag'], axis= 1, inplace= True)

        return df
    
    def save_processed_data(self, cleaned_df):

        """This function saves the data in the processed subfolder, changing the name to cleaned_data.parquet
        and converted back to a parquet file.
        
        Args:
            cleaned_df(dataframe): passes the processed data as a parameter
            
        Return:
            Returns a parquet file moved to the data folder
            
        Raise:
            Raises the success of the saved data in the terminal"""

        processed_dir = '/opt/airflow/scripts/Data/Processed'
        os.makedirs(processed_dir, exist_ok=True) # makes directory if doesn't exist

        output_path = os.path.join(processed_dir, 'cleaned_data.parquet') # joins to make one path
        cleaned_df.to_parquet(output_path, engine= 'pyarrow', compression= 'snappy') 
        
        logger.info(f'Cleaned data saved to {output_path}')





if __name__ == '__main__':

    raw_file_path = r'C:\Users\jorda\OneDrive\Documents\nyc-taxi-pipeline-1\Data\Raw\data.parquet'

    transformer = Transform()
    df = transformer.load_raw_data(raw_file_path)
    cleaned_df = transformer.clean_data(df)
    transformer.save_processed_data(cleaned_df)
            

