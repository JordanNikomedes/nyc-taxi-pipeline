import requests
import logging
import os

logger = logging.getLogger(__name__)
logging.basicConfig(level= logging.INFO)

class Ingest:

    def __init__(self, url, local_file_path):
        self.url = url
        self.local_file_path = local_file_path

    

    def retrieve_and_load_data(self):


        if os.path.exists(self.local_file_path):
            logger.info(f'File already exists at {self.local_file_path}. Skipping dowload.')
            return


        try:
            with requests.get(self.url, stream=True) as response:
                response.raise_for_status()
                with open(self.local_file_path, 'wb') as file:
                    for chunk in response.iter_content(chunk_size= 8192):
                        if chunk:
                            file.write(chunk)
            logger.info(f'File downloaded successfully to: {self.local_file_path}')
        except requests.exceptions.RequestException as e:
            logger.warning(f'Error downloading file: {e}')


if __name__ == '__main__':

    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet'
    local_file_path = r'C:\Users\jorda\OneDrive\Documents\nyc-taxi-pipeline-1\Data\Raw\data.parquet'

    ingest_instance = Ingest(url, local_file_path)
    ingest_instance.retrieve_and_load_data()