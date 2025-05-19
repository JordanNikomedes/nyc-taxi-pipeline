# nyc-taxi-pipeline

My aim in this project was to build a reliable ETL pipeline experimenting with docker and airflow to automate the process. In the progress I made building this project I managed to practice coding up my own pipeline in python to solidify my knowledge and to make sure I am career ready. During the project I was very aware about the improvements I needed to add to my code, so consequently I was experimenting with the logging library to ensure there were clear messages and exceptions when running the pipeline. In addition I learnt to retrieve and load a large dataset in chunks for a more efficient and memory optimized process. I think my biggest learning curve was using docker and creating a DAG that actually works. I have never had hands-on experience with using these tools, with 2 days worth of trial and error the orchestration was finally a success.


## Table of Contents

- [Project Overview](#project-overview)
- [Tech Stack](#tech-stack)
- [Environment Setup](#environment-setup)
- [Data Ingestion](#data-ingestion)
- [Data Transformation](#data-transformation)
- [Data Loading](#data-loading)
- [DAG Orchestration](#dag-orchestration)
- [SQL Queries](#sql-queries)
- [Conclusion](#conclusion)


## Project Overview

This project implements a robust ETL (Extract, Transform, Load) pipeline using Apache Airflow, designed to process and analyze NYC Taxi trip data. The pipeline reads raw Parquet files, performs data cleaning, and loads the results into a PostgreSQL database, all orchestrated via Airflow DAGs.

RAW PARQUET FILES → Extract (Airflow) → Clean/Transform (Pandas) → Load (PostgreSQL)


## Tech Stack

- **Airflow**: Task orchestration
- **PostgreSQL**: Data warehouse
- **Docker**: Containerization for all services
- **Pandas / PyArrow**: Data processing
- **SQLAlchemy**: Database connections
- **YAML**: Secure config management for credentials


## Environment Setup

To ensure organisation I created all the folders and files I needed before writing up any code. I first created the 'airflow-docker' folder for the DAG I wanted to create and got the 'docker-compose' yaml file ready to build the environment. Finally I created a separate folder called scripts and created my 3 python files - 'ingest', 'transform', 'load'.


## Data Ingestion

The source of this data was from the nyc.gov website under tlc trip record data. So to start this pipeline I made a new class 'Ingest' with 2 functions inside including the initalisation. Within the init function I made 2 global parameters for the local file path and the url from where I am receiving the data. I use the requests library to perform the retrieval of the taxi data in chunks to make the process a lot more efficient. I also used the try and except to raise any errors when calling this class and applied the logging library for a cleaner messaging system rather than using print.

<p align="center">
  <img src="\images\Screenshot 2025-05-19 164101.png" alt="Ingest Class" width="750">
</p>


## Data Transformation

Once the data was loaded into my local folder I started the transformation phase where I loaded that raw data within the class as smaller batches at a certain bite size in an empty array to priortize the memory optimization. The array was then concatenated into one dataframe ready for the cleaning process. 
To start with I got rid of all null values in the table, then in both the pickup and dropoff columns I corrected the format to make them the datetime structure. Finally I dropped 2 columns that were to relevant to the table. The new table was finally saved to the subfolder, processed, under a new name to separate both datasets.

<p align="center">
  <img src="\images\Screenshot 2025-05-19 171214.png" alt="Transform Class" width="750">
</p>


## Data Loading

With loading the data to my local database I first created a separate yaml file with my database credentials. Then within the python file I import the sqlalchemy, yaml and pandas library ready. In the class I first read the credentinals by loading the file safely, I use the try and except with an if statement in case there are any values missing. The next function initiates the engine. The final function uploads the newly cleaned dataset in chunks so the DAG can run smoothly.

<p align="center">
  <img src="\images\Screenshot 2025-05-19 172329.png" alt="Load Class1" width="750">
</p>

<p align="center">
  <img src="\images\Screenshot 2025-05-19 172340.png" alt="Load Class2" width="750">
</p>


## DAG Orchestration

After days of trial and error the DAG was completed by writing out the code within the DAG folder. In the python file I made 3 functions calling each class adjusting the file path to airflow. As this file was containerized I was getting errors importing the classes, however they were ignored as once the docker container was up and running there were no error. Finally I scheduled my newly created DAG performing each task using the python operator and triggered it watching the process happen.

<p align="center">
  <img src="\images\Screenshot 2025-05-18 145908.png" alt="DAG" width="750">
</p>


## SQL Queries

What were the top 10 routes by trip count?

<p align="center">
  <img src="\images\Screenshot 2025-05-19 122500.png" alt="SQLQ1" width="750">
</p>


What was the average fare by pickup hour?

<p align="center">
  <img src="\images\Screenshot 2025-05-19 150913.png" alt="SQLQ2" width="750">
</p>


What was the longest average trip duration by day of the week?

<p align="center">
  <img src="\images\Screenshot 2025-05-19 152446.png" alt="SQLQ3" width="750">
</p>


## Conclusion

In conclusion I feel a lot more confident coding my own pipeline all the way from ingestion to loading. However I do think I will need more practice automating the process and expanding my knowledge to testing and monitoring the DAG and integrate Great Expectations or Pandera.


