from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
from airflow.operators.python import PythonOperator
import os
import boto3,logging, io

start_date = datetime(2023, 3, 16)

default_args = {
    'owner': 'train',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}
current_hour = datetime.now().hour
today = datetime.now().strftime('%Y%m%d')
current_ts = datetime.now().strftime('%Y%m%d_%H%M%S')
original_file_name1 = "hour_"
original_file_name2 = "_supermarket_sales"
original_file_extension = "csv"
file_directory = "/opt/airflow_directory/"



with DAG('supermarket1_dag', default_args=default_args, schedule_interval='@hourly', catchup=False) as dag:

    t1 = BashOperator(task_id='download_file',
                      bash_command=f"curl -o {file_directory}{original_file_name1}{current_hour}{original_file_name2}.{original_file_extension} 'http://localhost:5000/data/{original_file_name1}{current_hour}{original_file_name2}.{original_file_extension}'",
                      retries=2, retry_delay=timedelta(seconds=15))

    t1