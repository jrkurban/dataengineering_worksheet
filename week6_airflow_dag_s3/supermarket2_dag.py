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

s3_client = boto3.client('s3',
                         aws_access_key_id='dataops',
                      aws_secret_access_key='Ankara06',
                         endpoint_url='http://localhost:9000')

s3_res = boto3.resource('s3',
                         aws_access_key_id='dataops',
                      aws_secret_access_key='Ankara06',
                         endpoint_url='http://localhost:9000')

df = pd.read_csv(f"/opt/airflow_directory/hour_{current_hour}_supermarket_sales.csv")

def save_df_to_s3(s3_res, df, bucket, key):
    ''' Store df as a buffer, then save buffer to s3'''
    try:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        # s3_resource = boto3.resource('s3')
        s3_res.Object(bucket, key).put(Body=csv_buffer.getvalue())
        logging.info(f'{key} saved to s3 bucket {bucket}')
    except Exception as e:
        raise logging.exception(e)





with DAG('supermarket2_dag', default_args=default_args, schedule_interval='@hourly', catchup=False) as dag:


    t5 = PythonOperator(
        task_id='save_to_s3',
        python_callable=save_df_to_s3,
        op_kwargs={
            's3_res': s3_res,
            'df': df,
            'bucket': "dataops",
            'key': f"supermarket_sales/{str(today)}/hour_{str(current_hour)}_supermarket_sales.csv"
        }
    )


t5