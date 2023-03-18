import io
import logging
from datetime import datetime, timedelta

import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

start_date = datetime(2023, 3, 11)
today = datetime.now().strftime('%Y%m%d')
this_hour = int(datetime.now().strftime('%H'))
default_args = {
    'owner': 'train',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}


# pandas df to s3
s3_client = boto3.client('s3',
                         aws_access_key_id='airflow',
                      aws_secret_access_key='Ankara06',
                         endpoint_url='http://localhost:9000')


def save_df_to_s3(df, bucket, key, s3_client, index=False):
    ''' Store df as a buffer, then save buffer to s3'''
    try:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucket, Key=key)
        logging.info(f'{key} saved to s3 bucket {bucket}')
    except Exception as e:
        raise logging.exception(e)


def ingest_to_object_store(**kwargs):
    try:
        df = pd.read_csv(kwargs['data'])
    except:
        return 'http_request_error'

    if len(df) > 0:
        save_df_to_s3(df=df, bucket='dataops', key=f'supermarket_sales/{today}/hour_{this_hour}_supermarket_sales.csv', s3_client=kwargs['s3_client'])
        return 'saving_success'
    else:
        return 'not_enough_data'


with DAG('airflow_homework_dag', default_args=default_args, schedule_interval='@hourly', catchup=False,) as dag:
    ingest_to_object_store = BranchPythonOperator(task_id='ingest_to_object_store',
                                                  python_callable=ingest_to_object_store,
                                                  op_kwargs={'data': f'http://trainvm.vbo.local:5000/data/hour_{this_hour}_supermarket_sales.csv',
                                                             's3_client': s3_client},
                                                  do_xcom_push=False)

    http_request_error = DummyOperator(task_id='http_request_error')

    saving_success = DummyOperator(task_id='saving_success')

    not_enough_data = DummyOperator(task_id='not_enough_data')

    following_tasks = DummyOperator(task_id='following_tasks',
                                    trigger_rule='none_failed_or_skipped')

    following_another_tasks = DummyOperator(task_id='following_another_tasks')

    ingest_to_object_store >> [http_request_error, saving_success, not_enough_data] >> following_tasks >> following_another_tasks