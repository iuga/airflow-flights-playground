from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.s3_file_transform_operator import S3FileTransformOperator
from operators.flightapi_to_s3_operator import FlightApiToS3Operator
from operators.json_to_csv_operator import JsonToCsvOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
import datetime
import requests
import collections
import logging
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    's3_bucket': 'fligoo.data-science',
    's3_folder': 'esteban.delboca',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily'
}

timestamp = '{{ ts_nodash }}'

with DAG(
    dag_id='landing_collect_flights',
    default_args=default_args,
    description='Pulls data from an API and store the result in S3',
    catchup=False
) as dag:

    flights_to_s3 = FlightApiToS3Operator(
        task_id='flights_to_s3',
        api_key='cc45fd7ae7407347986ba919853d2191',
        s3_folder='{}/landing/'.format(default_args['s3_folder'])
    )

    install_requirements = BashOperator(
        task_id = "install_requirements",
        bash_command='python -m pip install awscli'
    )

    download_flights = BashOperator(
        task_id = "download_flights",
        bash_command='aws s3 cp s3://{}/{}/landing /opt/airflow/data/landing/ --recursive --exclude "*" --include "flight_20210224*"'.format(
            default_args['s3_bucket'], default_args['s3_folder']
        )
    )

    flights_json_to_table = JsonToCsvOperator(
        task_id='json_flights_to_csv',
        json_filename='/opt/airflow/data/landing/flight_20210224*', 
        csv_filename='/opt/airflow/data/aggregated/flight_20210224.csv.gz'
    )

    upload_dataset_to_s3 =  BashOperator(
        task_id = "upload_dataset_to_s3",
        bash_command='aws s3 cp /opt/airflow/data/aggregated/flight_20210224.csv.gz s3://{}/{}/aggregated/'.format(
            default_args['s3_bucket'], default_args['s3_folder']
        )
    )

    flights_to_s3 >> install_requirements >> download_flights >> flights_json_to_table >> upload_dataset_to_s3
