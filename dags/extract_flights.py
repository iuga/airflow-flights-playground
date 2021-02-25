from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.s3_file_transform_operator import S3FileTransformOperator
from operators.flights_api_to_json_operator import FlightApiToJsonOperator
from operators.json_to_csv_operator import JsonToCsvOperator
from airflow.providers.mysql.transfers.s3_to_mysql import S3ToMySqlOperator
from airflow.providers.amazon.aws.transfers.mysql_to_s3 import MySQLToS3Operator
from airflow.operators.bash import BashOperator
import datetime
import requests
import collections
import logging
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'email': ['esteban.delboca@fligoo.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily',
    's3_bucket': 'fligoo.data-science',
    's3_folder': 'esteban.delboca'
}

with DAG(
    dag_id='extract_flights',
    default_args=default_args,
    description='Pulls data from an API and store the result in S3',
    tags=['landing'],
    catchup=False
) as dag:

    flights_to_s3 = FlightApiToJsonOperator(
        task_id='landing.flights_to_json',
        folder='/opt/airflow/data/landing/{{ds_nodash}}',
        api_key='cc45fd7ae7407347986ba919853d2191'
    )

    upload_flights = BashOperator(
        task_id = "landing.upload_flights",
        bash_command='aws s3 cp /opt/airflow/data/landing/{{ ds_nodash }} s3://fligoo.data-science/esteban.delboca/landing/{{ ds_nodash }} --recursive --exclude "*" --include "flight_*"'
    )

    flights_json_to_table = JsonToCsvOperator(
        task_id='aggregated.json_flights_to_csv',
        json_filename='/opt/airflow/data/landing/{{ds_nodash}}/flight_*', 
        csv_filename='/opt/airflow/data/aggregated/flights_{{ds_nodash}}.csv'
    )

    upload_agregated_flights = BashOperator(
        task_id = "aggregated.upload_flights",
        bash_command='aws s3 cp /opt/airflow/data/aggregated/flights_{{ds_nodash}}.csv s3://fligoo.data-science/esteban.delboca/aggregated/'

    )

    dump_to_mysql = S3ToMySqlOperator(
        task_id = "optimize.dump_to_mysql",
        s3_source_key = "s3://fligoo.data-science/esteban.delboca/aggregated/flights_{{ds_nodash}}.csv",
        mysql_table = 'flights',
        mysql_extra_options="""
        FIELDS TERMINATED BY ','
        IGNORE 1 LINES
        """
    )

    export_ds_canceled_flights = MySQLToS3Operator(
        task_id = "optimize.export.canceled_flights",
        query = "SELECT airline_code, flight_number, flight_date FROM flights WHERE flight_status = 'cancelled';",
        s3_bucket = default_args['s3_bucket'],
        s3_key = "{}/optimized/canceled_flights_{{ds_nodash}}.csv".format(default_args['s3_folder']),
        header = True
    )

    export_ds_active_flights = MySQLToS3Operator(
        task_id = "optimize.export.active_flights",
        query = "SELECT airline_code, flight_number, flight_date FROM flights WHERE flight_status = 'active';",
        s3_bucket = '{{params.s3_bucket}}',
        s3_key = "{{params.s3_folder}}/optimized/active_flights_{{ds_nodash}}.csv",
        header = True,
        params = {
            's3_bucket': default_args['s3_bucket'],
            's3_folder': default_args['s3_folder']
        }
    )
    
    flights_to_s3 >> upload_flights >> flights_json_to_table >> upload_agregated_flights >> dump_to_mysql
    dump_to_mysql >> [export_ds_canceled_flights, export_ds_active_flights]
