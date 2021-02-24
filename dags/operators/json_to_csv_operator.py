import json
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from airflow.models.baseoperator import BaseOperator
import requests
import logging
import glob
import pandas as pd
import os


class JsonToCsvOperator(BaseOperator):
    """
    Transform a list of Json files with the same format to CSV
    """
    template_fields = ['json_filename', 'csv_filename']

    @apply_defaults
    def __init__(self, json_filename:str, csv_filename:str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.json_filename = json_filename
        self.csv_filename = csv_filename

    def execute(self, context):
        """
        Execute the operator.
        """
        logging.info("JsonToCsvOperator starting...")
        json_files = self.list_json_files()
        df = None
        for json_file in json_files:
            content = self.read_parse_json(json_file)
            if df is None:
                df = pd.DataFrame([content])
            else:
                df = pd.concat([df, pd.DataFrame([content])])        
        logging.info(f"Final DataFrame shape: {df.shape}")
        logging.info(f"Final DataFrame columns: {list(df.columns)}")
        if not os.path.exists(os.path.dirname(self.csv_filename)):
            os.makedirs(os.path.dirname(self.csv_filename))
        df.to_csv(self.csv_filename, index=False)
        logging.info(f"CSV File: {self.csv_filename}")
        logging.info("JsonToCsvOperator complete!")

    def list_json_files(self) -> list:
        return glob.glob(self.json_filename)

    def read_parse_json(self, json_file:str) -> dict:
        """
        Load the json file into a dictionary.

        :param json_file as the absolute filename of the json
        :returns the parsed dict with the content
        """
        with open(json_file, 'r') as jfp:
            return self.parse_payload(json.load(jfp))

    def parse_payload(self, payload:dict) -> dict:
        """
        Parse the payload and return the most important fields

        :param payload dict from the API
        :returns the parsed flat dict
        """
        return {
            'airline_code': payload['airline']['iata'],
            'flight_number': payload['flight']['number'],
            'flight_date': payload['flight_date'],
            'flight_status': payload['flight_status'],
            'departure_airport': payload['departure']['airport'].replace(',', '') if payload['departure']['airport'] is not None else '',
            'arrival_airport': payload['arrival']['airport'].replace(',', '') if payload['arrival']['airport'] is not None else '',
            'airline_name': payload['airline']['name']   
        }