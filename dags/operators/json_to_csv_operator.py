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
            if df is None:
                df = pd.read_json(json_file, orient="records", typ="series").to_frame().T
            else:
                dfx = pd.read_json(json_file, orient="records", typ="series").to_frame().T
                df = pd.concat([df, dfx])
        logging.info(f"Final DataFrame shape: {df.shape}")
        logging.info(f"Final DataFrame columns: {df.columns}")
        if not os.path.exists(os.path.dirname(self.csv_filename)):
            os.makedirs(os.path.dirname(self.csv_filename))
        df.to_csv(self.csv_filename, index=False)
        logging.info(f"CSV File: {self.csv_filename}")
        logging.info("JsonToCsvOperator complete!")

    def list_json_files(self):
        return glob.glob(self.json_filename)