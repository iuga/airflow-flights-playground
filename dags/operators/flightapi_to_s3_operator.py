import json
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from airflow.models.baseoperator import BaseOperator
import requests
import logging


class FlightApiToS3Operator(BaseOperator):
    """
    Flight API to S3 Operator
    """
    api_url = 'http://api.aviationstack.com/v1/flights?access_key='

    @apply_defaults
    def __init__(self, s3_bucket:str, api_key:str, s3_folder="esteban.delboca/", s3_conn_id="my_conn_S3", **kwargs) -> None:
        super().__init__(**kwargs)
        self.s3_bucket = s3_bucket
        self.s3_folder = s3_folder
        self.api_key = api_key
        self.s3 = S3Hook(s3_conn_id)

    def execute(self, context):
        """
        Execute the operator.
        """
        logging.info("FlightApiToS3Operator starting...")
        r = requests.get('{}{}'.format(self.api_url, self.api_key))
        if r.status_code == 200:
            response = r.json()
            if 'data' in response:
                flights_data = response['data']
                for flight in flights_data:
                    content = self.parse_payload(flight)
                    key = self.generate_file_key(
                        content['airline_code'], content['flight_number'], content['flight_date']
                    )
                    self.upload_to_s3(content=json.dumps(content), key=key)
        else:
            logging.error(f"There was an error fetching the API {r.status_code} : {r.json()}")
        logging.info("FlightApiToS3Operator complete")

    def upload_to_s3(self, content:str, key:str) -> None:
        """
        Wrapper to upload the content to a file in S3

        :param content to upload to s3 in a file
        :key s3 key to use
        """
        logging.info(f"FlightApiToS3Operator: Uploading: {content} to S3 with key: {key}")
        self.s3.load_string(
            string_data=content,
            key=key,
            bucket_name=self.s3_bucket,
            replace=True
        )
        logging.info("FlightApiToS3Operator: Uploading to S3 complete")

    def parse_payload(self, payload:dict) -> dict:
        """
        Parse the payload and return the most important fields

        :param payload dict from the API
        :returns the parsed flat dict
        """
        return {
            'flight_number': payload['flight']['number'],
            'flight_date': payload['flight_date'],
            'flight_status': payload['flight_status'],
            'departure_airport': payload['departure']['airport'],
            'arrival_airport': payload['arrival']['airport'],
            'airline_name': payload['airline']['name'],
            'airline_code': payload['airline']['iata'],
        }

    def generate_file_key(self, airline_code, flight_number, flight_date):
        """
        Generate the json file key ( name ) using the flight number and date

        :param flight_number like 345
        :param flight_date like 2021-02-23
        :return str with the key to use
        """
        return '{}flight_{}_{}{}.json'.format(
            self.s3_folder, flight_date.replace("-", ""), airline_code, flight_number
        )