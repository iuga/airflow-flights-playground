import json
import os
from airflow.utils.decorators import apply_defaults
from airflow.models.baseoperator import BaseOperator
import requests
import logging


class FlightApiToJsonOperator(BaseOperator):
    """
    Flight API to S3 Operator
    """
    template_fields = ['folder']
    api_url = 'http://api.aviationstack.com/v1/flights?access_key='

    @apply_defaults
    def __init__(self, folder:str, api_key:str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.api_key = api_key
        self.folder = folder

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
                    filename = self.generate_file_key(
                        flight['airline']['iata'], flight['flight']['number'], flight['flight_date']
                    )
                    self.save_json_file(content=flight, filename=filename)
        else:
            logging.error(f"There was an error fetching the API {r.status_code} : {r.json()}")
        logging.info("FlightApiToS3Operator complete")

    def save_json_file(self, content:str, filename:str) -> None:
        """
        Wrapper to write the json file

        :param content of the json file
        :filename local filename to use
        """
        if not os.path.exists(self.folder):
            os.makedirs(self.folder)
        with open('{}/{}'.format(self.folder, filename), 'w') as jfp:
            json.dump(content, jfp)

    def generate_file_key(self, airline_code, flight_number, flight_date):
        """
        Generate the json filename using the flight number and date

        :param flight_number like 345
        :param flight_date like 2021-02-23
        :return str with the filename to use
        """
        return 'flight_{}_{}{}.json'.format(
            flight_date.replace("-", ""), airline_code, flight_number
        )