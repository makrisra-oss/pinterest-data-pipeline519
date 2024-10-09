import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
from datetime import datetime


random.seed(100)


class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()

invoke_url = "https://ufkcjan9a0.execute-api.us-east-1.amazonaws.com/dev"

def convert_datetime_to_string(data):
    for key, value in data.items():
        if isinstance(value, datetime):
            data[key] = value.isoformat()  # Convert datetime to ISO 8601 string
    return data

def send_data_to_api(invoke_url, data):
    """Send the data to the specified API endpoint."""
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    
    # Convert datetime objects to strings
    data = convert_datetime_to_string(data)
    
    # Prepare the payload
    data_payload = {
        "records": [
            {
                "key": None,  # Use a specific key if required by your Kafka topic
                "value": data  # Your actual data
            }
        ]
    }
    
    # Print the payload for debugging
    print("Sending data to API:")
    print(data_payload)
    
    # Send the data to the API
    response = requests.post(invoke_url, headers=headers, json=data_payload)

    if response.status_code == 200:
        print(f"Successfully sent data to {invoke_url}")
    else:
        print(f"Failed to send data. Status Code: {response.status_code}, Response: {response.text}")


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            print(pin_result)
            print(geo_result)
            print(user_result)

            send_data_to_api(f"{invoke_url}/topics/0affc56add51.pin", pin_result)  # Send to the Pinterest topic
            send_data_to_api(f"{invoke_url}/topics/0affc56add51.geo", geo_result)  # Send to the Geolocation topic
            send_data_to_api(f"{invoke_url}/topics/0affc56add51.user", user_result) # Send to the User topic

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')

