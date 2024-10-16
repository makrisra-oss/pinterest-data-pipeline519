import requests
import json
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
from datetime import datetime
import yaml


random.seed(100)

def load_db_credentials():
        with open('db_creds_streaming.yaml', 'r') as file:
            data = yaml.load(file, Loader=yaml.FullLoader)
        return data

creds = load_db_credentials()
print("CREDS: ", creds)

class AWSDBConnector:
    def __init__(self, creds):

        self.HOST = creds['HOST']
        self.USER = creds['USER']
        self.PASSWORD = creds['PASSWORD']
        self.DATABASE = creds['DATABASE']
        self.PORT = creds['PORT']
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector(creds)

def convert_datetime_to_string(data):
    for key, value in data.items():
        if isinstance(value, datetime):
            data[key] = value.isoformat()  # Convert datetime to ISO 8601 string
    return data

def send_data_to_api(data, stream_name):



    invoke_url = f"https://ufkcjan9a0.execute-api.us-east-1.amazonaws.com/dev/streams/{stream_name}/record"



    """Send the data to the specified API endpoint."""
    headers = {'Content-Type': 'application/json'}
    


    # Convert datetime objects to strings
    data = convert_datetime_to_string(data)
    
    # Prepare the payload
    payload = json.dumps({
    "StreamName": f"{stream_name}",
    "Data": data,
    "PartitionKey": "shard_1"
})
    
    # Print the payload for debugging
    print("Sending data to API:")
    print(payload)
    
    # Send the data to the API
    response = requests.request("PUT", invoke_url, headers=headers, data=payload)

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

            send_data_to_api(data=pin_result, stream_name="streaming-0affc56add51-pin")  # Send to the Pinterest topic
            send_data_to_api(data=geo_result, stream_name="streaming-0affc56add51-geo")  # Send to the Geolocation topic
            send_data_to_api(user_result, stream_name="streaming-0affc56add51-user") # Send to the User topic

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')