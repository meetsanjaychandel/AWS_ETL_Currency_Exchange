import requests
import boto3
import json
from datetime import date
from airflow.models import Variable



class Currency:
    def __init__(self):

        # AWS S3 Configuration
        self.s3_raw_bucket = Variable.get("s3_raw_bucket")
        self.s3_prefix = Variable.get("s3_prefix")

        # api_configuration
        self.api_url = Variable.get("api_url")
        self.params = {
            'vs_currency': 'usd',
            'order': 'market_cap_desc',
            'per_page': 10,
            'page': 1,
            'sparkline': False
        }
    

def fetch_data():
    response = requests.get(api_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")

def upload_to_s3(data):
    s3_client = boto3.client('s3')
    today = date.today().strftime("%Y-%m-%d")
    s3_key = f"{s3_prefix}/date={today}/crypto_data.json"

    s3_client.put_object(
        Bucket=s3_raw_bucket,
        Key=s3_key,
        Body=json.dumps(data),
        ContentType="application/json"
    )
    print(f" Uploaded data to S3: {s3_key}")

cur = Currency()
data = cur.fetch_data()
cur.upload_to_s3(data)
