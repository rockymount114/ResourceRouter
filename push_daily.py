import pandas as pd
import requests, os
from dotenv import load_dotenv
from DB import DatabaseManager
from query import initial_query, daily_query
from sqlalchemy import text


def upload_to_api(csv_file):
    url = "https://hooks.prismatic.io/trigger/SW5zdGFuY2VGbG93Q29uZmlnOmIyYzk4MWMyLTEyZDItNDExYS05ZTNiLTc1MGYzNzIzMGJmYg=="
    headers = {
        "Content-Type": "text/csv",
        "Api-Key": 'd799bc3ea87b182112eeb787f4c5a876'
    }
    with open(csv_file, 'rb') as f:
        response = requests.post(url, headers=headers, data=f)
    return response


if __name__ == "__main__":
    response = upload_to_api('initial_data.csv')
    print(f'Data upload response: {response.status_code} - {response.text}')