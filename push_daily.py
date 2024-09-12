import pandas as pd
import requests, os
from dotenv import load_dotenv
from datetime import datetime
from DB import DatabaseManager
from query import initial_query, daily_query
from sqlalchemy import text
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")

def upload_to_api(csv_file):
    url = "https://hooks.prismatic.io/trigger/SW5zdGFuY2VGbG93Q29uZmlnOmIyYzk4MWMyLTEyZDItNDExYS05ZTNiLTc1MGYzNzIzMGJmYg=="
    headers = {
        "Content-Type": "text/csv",
        "Api-Key": API_KEY
    }
    with open(csv_file, 'rb') as f:
        response = requests.post(url, headers=headers, data=f)
    return response


if __name__ == "__main__":
    df_daily = pd.read_csv('daily_data.csv')
    response = upload_to_api('daily_data.csv')
    print(f'Data upload response: {response.status_code} - {response.text}')
    
    with open('log.txt', 'a') as f:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"{current_time}: {len(df_daily)} rows have been saved to daily_data.csv\n")