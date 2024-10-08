import pandas as pd
import requests, os
from dotenv import load_dotenv
from DB import DatabaseManager, EmailManager
from query import initial_query, daily_query
from sqlalchemy import text
from dotenv import load_dotenv

load_dotenv()

EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")

if EMAIL_ADDRESS is None or EMAIL_PASSWORD is None:
    raise ValueError("EMAIL_ADDRESS and EMAIL_PASSWORD must be set in the environment variables.")
else:
    email_manager = EmailManager(EMAIL_ADDRESS, EMAIL_PASSWORD)

    email_manager.send_email("Test", "Test", "ip114@msn.com")

#### default upload to api
    
# def upload_to_api(csv_file):
#     url = API_URL
#     headers = {
#         "Content-Type": "text/csv",
#         "Api-Key": API_KEY
#     }
#     with open(csv_file, 'rb') as f:
#         response = requests.post(url=url, headers=headers, data=f, stream=True)
#     return response


# if __name__ == "__main__":
#     response = upload_to_api('initial_data.csv')
#     print(f'Data upload response: {response.status_code} - {response.text}')


# load_dotenv()

# SERVER_ADDRESS = os.getenv('CAD_ADDRESS')
# DATABASENAME = os.getenv('CAD_DATABASE')
# DB_USERNAME = os.getenv('CAD_USERNAME')
# DB_PASSWORD = os.getenv('CAD_PASSWORD')
# DB_PORT = os.getenv('DB_PORT')

# SERVER_ADDRESS = os.getenv('RMS_ADDRESS')
# DATABASENAME = os.getenv('RMS_DATABASE')
# DB_USERNAME = os.getenv('RMS_USERNAME')
# DB_PASSWORD = os.getenv('RMS_PASSWORD')

# # print(daily_query)

# db_manager = DatabaseManager(SERVER_ADDRESS, DATABASENAME, DB_USERNAME, DB_PASSWORD)

# if db_manager.engine:
#     query = initial_query
#     df = db_manager.fetch_data(db_manager.engine, query)
#     if df is not None:
#         db_manager.write_to_csv(df, 'initial_data.csv')
#         print(df)
# else:
#     print("Failed to create database engine.")

# from DB import DatabaseManager

# x = 2351763.75
# y = 802305.50

# data = DatabaseManager.get_cords(x, y)
# print(data)