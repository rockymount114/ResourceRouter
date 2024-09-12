import os
import pandas as pd
import requests
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv
from datetime import datetime
from time import sleep

from DB import DatabaseManager
from query import initial_query, daily_query

load_dotenv()

SERVER_ADDRESS = os.getenv('RMS_ADDRESS')
DATABASENAME = os.getenv('RMS_DATABASE')
DB_USERNAME = os.getenv('RMS_USERNAME')
DB_PASSWORD = os.getenv('RMS_PASSWORD')

db_manager = DatabaseManager(SERVER_ADDRESS, DATABASENAME, DB_USERNAME, DB_PASSWORD)
    
if __name__ == "__main__":    
    if db_manager.engine:        
        # get initial 5 years data
        
        # query = initial_query # Set query here
        # df = db_manager.fetch_data(db_manager.engine, query)
        # if df is not None:
        #     db_manager.write_to_csv(df, 'initial_data.csv')
        #     print(f"{len(df)} rows have been saved to the csv file")
            
        
        # get last 15 days data save to daily_data.csv
        
        query_daily = daily_query # Set query here
        df_daily = db_manager.fetch_data(db_manager.engine, query_daily)
        if df_daily is not None:
            db_manager.write_to_csv(df_daily, 'daily_data.csv')
            # print(f"{len(df_daily)} rows have been saved to the csv file")
            
        with open('log.txt', 'a') as f:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # f.write(f"{current_time}: {len(df)} rows have been saved to initial_data.csv\n")
            f.write(f"{current_time}: {len(df_daily)} rows have been saved to daily_data.csv\n")
            
        sleep(5)
        
        df_daily = pd.read_csv('daily_data.csv')
        response = db_manager.upload_to_api('daily_data.csv')
        print(f'Data upload response: {response.status_code} - {response.text}')
        
        with open('log.txt', 'a') as f:
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"{current_time}: {len(df_daily)} rows daily data have been pushed to the API\n")
                
    else:
        print("Failed to create database engine.")