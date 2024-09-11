import os
import pandas as pd
import requests
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv
from DB import DatabaseManager
from query import initial_query, daily_query,avl_query

load_dotenv()

SERVER_ADDRESS = os.getenv('TS_ADDRESS')
DATABASENAME = os.getenv('TS_DATABASE')
DB_USERNAME = os.getenv('TS_USERNAME')
DB_PASSWORD = os.getenv('TS_PASSWORD')

db_manager = DatabaseManager(SERVER_ADDRESS, DATABASENAME, DB_USERNAME, DB_PASSWORD)
    
if __name__ == "__main__":    
    if db_manager.engine:
        
        query = avl_query # Set query here
        
        df = db_manager.fetch_data(db_manager.engine, query)
        if df is not None:
            db_manager.write_to_csv(df, 'avl_data.csv')
            print(len(df))
    else:
        print("Failed to create database engine.")