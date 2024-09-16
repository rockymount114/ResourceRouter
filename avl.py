import os
import pandas as pd
import requests
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv
from DB import DatabaseManager, EmailManager
from datetime import datetime
from time import sleep
from query import avl_query

load_dotenv()

SERVER_ADDRESS = os.getenv('CAD_ADDRESS')
DATABASENAME = os.getenv('CAD_DATABASE')
DB_USERNAME = os.getenv('CAD_USERNAME')
DB_PASSWORD = os.getenv('CAD_PASSWORD')

AVL_URL = os.getenv('AVL_URL')
AVL_KEY = os.getenv('AVL_KEY')

db_manager = DatabaseManager(SERVER_ADDRESS, DATABASENAME, DB_USERNAME, DB_PASSWORD)
    
if __name__ == "__main__":    
    if db_manager.engine:
        
        query = avl_query # Set query here
        
        df = db_manager.fetch_data(db_manager.engine, query)
        
        if df is not None:
            db_manager.write_to_csv(df, 'avl_5minutes.csv')
            
            with open('log.txt', 'a') as f:
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"{current_time}: {len(df)} rows avl data have been saved to the API\n")
                
            sleep(1)    
            
            # put data to AVL api
            response = db_manager.upload_avl_to_api('avl_5minutes.csv')  # Ensure the correct file name is used
            
            if response.status_code == 200:           
                with open('log.txt', 'a') as f:
                    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    f.write(f"{current_time}: {len(df)} rows avl data have been pushed to the API, {response.status_code} - {response.text}\n")
                
            else:
                
                with open('log.txt', 'a') as f:
                        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        f.write(f"{current_time}: error as {response.status_code}, {response.text}\n")
                        
                EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
                EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")

                if EMAIL_ADDRESS is None or EMAIL_PASSWORD is None:
                    raise ValueError("EMAIL_ADDRESS and EMAIL_PASSWORD must be set in the environment variables.")
                else:
                    email_manager = EmailManager(EMAIL_ADDRESS, EMAIL_PASSWORD)                    
                    email_manager.send_email(
                        f"AVL API error with status code: {response.status_code}", 
                        f"AVL Data upload failed, error code: {response.status_code} - {response.text}",  
                        "ip114@msn.com"
                    )
                    with open('log.txt', 'a') as f:
                        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        f.write(f"{current_time}: avl API error {response.status_code} - {response.text}\n")
                        
                
    else:
        print("Failed to create database engine.")