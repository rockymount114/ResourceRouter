import os,time, datetime
from os.path import join, dirname
from dotenv import load_dotenv
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import requests
import pandas as pd
from query import query_txt


def save_df():
    dotenv_path = join(dirname(__file__), '.env')
    load_dotenv(dotenv_path)

    SERVERNAME = os.environ.get("SERVER_ADDRESS")
    DATABASENAME = os.environ.get("DATABASE")
    USERNAME = os.environ.get("DB_USERNAME")
    PASSWORD = os.environ.get("DB_PASSWORD")
    PORT = os.environ.get("DB_PORT")

    try:
        conn = URL.create(
            "mssql+pyodbc",
            username=USERNAME,
            password=PASSWORD,
            host=SERVERNAME,
            database=DATABASENAME,
            query={"driver": "ODBC Driver 17 for SQL Server"}
        )
        engine = create_engine(conn)
        df = pd.read_sql_query(query_txt, engine)
        df.to_csv('data/df_lwmain.csv', index=False)
        print(f"{len(df)} rows has been saved to csv files")

    except sqlalchemy.exc.OperationalError as e:
        print(f"Operational Error: {e}")
    except Exception as e:
        print(f"Other Error: {e}")
        
def get_df():
    cols = ['id','address','pointx','pointy','class','datetimefrom','datetimeto','report_time','last_updated','datasource']
    df = pd.read_csv('data/df_lwmain.csv', usecols = cols, low_memory = True)
    return df

def send_df_to_db(df):
    '''send csv data into our ts db, this db is the data we sent to the vendor'''
    dotenv_path = join(dirname(__file__), '.env')
    load_dotenv(dotenv_path)

    SERVERNAME = os.environ.get("SERVER_ADDRESS")
    DATABASENAME = os.environ.get("DATABASE")
    USERNAME = os.environ.get("DB_USERNAME")
    PASSWORD = os.environ.get("DB_PASSWORD")
    PORT = os.environ.get("DB_PORT")

    try:
        conn = URL.create(
            "mssql+pyodbc",
            username=USERNAME,
            password=PASSWORD,
            host=SERVERNAME,
            database=DATABASENAME,
            query={"driver": "ODBC Driver 17 for SQL Server"}
        )
        engine = create_engine(conn)
        df.to_sql('df_sent', engine, if_exists='replace', index=False)
        print(f"{len(df)} rows has been sent to SQL Server")

    except sqlalchemy.exc.OperationalError as e:
        print(f"Operational Error: {e}")
    except Exception as e:
        print(f"Other Error: {e}")

def send_df_to_api():    
    url = "https://hooks.prismatic.io/trigger/SW5zdGFuY2U6Mzc3MjBiNjctOGI5OC00NWI2LWExODktZTNjOGJjOWQzZjFh"
    api_key = os.environ.get("API")
    file_path = "data/df_lwmain.csv"
    headers = {
        "Content-Type": "text/csv",
        "Api-Key": api_key
    }
    with open(file_path, "rb") as file:
        response = requests.post(url, headers=headers, data=file)
    print(response.status_code)       
 
def get_five_years_df():
    '''Get initial 5 years data save as csv or db'''
    pass               
def get_daily_df():
    '''get daily df compaire with init data, if new save as dailydata.csv'''
    pass 

if __name__ == "__main__":
    start_time = time.time()
    get_five_years_df()
    
    get_daily_df()
    
    # save_df()
    # df = get_df()
    # send_df_to_db(df)
    
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {datetime.timedelta(seconds=execution_time)}")