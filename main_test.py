import os
import pandas as pd
import requests
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv
from datetime import datetime
from query import query_txt

# Load environment variables from .env file
load_dotenv()

# Get environment variables
SERVER_ADDRESS = os.getenv('SERVER_ADDRESS')
DATABASENAME = os.getenv('DATABASE')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_PORT = os.getenv('DB_PORT')
API_KEY = os.getenv('API_KEY')

# Create database engine using SQLAlchemy
def get_db_engine():    
    try:
        conn = URL.create(
            "mssql+pyodbc",
            username=DB_USERNAME,
            password=DB_PASSWORD,
            host=SERVER_ADDRESS,
            database=DATABASENAME,
            query={"driver": "ODBC Driver 17 for SQL Server"}
        )      

    except sqlalchemy.exc.OperationalError as e:
        print(f"Operational Error: {e}")
    except Exception as e:
        print(f"Other Error: {e}")        

    return create_engine(conn)

# connect to TS db
def get_tsdb_engine():    
    try:
        conn = URL.create(
            "mssql+pyodbc",
            username=DB_USERNAME,
            password=DB_PASSWORD,
            host=SERVER_ADDRESS,
            database=DATABASENAME,
            query={"driver": "ODBC Driver 17 for SQL Server"}
        )      

    except sqlalchemy.exc.OperationalError as e:
        print(f"Operational Error: {e}")
    except Exception as e:
        print(f"Other Error: {e}")        

    return create_engine(conn)

# create CAD db
def get_caddb_engine():    
    try:
        conn = URL.create(
            "mssql+pyodbc",
            username='cad_reports',
            password='reports',
            host='CRMPRODDB5',
            database='Cad',
            query={"driver": "ODBC Driver 17 for SQL Server"}
        )      

    except sqlalchemy.exc.OperationalError as e:
        print(f"Operational Error: {e}")
    except Exception as e:
        print(f"Other Error: {e}")        

    return create_engine(conn)

# Fetch initial data
def fetch_initial_data(engine):
    query = """
    SELECT TOP 100
        lwmain.inci_id AS id,
        CAST(REPLACE(lwmain.streetnbr,',','') AS CHAR(8)) + lwmain.street AS [address],
        CASE WHEN LEN(lwmain.geox) < 9 THEN CAST(((CAST(lwmain.geox AS DECIMAL(16,2)) * (POWER(10, (9 - LEN(lwmain.geox)))))/100) AS DECIMAL(16,2)) 
            ELSE CAST((CAST(lwmain.geox AS DECIMAL(16,2)))/100 AS DECIMAL(16,2)) END AS pointx,
        CASE WHEN LEN(lwmain.geoy) < 9 THEN CAST(((CAST(lwmain.geoy AS DECIMAL(16,2)) * (POWER(10, (9 - LEN(lwmain.geoy)))))/100) AS DECIMAL(16,2)) 
            ELSE CAST((CAST(lwmain.geoy AS DECIMAL(16,2)))/100 AS DECIMAL(16,2)) END AS pointy,
        lwchrg.ibr_code AS class,
        FORMAT(lwmain.date_occu, 'yyyy-MM-ddTHH:mm:ss+00:00') AS datetimefrom,
        FORMAT(lwmain.date_fnd, 'yyyy-MM-ddTHH:mm:ss+00:00') AS datetimeto,
        FORMAT(lwmain.date_rept, 'yyyy-MM-ddTHH:mm:ss+00:00') AS report_time,
        FORMAT(lwmain.date_rept, 'yyyy-MM-ddTHH:mm:ss+00:00') AS last_updated,
        'rms8' AS datasource
    FROM lwmain     
    JOIN lwchrg ON lwmain.lwmainid = lwchrg.lwmainid
    WHERE lwmain.inci_id IS NOT NULL
    AND lwmain.geox != 0
    AND lwmain.geoy != 0
    AND lwmain.date_rept >= '1999-01-01' 
    """
    return pd.read_sql(query, engine)

# Fetch new data since the last upload
def fetch_new_data(engine, last_update):
    query = text("""
    SELECT
        lwmain.inci_id AS id,
        CAST(REPLACE(lwmain.streetnbr,',','') AS CHAR(8)) + lwmain.street AS [address],
        CASE WHEN LEN(lwmain.geox) < 9 THEN CAST(((CAST(lwmain.geox AS DECIMAL(16,2)) * (POWER(10, (9 - LEN(lwmain.geox)))))/100) AS DECIMAL(16,2)) 
            ELSE CAST((CAST(lwmain.geox AS DECIMAL(16,2)))/100 AS DECIMAL(16,2)) END AS pointx,
        CASE WHEN LEN(lwmain.geoy) < 9 THEN CAST(((CAST(lwmain.geoy AS DECIMAL(16,2)) * (POWER(10, (9 - LEN(lwmain.geoy)))))/100) AS DECIMAL(16,2)) 
            ELSE CAST((CAST(lwmain.geoy AS DECIMAL(16,2)))/100 AS DECIMAL(16,2)) END AS pointy,
        lwchrg.ibr_code AS class,
        FORMAT(lwmain.date_occu, 'yyyy-MM-ddTHH:mm:ss+00:00') AS datetimefrom,
        FORMAT(lwmain.date_fnd, 'yyyy-MM-ddTHH:mm:ss+00:00') AS datetimeto,
        FORMAT(lwmain.date_rept, 'yyyy-MM-ddTHH:mm:ss+00:00') AS report_time,
        FORMAT(lwmain.date_rept, 'yyyy-MM-ddTHH:mm:ss+00:00') AS last_updated,
        'rms8' AS datasource
    FROM lwmain     
    JOIN lwchrg ON lwmain.lwmainid = lwchrg.lwmainid
    WHERE lwmain.inci_id IS NOT NULL
    AND lwmain.geox != 0
    AND lwmain.geoy != 0
    AND lwmain.date_rept >= '1999-01-01'
    """)
    return pd.read_sql(query, engine, params={'last_update': last_update})

from avl_query import query_txt
avl_query_txt = text('''select top 1000 * from v_avl''')

def fetch_test_data(engine):
    query = avl_query_txt
    return pd.read_sql(query, engine)

# Write data to CSV
def write_to_csv(df, filename):
    df.to_csv(filename, index=False, sep=',', quoting=1)

# Upload CSV to API
def upload_to_api(csv_file):
    url = "https://hooks.prismatic.io/trigger/SW5zdGFuY2VGbG93Q29uZmlnOmIyYzk4MWMyLTEyZDItNDExYS05ZTNiLTc1MGYzNzIzMGJmYg=="
    headers = {
        "Content-Type": "text/csv",
        "Api-Key": API_KEY
    }
    with open(csv_file, 'rb') as f:
        response = requests.post(url, headers=headers, data=f)
    return response

def main():
    engine = get_db_engine()
    
    # Fetch and upload initial data if needed
    initial_data_file = 'test.csv'
    initial_data = fetch_initial_data(engine)
    write_to_csv(initial_data, initial_data_file)
    # response = upload_to_api(initial_data_file)
    # print(f'Initial data upload response: {response.status_code} - {response.text}')
    
    # Fetch and upload new data daily
    last_update_file = 'last_update.txt'
    
    try:
        with open(last_update_file, 'r') as f:
            last_update = f.read().strip()
    except FileNotFoundError:
        last_update = '1999-01-01'
    
    new_data = fetch_new_data(engine, last_update)
    if not new_data.empty:
        new_data_file = 'DailyData_Uploads.csv'
        write_to_csv(new_data, new_data_file)
        with open(last_update_file, 'w') as f:
            f.write(new_data['report_time'].max())
        
        # response = upload_to_api(new_data_file)
        # print(f'Daily data upload response: {response.status_code} - {response.text}')
        # if response.status_code == 200:
        #     with open(last_update_file, 'w') as f:
        #         f.write(new_data['report_time'].max())
    
if __name__ == "__main__":
    # main()
    # df = fetch_test_data(get_caddb_engine())
    # write_to_csv(df, 'avl.csv')
    
    df = fetch_test_data(get_tsdb_engine())
    write_to_csv(df, 'avl.csv')