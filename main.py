import os,time, datetime
from os.path import join, dirname
from dotenv import load_dotenv
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import pyodbc
import pandas as pd
from query import query_txt

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