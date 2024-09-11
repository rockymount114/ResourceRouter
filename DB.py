import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import pyodbc
from dotenv import load_dotenv
import pyproj



class DatabaseManager:
    def __init__(self, server_address, database, username, password, port=1433):
        self.server_address = server_address
        self.database = database
        self.username = username
        self.password = password
        self.port = port
        self.engine = self.create_db_engine()

    def create_db_engine(self):
        try:
            conn = URL.create(
                "mssql+pyodbc",
                username=self.username,
                password=self.password,
                host=self.server_address,
                database=self.database,
                query={"driver": "ODBC Driver 17 for SQL Server"}
            )
            engine = create_engine(conn)
            return engine
        
        except sqlalchemy.exc.OperationalError as e:
            print(f"Operational Error: {e}")
            return None
        except Exception as e:
            print(f"Other Error: {e}")
            return None
            
    def fetch_data(self, engine, query):
        if engine is None:
            print("Engine is not initialized.")
            return None
        try:
            query = text(query)
            return pd.read_sql(query, engine)
        except Exception as e:
            print(f"Error fetching data: {e}")
            return None
        
    def write_to_csv(self, df, filename):
        """Provides a simple way to write a dataframe to a CSV file"""
        df.to_csv(filename, index=False, sep=',', quoting=1)
     
    @staticmethod    
    def get_cords(x, y):        
        src_crs = pyproj.CRS.from_epsg(2264)  # NAD_1983_StatePlane_California_VI_FIPS_0406
        tgt_crs = pyproj.CRS.from_epsg(4326)  # WGS 84 (latitude and longitude)

        transformer = pyproj.Transformer.from_crs(src_crs, tgt_crs)
        lat, lon = transformer.transform(x, y)

        return lat, lon
