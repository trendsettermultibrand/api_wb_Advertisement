import pandas as pd
from sqlalchemy import create_engine,MetaData, Table
import psycopg2
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv
import os
from pathlib import Path

#load_dotenv('.env')
env_path = Path("/root/api_wb_Advertisement/.env")
load_dotenv(dotenv_path=env_path)


def test_db_connection(user, password, host, port, database, sslmode):
    """
    Function to test direct connection to PostgreSQL.
    """
    try:
        connection = psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user,
            password=password,
            sslmode=sslmode
            
        )
        print("Connection successful!")
        connection.close()
 
    except Exception as e:
        print(f"Error testing connection: {e}")
        

def load_dataframe_to_postgresql(df):
    """
    Function to load DataFrame into a PostgreSQL table.
    """
    connection_string = (
        f"postgresql://{os.getenv("user")}:{os.getenv("password")}@{os.getenv("host")}:{os.getenv("port")}/{os.getenv("database")}?sslmode={os.getenv("sslmode")}"
    )

    try:
        engine = create_engine(connection_string)
        print("SQLAlchemy engine created successfully.")

        with engine.connect() as conn:
            
            df.to_sql(name="api_wb_Advertisement", con=engine, if_exists='append', index=False)
            print(f"DataFrame successfully loaded into api_wb_Advertisement table.")
            
    except Exception as e:
        print(f"Error loading DataFrame: {e}")
        raise
        
 
def get_engine():
    user = os.getenv('user')
    password = os.getenv('password')
    host = os.getenv('host')
    port = os.getenv('port')
    database = os.getenv('database')
    sslmode = os.getenv('sslmode')
    
    connection_string = (
        f"postgresql://{user}:{password}@{host}:{port}/{database}?sslmode={sslmode}"
    )
    
    return create_engine(connection_string)
