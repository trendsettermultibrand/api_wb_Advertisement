import pandas as pd
import connection
from dotenv import load_dotenv
import os

load_dotenv('.env')
user = os.getenv('user')
password = os.getenv('password')
host = os.getenv('host')
port = os.getenv('port')
database = os.getenv('database')
sslmode = os.getenv('sslmode')

file_path = pd.read_excel('reklama_1-31oct2023.xlsx')
print(f"lenght of dataframe is {len(file_path)}")
print(f"Processing file {file_path}!")

engine = connection.load_dataframe_to_postgresql(
    df=file_path,
    user=user,
    password=password,
    host=host,
    port=port,
    database=database,
    table_name="advertisement",
    sslmode=sslmode
    )
        
print(f"Finished processing file")