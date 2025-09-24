import pandas as pd
import connection 
import get_stats
from datetime import datetime,timedelta
import mail 
from dotenv import load_dotenv
import os 
from pathlib import Path

env_path = Path("/root/api_wb_Advertisement/.env")
load_dotenv(dotenv_path=env_path)
#load_dotenv('.env')

try:
	print('start processing')
	df = get_stats.fetch_data()
	connection.load_dataframe_to_postgresql(df)
	print('Ended processing')
	subject = f'Advertisement Successfully done'
	body = f'Advertisement script run without errors'
	mail.send_success_email(subject, body)

except Exception as e:
	print(f"Error processing file: {e}")
	subject1 = f"Advertisement Error accured"
	body1 = f"Advertisement script returns an error,please check me:)."
	mail.send_error_email(subject1, body1)
	raise
