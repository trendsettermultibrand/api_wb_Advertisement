import requests
import pandas as pd
from dotenv import load_dotenv
import os
from pathlib import Path

env_path = Path("/root/api_wb_Advertisement/.env")
load_dotenv(dotenv_path=env_path)
#load_dotenv('.env')
api_token = os.getenv('wb_api_token')

url = "https://advert-api.wildberries.ru/adv/v1/promotion/count"

payload = {}
headers = {
    'Authorization': api_token
}

def get_advertids():
	r = requests.get(url, headers=headers, data=payload)
	data = r.json()

	df = pd.json_normalize(
	    data,
	    record_path=['adverts', 'advert_list'],
	    meta=[['adverts', 'type'], ['adverts', 'status'], ['adverts', 'count']], 
	    meta_prefix='adverts_'
	)

	# df.to_excel('advertIDs.xlsx', index=False)
	return df

