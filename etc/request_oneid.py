import requests
import json
import pandas as pd
from dateutil import parser
from dotenv import load_dotenv
import os

load_dotenv('.env')

url = "https://advert-api.wildberries.ru/adv/v2/fullstats"

payload = json.dumps([
  {
    "id": 21500307,
    "interval": {
      "begin": "2025-03-24",
      "end": "2025-03-30"
    }
  }
])
headers = {
  'Content-Type': 'application/json',
  'Authorization': os.getenv('wb_api_token')
}

response = requests.request("POST", url, headers=headers, data=payload)
data = response.json() 

flattened_data = []
for entry in data:
    advertId = entry.get("advertId")
    booster_mapping = {}
    for booster in entry.get("boosterStats", []):
        nm_id = booster.get("nm")
        booster_date_str = booster.get("date")
        if booster_date_str:
            try:
                booster_date_obj = parser.parse(booster_date_str).date()
            except Exception as e:
                print(f"Error parsing booster date '{booster_date_str}': {e}")
                continue
            pos = booster.get("avg_position")
            booster_mapping[(nm_id, booster_date_obj)] = pos
                
    for day in entry.get("days", []):
        day_date_str = day.get("date")
        try:
            day_date_obj = parser.parse(day_date_str).date()
        except Exception as e:
            print(f"Error parsing day date '{day_date_str}': {e}")
            continue
        for app in day.get("apps", []):
            appType = app.get("appType")
            app_views = app.get("views")
            app_clicks = app.get("clicks")
            app_ctr = app.get("ctr")
            app_cpc = app.get("cpc")
            app_sum = app.get("sum")
            app_atbs = app.get("atbs")
            app_orders = app.get("orders")
            app_cr = app.get("cr")
            app_shks = app.get("shks")
            app_sum_price = app.get("sum_price")
                
            for nm in app.get("nm", []):
                nmID = nm.get("nmId")
                avg_position = booster_mapping.get((nmID, day_date_obj), None)
                record = {
                        "advertId": advertId,
                        "date": day_date_str,
                        "views": app_views,
                        "clicks": app_clicks,
                        "ctr": app_ctr,
                        "cpc": app_cpc,
                        "sum": app_sum,
                        "atbs": app_atbs,
                        "orders": app_orders,
                        "cr": app_cr,
                        "shks": app_shks,
                        "sum_price": app_sum_price,
                        "nmID": nmID,
                        "appType": appType,
                        "avg_position": avg_position
                    }
                flattened_data.append(record)
  
df = pd.DataFrame(flattened_data)
df.to_excel('try1.xlsx', index=False)