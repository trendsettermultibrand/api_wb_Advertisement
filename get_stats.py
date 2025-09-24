import pandas as pd
import connection
import requests
import json
import get_ids
import time
import random
from dateutil import parser
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta, date
from pathlib import Path

def get_last_date():
    engine = connection.get_engine()
    query = 'SELECT MAX("date") AS max_date FROM "api_wb_Advertisement"'
    result = pd.read_sql(query, engine)
    latest_change = result.iloc[0, 0]
    print("Latest max_date from DB:", latest_change)
    return latest_change

def get_date_only(full_date_str):
    return full_date_str.split("T")[0]

def fetch_data():
    #load_dotenv('.env')
    env_path = Path("/root/api_wb_Advertisement/.env")
    load_dotenv(dotenv_path=env_path)
    api_token = os.getenv('wb_api_token')
    
    ids = get_ids.get_advertids()
    ids = ids['advertId'].to_list()
    
    def chunked(iterable, chunk_size=50):
        for i in range(0, len(iterable), chunk_size):
            yield iterable[i:i + chunk_size]
    
    all_data = []
    url = "https://advert-api.wildberries.ru/adv/v2/fullstats"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': api_token
    }
    
    total_chunks = (len(ids) + 49) // 50
    print(f"Total chunks to process: {total_chunks}")
    
    #begin_date_str = get_lastChangedate()
    #begin_date = get_date_only(begin_date_str)  
    #begin_date_obj = datetime.strptime(begin_date, "%Y-%m-%d")
    #begin_date_obj = begin_date_obj + timedelta(days=1)   
    #end_date_obj = begin_date_obj + timedelta(days=7)    
    #begin_date_new = begin_date_obj.strftime("%Y-%m-%d")  
    #end_date = end_date_obj.strftime("%Y-%m-%d")
    # 2rd
    # today = date.today()
    # weekday = today.weekday()
    # monday_last_week = today - timedelta(days=weekday + 7)
    # sunday_last_week = monday_last_week + timedelta(days=6)
    # begin_date_new = monday_last_week.strftime("%Y-%m-%d")
    # end_date = sunday_last_week.strftime("%Y-%m-%d")
    # today = date.today()
    last_date = pd.to_datetime(get_last_date()).date()
    print(f"Last date {last_date}")
    previous_day = last_date + timedelta(days=1)
    # start = previous_day.strftime("%Y-%m-%dT00:00:00Z")
    # end = previous_day.strftime("%Y-%m-%dT23:59:59Z")
    previous_day = previous_day.strftime("%Y-%m-%d")
    print(previous_day)
    
    for idx, chunk in enumerate(chunked(ids, 50)):
        print(f"\nProcessing chunk {idx+1}/{total_chunks} with {len(chunk)} IDs")
        
        payload_data = [
            {
                "id": campaign_id,
                # "interval": {
                #     "begin": start,
                #     "end": end
                # }
                "dates": [previous_day]
            }
            for campaign_id in chunk
        ]
        payload = json.dumps(payload_data)
        
        max_retries = 5
        attempt = 0
        
        print(f"For date from {previous_day} to {previous_day}")
        
        while attempt < max_retries:
            response = requests.post(url, headers=headers, data=payload)
            
            if response.status_code in (429, 503):
                attempt += 1
                x_ratelimit_retry = response.headers.get('X-Ratelimit-Retry')
                x_ratelimit_limit = response.headers.get('X-Ratelimit-Limit')
                x_ratelimit_reset = response.headers.get('X-Ratelimit-Reset')
                
                if x_ratelimit_retry:
                    wait_time = int(x_ratelimit_retry)
                    print(
                        f"Chunk {idx+1}: Received {response.status_code} "
                        f"(Too Many Requests). Retry in {wait_time} seconds "
                        f"(Attempt {attempt}/{max_retries})."
                    )
                else:
                    wait_time = 90 * (2 ** (attempt - 1))
                    print(
                        f"Chunk {idx+1}: Received {response.status_code}, no 'X-Ratelimit-Retry' header. "
                        f"Retrying attempt {attempt}/{max_retries} after {wait_time} seconds..."
                    )
                
                if x_ratelimit_limit:
                    print(f"X-Ratelimit-Limit: {x_ratelimit_limit}")
                if x_ratelimit_reset:
                    print(f"X-Ratelimit-Reset: {x_ratelimit_reset}")

                time.sleep(wait_time)
            
            else:
                break
        
        if response.status_code != 200:
            print(f"Chunk {idx+1}: Failed with status code {response.status_code} after {attempt} attempts")
            continue

        try:
            data = response.json()
        except Exception as e:
            print(f"Chunk {idx+1}: Error parsing JSON: {e}")
            continue

        if not data:
            print(f"Chunk {idx+1}: No data returned, skipping this chunk.")
            continue

        print(f"Chunk {idx+1}: Response JSON type: {type(data)}")
        
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
                            "views": nm.get("views", app_views),
                            "clicks": nm.get("clicks", app_clicks),
                            "ctr": nm.get("ctr", app_ctr),
                            "cpc": nm.get("cpc", app_cpc),
                            "sum": nm.get("sum", app_sum),
                            "atbs": nm.get("atbs", app_atbs),
                            "orders": nm.get("orders", app_orders),
                            "cr": nm.get("cr", app_cr),
                            "shks": nm.get("shks", app_shks),
                            "sum_price": nm.get("sum_price", app_sum_price),
                            "nmID": nmID,
                            "appType": appType,
                            "avg_position": avg_position
                        }
                        flattened_data.append(record)
        
        try:
            df_chunk = pd.DataFrame(flattened_data)
        except Exception as ex:
            print(f"Chunk {idx+1}: An unexpected error occurred during DataFrame creation: {ex}")
            continue
    
        print(f"Chunk {idx+1}: Processed successfully with {df_chunk.shape[0]} records.")
        all_data.append(df_chunk)
        time.sleep(random.uniform(5, 15))
    
    final_df = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    #final_df.to_excel("try.xlsx")
    print("\nFinal cumulative DataFrame shape:", final_df.shape)
    
    return final_df

