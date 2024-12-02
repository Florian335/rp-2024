import functions
import pandas as pd
import datetime
import os
import requests
import json
import urllib.request
from dateutil.relativedelta import relativedelta
import calendar
import time
import psutil
import logging

functions.configure()
today = datetime.datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(days=31)
today = today.replace(day=1)
month_today = today.strftime("%Y-%m-%d")
future_date = today + relativedelta(months=2)
month_2m = future_date.strftime("%Y-%m-%d")

log_storage = []

LOG_FILE_PATH = "pandas-queries-performance.json"

def write_logs_to_file():
    if os.path.exists(LOG_FILE_PATH):
        with open(LOG_FILE_PATH, "r") as log_file:
            try:
                existing_logs = json.load(log_file)
                if not isinstance(existing_logs, list):
                    existing_logs = [existing_logs]
            except json.JSONDecodeError:
                existing_logs = []
    else:
        existing_logs = []

    all_logs = {json.dumps(log, sort_keys=True): log for log in (existing_logs + log_storage)}.values()

    with open(LOG_FILE_PATH, "w") as log_file:
        json.dump(list(all_logs), log_file, indent=4)

    log_storage.clear()

def log_to_json(step_name, **kwargs):
    """
    Log a record ensuring all keys are present, with missing ones set to None (null in JSON).
    """
    all_fields = [
        "timestamp", "step", "cpu_usage", "memory_usage", 
        "latency_seconds", "execution_time_seconds"
    ]
    
    log_record = {field: None for field in all_fields}
    
    log_record["timestamp"] = datetime.datetime.now().isoformat()
    log_record["step"] = step_name
    
    log_record.update(kwargs)
    
    log_storage.append(log_record)
    write_logs_to_file()

def log_resource_usage(step_name):
    cpu_usage = psutil.cpu_percent(interval=1)
    memory_usage = psutil.virtual_memory().percent
    log_to_json(step_name, cpu_usage=cpu_usage, memory_usage=memory_usage)

def log_api_latency(start_time, end_time, step_name):
    latency_seconds = round(end_time - start_time, 2)
    log_to_json(step_name, latency_seconds=latency_seconds)

def log_query_time(start_time, end_time, step_name):
    execution_time_seconds = round(end_time - start_time, 2)
    log_to_json(step_name, execution_time_seconds=execution_time_seconds)


def forecast_pandas():
    headers = {
        "User-Agent": os.getenv('Forecast_user_agent'),
        "Authorization": "Bearer "+os.getenv('Forecast'),
        "Forecast-Account-ID": os.getenv('Forecast_user_agent')
    }

    #### API monitoring
    start_time = time.time()
    log_resource_usage("Before Forecast Query")

    api_start_time = time.time()
    url_forecast = f"https://api.forecastapp.com/aggregate/project_export?timeframe_type=monthly&timeframe=custom&starting={month_today}&ending={month_2m}"
    forecast = pd.concat([pd.Series(x) for x in functions.forecast_api(url_forecast)], axis=1)
    api_end_time = time.time()
    log_api_latency(api_start_time, api_end_time, "Forecast API")

    forecast = forecast.T
    forecast.columns = forecast.loc[0]
    forecast = forecast.tail(-1)
    forecast = forecast.dropna(thresh = 2)
    try:
        forecast.drop(columns = [np.nan],inplace = True)
    except:
        pass

    #### Query below
    forecast_df = forecast[(forecast['Roles'].isin(['DK','US inc.']))]
    capacity = len(set(forecast_df['Person']))
    fte_values = sum(forecast_df['Jan 2025'].astype(float)/172.5)

    #### Query monitoring
    end_time = time.time()
    log_query_time(start_time, end_time, "Forecast Query")
    log_resource_usage("After Forecast Query")

    #### Return processed values
    return fte_values, capacity

def hubspot_pandas():
    headers = {
    'accept': "application/json",
    'content-type': "application/json",
    'authorization': "Bearer " + os.getenv('Hubspot')
    }

    #### API monitoring
    start_time = time.time()
    log_resource_usage("Before HubSpot Query")

    api_start_time = time.time()
    deal_test = requests.get("https://api.hubapi.com/crm/v3/objects/deals?limit=100&properties=start_date,end_date,hs_deal_stage_probability,fte_s_",
    headers = headers)

    deals_list = []
    deals_list.append(json.loads(deal_test.text)['results'])
    while True:
        if 'paging' in json.loads(deal_test.text):
            deal_test = requests.get(json.loads(deal_test.text)['paging']['next']['link'],headers = headers)
            deals_list.append(json.loads(deal_test.text)['results'])
        else:
            break
    api_end_time = time.time()
    log_api_latency(api_start_time, api_end_time, "HubSpot API")


    deal_properties = []
    for i in range(len(functions.flatten(deals_list))):
        deal_properties.append(functions.flatten(deals_list)[i]['properties'])

    #### Query below
    deals_df = pd.DataFrame(deal_properties)
    deals_df['fte_s_'] = deals_df['fte_s_'].astype(float)
    deals_df['start_date'] = pd.to_datetime(deals_df['start_date'],format = '%Y-%m-%d')
    deals_df['end_date'] = pd.to_datetime(deals_df['end_date'],format = '%Y-%m-%d')
    deals_df["months_between"] = (
        ((deals_df["end_date"].dt.year - deals_df["start_date"].dt.year) * 12 + 
        (deals_df["end_date"].dt.month - deals_df["start_date"].dt.month))
        .where((deals_df["start_date"].dt.year == today.year) & (deals_df["start_date"].dt.month == today.month), None)
    )
    deals_df["months_between"] = deals_df["months_between"].clip(lower=1)
    deals_jan_df = deals_df[(deals_df['start_date'].dt.year == today.year) & (deals_df['start_date'].dt.month == today.month)].copy()
    deals_jan_df.loc[:, 'ftes_norm'] = deals_jan_df['fte_s_'] / deals_jan_df['months_between']
    total_ftes = deals_jan_df['ftes_norm'].sum()

    #### Query monitoring
    end_time = time.time()
    log_query_time(start_time, end_time, "HubSpot Query")
    log_resource_usage("After HubSpot Query")


    #### Return processed values
    return total_ftes

overall_start_time = time.time()
forecast_ftes, forecast_capacity = forecast_pandas()
hubspot_ftes = hubspot_pandas()
overall_end_time = time.time()
log_query_time(overall_start_time, overall_end_time, "Entire Query Process")

logging.info(f'Pipeline FTEs: {hubspot_ftes}')
logging.info(f'Capacity: {forecast_capacity}')
logging.info(f'Committed FTEs: {forecast_ftes}')
remaining_capacity = forecast_capacity - (hubspot_ftes + forecast_ftes)
if remaining_capacity < 0:
    logging.warning(f"Over capacity! Remaining capacity: {remaining_capacity}")
else:
    logging.info(f"Enough capacity. Remaining capacity: {remaining_capacity}")
