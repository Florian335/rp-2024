import functions
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

LOG_FILE_PATH = "json-queries-performance.json"

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

def forecast_json():
    headers = {
        "User-Agent": os.getenv('Forecast_user_agent'),
        "Authorization": "Bearer "+os.getenv('Forecast'),
        "Forecast-Account-ID": os.getenv('Forecast_user_agent')
    }

    #### API monitoring
    start_time = time.time()
    log_resource_usage("Before Forecast Query")
    url_forecast = f"https://api.forecastapp.com/aggregate/project_export?timeframe_type=monthly&timeframe=custom&starting={month_today}&ending={month_2m}"
    
    api_start_time = time.time()
    request = urllib.request.Request(url=url_forecast, headers=headers)
    response = urllib.request.urlopen(request, timeout=10)
    api_end_time = time.time()
    log_api_latency(api_start_time, api_end_time, "Forecast API")

    #### Query below
    responseBody = response.read().decode("utf-8")
    lines = responseBody.strip().split('\n')
    columns = lines[0].split(',')
    data = []
    for i in lines[1:]:
        values = i.split(",")
        record = dict(zip(columns,values))
        data.append(record)
    allowed_roles = ['DK','US inc.']
    filtered_data = []
    for item in data:
        if item.get("Roles", "") in allowed_roles:
            jan_2025_str = item.get("Jan 2025", "0")
            try:
                fte = float(jan_2025_str) / 172.5
            except ValueError as e:
                logger.error(f"Invalid number for January 2025: {jan_2025_str}", exc_info=e)
                fte = 0.0
            person = item.get("Person", "Unknown")
            filtered_data.append((fte, person))
    fte_values = sum([fte for fte, _ in filtered_data])
    capacity = len(set([cap for _, cap in filtered_data]))

    #### Query monitoring
    end_time = time.time()
    log_query_time(start_time, end_time, "Forecast Query")
    log_resource_usage("After Forecast Query")

    #### Return processed values
    return fte_values, capacity

def hubspot_json():
    headers = {
    'accept': "application/json",
    'content-type': "application/json",
    'authorization': "Bearer " + os.getenv('Hubspot')
        }

    #### API monitoring
    start_time = time.time()
    log_resource_usage("Before HubSpot Query")
    deals_list = []

    api_start_time = time.time()
    deal_test = requests.get("https://api.hubapi.com/crm/v3/objects/deals?limit=100&properties=start_date,end_date,hs_deal_stage_probability,fte_s_",
    headers = headers)

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
    fte_collection = []
    for i in deal_properties:
        try:
            if all(key in i and i[key] is not None for key in ["start_date", "end_date", "fte_s_"]):
                start_date = datetime.datetime.strptime(i["start_date"], "%Y-%m-%d")
                end_date = datetime.datetime.strptime(i["end_date"], "%Y-%m-%d")
                fte_value = float(i["fte_s_"])

                if start_date.year == today.year and start_date.month == today.month:
                    months_between = max(1, (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month))
                    fte_collection.append(fte_value / months_between)
        except Exception as e:
            pass

    total_ftes = sum(fte_collection) if fte_collection else 0.0

    #### Query monitoring
    end_time = time.time()
    log_query_time(start_time, end_time, "HubSpot Query")
    log_resource_usage("After HubSpot Query")

    #### Return processed values
    return total_ftes


overall_start_time = time.time()
forecast_ftes, forecast_capacity = forecast_json()
hubspot_ftes = hubspot_json()
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
