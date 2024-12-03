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

def to_epoch_milliseconds(date_string):
    date = datetime.datetime.strptime(date_string, "%Y-%m-%d")
    epoch_milliseconds = int(date.replace(tzinfo=datetime.timezone.utc).timestamp() * 1000)
    return epoch_milliseconds

log_storage = []

LOG_FILE_PATH = "post-queries-performance.json"

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
    all_fields = [
        "timestamp", "step", 
        "latency_seconds", "execution_time_seconds"
    ]
    
    log_record = {field: None for field in all_fields}
    
    log_record["timestamp"] = datetime.datetime.now().isoformat()
    log_record["step"] = step_name
    
    log_record.update(kwargs)
    
    log_storage.append(log_record)
    write_logs_to_file()

def log_api_latency(start_time, end_time, step_name):
    latency_seconds = round(end_time - start_time, 4)
    log_to_json(step_name, latency_seconds=latency_seconds)

def log_query_time(start_time, end_time, step_name):
    execution_time_seconds = round(end_time - start_time, 4)
    log_to_json(step_name, execution_time_seconds=execution_time_seconds)

def forecast_json():
    headers = {
        "User-Agent": os.getenv('Forecast_user_agent'),
        "Authorization": "Bearer "+os.getenv('Forecast'),
        "Forecast-Account-ID": os.getenv('Forecast_user_agent')
    }

    #### API monitoring
    api_start_time = time.time() #### API START TIME
    url_forecast = f"https://api.forecastapp.com/aggregate/project_export?timeframe_type=monthly&timeframe=custom&starting={month_today}&ending={month_2m}"
    
    request = urllib.request.Request(url=url_forecast, headers=headers)
    response = urllib.request.urlopen(request, timeout=10)
    responseBody = response.read().decode("utf-8")
    
    api_end_time = time.time() #### API END TIME
    log_api_latency(api_start_time, api_end_time, "Forecast API")

    start_time = time.time() #### QUERY START TIME

    #### Query below
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
    end_time = time.time() #### QUERY END TIME
    log_query_time(start_time, end_time, "Forecast Query")

    #### Return processed values
    return fte_values, capacity

def hubspot_post():
    headers = {
    'accept': "application/json",
    'content-type': "application/json",
    'authorization': "Bearer " + os.getenv('Hubspot')
    }

    #### API monitoring
    api_start_time = time.time() #### API START TIME

    end_of_month = ((today.replace(day=28) + datetime.timedelta(days=4)).replace(day=1) - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    after_value = ""
    payload = f'''{{
    "filterGroups": [
        {{
        "filters": [
            {{
            "propertyName": "start_date",
            "operator": "BETWEEN",
            "value": "{to_epoch_milliseconds(month_today)}",
            "highValue": "{to_epoch_milliseconds(end_of_month)}"
            }}
        ]
        }}
    ],
    "properties": ["start_date", "end_date", "fte_s_"],
    "after": "{after_value}"
    }}'''

    deal_test = requests.post("https://api.hubapi.com/crm/v3/objects/deals/search",
    headers = headers,data=payload)
    
    deals_list = []
    deals_list.append(json.loads(deal_test.text)['results'])
    while True:
        if 'paging' in json.loads(deal_test.text):
            after_value = json.loads(deal_test.text)['paging']['next']['after']
            payload = json.dumps({
                "filterGroups": [
                    {
                        "filters": [
                            {
                                "propertyName": "start_date",
                                "operator": "BETWEEN",
                                "value": str(to_epoch_milliseconds(month_today)),
                                "highValue": str(to_epoch_milliseconds(end_of_month))
                            }
                        ]
                    }
                ],
                "properties": ["start_date", "end_date", "fte_s_"],
                "after": after_value
            })
            
            deal_test = requests.post("https://api.hubapi.com/crm/v3/objects/deals/search", headers=headers, data=payload)

            if deal_test.status_code == 200:
                deals_list.append(json.loads(deal_test.text)['results'])
            else:
                print(f"Request failed with status code: {deal_test.status_code}")
                break
        else:
            break

    api_end_time = time.time() #### API END TIME
    log_api_latency(api_start_time, api_end_time, "HubSpot POST API")

    start_time = time.time() #### QUERY START TIME
    
    #### Query below
    deal_properties = []
    for i in range(len(functions.flatten(deals_list))):
        deal_properties.append(functions.flatten(deals_list)[i]['properties'])
    
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
    end_time = time.time() #### QUERY END TIME
    log_query_time(start_time, end_time, "HubSpot Query")

    #### Return processed values
    return total_ftes

def main():
    forecast_overall_start_time = time.time()
    forecast_ftes, forecast_capacity = forecast_json()
    forecast_overall_end_time = time.time()
    log_query_time(forecast_overall_start_time, forecast_overall_end_time, "Forecast Entire Query Process")

    hubspot_overall_start_time = time.time()
    hubspot_ftes = hubspot_post()
    hubspot_overall_end_time = time.time()
    log_query_time(hubspot_overall_start_time, hubspot_overall_end_time, "HubSpot Entire Query Process")

    remaining_capacity = forecast_capacity - (hubspot_ftes + forecast_ftes)
    if remaining_capacity < 0:
        logging.warning(f"Over capacity! Remaining capacity: {remaining_capacity}")
    else:
        logging.warning(f"Enough capacity. Remaining capacity: {remaining_capacity}")

if __name__ == "__main__":
    main()


