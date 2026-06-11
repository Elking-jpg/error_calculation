import requests 
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import os  
import re
from datetime import datetime, timezone, timedelta

import os

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "neondb"),
    "user": os.getenv("DB_USER", "neondb_owner"),
    "password": os.getenv("DB_PASSWORD", "npg_T8ZGQlMIa0dm"),
    "host": os.getenv("DB_HOST", "ep-bold-feather-at2890k4.c-9.us-east-1.aws.neon.tech"),
    "port": os.getenv("DB_PORT", "5432"),
    "sslmode": "require"
}
headers_config = {
    "User-Agent": "DataPipeline/1.0",
    "Accept": "application/json"
}

def GMT_cleaning(string):
    if not string or string in ["GMT", "UTC", "Z"]:
        return ""
    match = re.search(r'([+-])(\d{1,2}):?(\d{2})?', string)
    if not match:
        return ""
    signo = match.group(1)       
    horas = match.group(2)       
    minutos = match.group(3)     
    if len(horas) == 1:
        horas = f"0{horas}"
    if not minutos:
        minutos = "00"
    return f"{signo}{horas}:{minutos}"

def data_extractor(city):
    try:
        geocoding_url = f"https://geocoding-api.open-meteo.com/v1/search?name={city}&count=1&language=en&format=json"
        geo_response = requests.get(geocoding_url, headers=headers_config)
        if geo_response.status_code != 200:
            return None
        geo_data = geo_response.json()
        if 'results' not in geo_data or len(geo_data['results']) == 0:
            return None
        latitude = geo_data['results'][0]['latitude']
        longitude = geo_data['results'][0]['longitude']
    except Exception as e:
        print(f"Geocoding error: {e}")
        return None

    URL_API = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&hourly=temperature_2m&timezone=auto&past_days=1"
    try:
        raw_data = requests.get(URL_API, headers=headers_config)
        if raw_data.status_code == 200:
            data_json = raw_data.json()
            code_GMT = GMT_cleaning(data_json['timezone_abbreviation'])
            return (data_json, code_GMT)
        return None
    except Exception as e:
        print(f"API error: {e}")
        return None

def to_the_database(df_real, df_forecast):
    if df_real.empty and df_forecast.empty:
        print("No new data to process.")
        return
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        if not df_real.empty:
            tuple_real = list(df_real[['date_time', 'city', 'temperature']].itertuples(index=False))
            query_real = """
                INSERT INTO historical_hourly_weather (weather_timestamp, city, actual_temperature)
                VALUES (%s, %s, %s)
                ON CONFLICT (weather_timestamp, city) DO NOTHING;
            """
            execute_batch(cursor, query_real, tuple_real)
            print(f"{len(tuple_real)} rows inserted in historical_hourly_weather")

        if not df_forecast.empty:
            tuple_forecast = list(df_forecast[['date_time', 'city', 'temperature', 'lead_time_hours']].itertuples(index=False))
            query_forecast = """
                INSERT INTO hourly_forecasts (forecast_timestamp, city, predicted_temperature, lead_time_hours, execution_timestamp)
                VALUES (%s, %s, %s, %s, DATE_TRUNC('hour', NOW()))
                ON CONFLICT (execution_timestamp, forecast_timestamp, city) DO NOTHING;
            """
            execute_batch(cursor, query_forecast, tuple_forecast)
            print(f"{len(tuple_forecast)} rows inserted in hourly_forecasts")

        conn.commit()
        cursor.close()
    except Exception as e:
        print(f"Database error: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

def main():
    city = "Buenos Aires"
    extractor_result = data_extractor(city)
    if not extractor_result:
        print("Extraction failed.")
        return

    data_json, code_GMT = extractor_result

    now = datetime.now(timezone.utc)
    limite_historial = now - timedelta(hours=24)  

    times = data_json["hourly"]["time"]              
    temperatures = data_json["hourly"]["temperature_2m"]  
    processed_rows = []

    for t, temp in zip(times, temperatures):
        iso_string = f"{t}:00Z" if code_GMT == "" else f"{t}:00{code_GMT}"
        obj_date = datetime.fromisoformat(iso_string)
        lead_time_hours = int((obj_date - now).total_seconds() / 3600)
        processed_rows.append((obj_date, city, temp, lead_time_hours))
    
    df_raw = pd.DataFrame(processed_rows, columns=["date_time", "city", "temperature", "lead_time_hours"])
    df_real = df_raw[df_raw["date_time"] <= limite_historial]
    df_forecast = df_raw[df_raw["date_time"] > now]

    to_the_database(df_real, df_forecast)

if __name__ == "__main__":
    main()