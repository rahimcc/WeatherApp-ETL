from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime,timedelta
from sqlalchemy import create_engine,text


import requests
import psycopg2
import pandas as pd
import os
import sqlalchemy



CITIES = ['London','Istanbul','Baku','Tokyo','New York']
#CITIES = ['Baku']

load_dotenv(Path(__file__).parent.parent / ".env")
API_KEY = os.environ["OPENWEATHER_API_KEY"]

DB_CONN = "postgresql+psycopg2://rahimsharifov:root@host.docker.internal:5432/weather"



default_args = { 
    "owner": "rahimsharifov",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False
}


def extract(**context):
    records = []

    for city in CITIES:

        #Get lat and lon by city names 
        geo_url = 'https://api.openweathermap.org/geo/1.0/direct'
        params = {'q':f'{city}', 'appid': f'{API_KEY}'}
        geo_resp = requests.get(geo_url, params=params)
        geo_data = geo_resp.json()[0]
        
        lon = geo_data['lon']
        lat = geo_data['lat']

        print("Location Data successfully retrieved")


        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {'lon': lon,'lat': lat , 'appid': API_KEY , 'units':'metric'}
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        print("Weather Data Successfully")

        records.append( {
            "city": data['name'],
            'country': data['sys']['country'],
            'temperature_c': data['main']['temp'],
            'feels_like_c': data['main']['feels_like'],
            'humidity_pct': data['main']['humidity'],
            'wind_speed_ms': data['wind']['speed'],
            'weather_desc': data['weather'][0]['description'],
            'recorded_at': data['dt'],
            'ingested_at': pd.Timestamp.now(tz='UTC')
        })

         #  context['ti'].xcom_push(key="raw_records", value=records)

        ## Converting 
    df = pd.DataFrame(records)
    print(df.head())
    print(pd.__version__)
    print(sqlalchemy.__version__)

        
    engine = create_engine(DB_CONN)

    with engine.begin() as conn:

        df.to_sql("weather_raw", conn, if_exists="replace", index=False)
        print(f'Extracted: {len(records[0])} records')
        #return records


def transform(**context):
    """Clean and Validate the extracted records"""

    # records = context["ti"].xcom_pull(key="raw_records", task_ids="extract")
    engine = create_engine(DB_CONN)

    records = pd.read_sql("SELECT * FROM weather_raw",engine)

    df = pd.DataFrame(records)

    # Drop duplicates on city + recorded_at
    df = df.drop_duplicates(subset=["city","recorded_at"])

    # Validate temperature range (-90 to 60 is psycally possible)
    df = df[df["temperature_c"].between(-90,60)]
    # Validate humidity range (0 , 100 )
    df = df[df['humidity_pct'].between(0,100)]
    # Epoch to Human Readable format 
    df['recorded_at'] = pd.to_datetime(df['recorded_at'],unit='s')
    

    df.to_sql('weather_clean',engine, if_exists="append", index=False)

    print("Successfully transformed. ")

def load(**context):
    engine = create_engine(DB_CONN)
    
    upsert_query = """
                    INSERT INTO weather_daily_agg ( city, date , avg_temp_c, max_temp_c, avg_humidity)
                        SELECT
                                city,
                                DATE(recorded_at) as date,
                                ROUND(AVG(temperature_c)::NUMERIC,2) as avg_temperature,
                                MAX(temperature_c) as max_temperature,
                                ROUND(AVG(humidity_pct)::NUMERIC,2) as avg_humidity
                        FROM weather_clean
                        WHERE DATE(recorded_at) = CURRENT_DATE
                        GROUP BY city, DATE(recorded_at)
                        ON CONFLICT (city, date)
                            DO UPDATE SET
                                avg_temp_c  = EXCLUDED.avg_temp_c,
                                max_temp_c  = EXCLUDED.max_temp_c,
                                min_temp_c  = EXCLUDED.min_temp_c,
                                avg_humidity = EXCLUDED.avg_humidity
                   """
    with engine.begin() as conn:
        conn.execute(text(upsert_query))

with  DAG ( 
    dag_id = "weather_etl",
    default_args = default_args,
    description= " Hourly weather ETL: OpenWeatherMap -> PostgreSQL",
    start_date= datetime(2026,3,29),
    tags = ['portfolio','etl','weather']
) as dag:
    
    t_extract = PythonOperator(task_id="extract", python_callable = extract )

    t_transform = PythonOperator(task_id="transform", python_callable = transform)

    t_aggr = PythonOperator(task_id="load", python_callable = load)

if __name__ == "__main__":
    dag.test()