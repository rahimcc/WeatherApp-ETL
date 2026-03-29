from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


from datetime import datetime,timedelta
import requests
import psycopg2
import pandas as pd
import os



CITIES = ['London','Istanbul','Baku','Tokyo','New York']
CITIES = ['Baku']

#API_KEY = os.environ["OPENWEATHER_API_KEY"]

API_KEY = '82a1ddbac7c82bbf9db7c4afa01c2ab6'

DB_CONN = "postgrsql://rahimsharifov:root@host.docker.internal:5432/airflow"



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
        params = {'q':'Baku', 'appid': '82a1ddbac7c82bbf9db7c4afa01c2ab6'}
        geo_resp = requests.get(geo_url, params=params)
        geo_data = geo_resp.json()[0]
        
        lon = geo_data['lon']
        lat = geo_data['lat']

        # Get Weather data using lat and lon 

        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {'lon': lon,'lat': lat , 'appid': API_KEY , 'units':'metric'}
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        records.append( {
            "city": data['name'],
            'country': data['sys']['country'],
            'temperature_c': data['main']['temp'],
            'feels_like_c': data['main']['feels_like'],
            'humidity_pct': data['main']['humidity'],
            'wind_speed_ms': data['wind']['speed'],
            'weather_desc': data['weather'][0]['description']
        })

      #  context['ti'].xcom_push(key="raw_records", value=records)
        print(f'Extracted: {len(records[0])} records')

        return records




with  DAG ( 
    dag_id = "weather_etl",
    default_args = default_args,
    description= " Hourly weather ETL: OpenWeatherMap -> PostgreSQL",
    start_date= datetime(2026,3,29),
    tags = ['portfolio','etl','weather']
) as dag:
    
    t_extract = PythonOperator(task_id="extract", python_callable = extract )


if __name__ == "__main__":
    dag.test()