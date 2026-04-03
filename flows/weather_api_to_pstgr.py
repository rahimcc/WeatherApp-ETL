import requests
import psycopg2
import os
import sys 
import sqlalchemy

from dotenv import load_dotenv
from datetime import datetime,timedelta
from sqlalchemy import create_engine,text
from sqlalchemy.orm import Session

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.models import WeatherData,get_engine
from prefect import flow, task 


load_dotenv()

CITIES = ['Baku']     #  ['London','Istanbul','Baku','Tokyo','New York']
GEO_API_URL= 'https://api.openweathermap.org/geo/1.0/direct' # To Get Geolocation Data 
WEATHER_API_URL = "https://api.openweathermap.org/data/2.5/weather" # To get weather data 
API_KEY = os.getenv("OPENWEATHER_API_KEY") 
DB_CONN = os.getenv("DATABASE_URL")



@task
def fetch_and_save_raw():

    for city in CITIES:

        #Get location data
        params = {'q':f'{city}', 'appid': f'{API_KEY}'}
        geo_resp = requests.get(GEO_API_URL, params=params)
        geo_data = geo_resp.json()[0]
        
        lon = geo_data['lon']
        lat = geo_data['lat']

        print("Location Data successfully retrieved")

        # Get Weather Data 
        params = {'lon': lon,'lat': lat , 'appid': API_KEY , 'units':'metric'}
        resp = requests.get(WEATHER_API_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        print("Weather Data extracted successfully")

        print(data)
        
        engine = get_engine()
        """
        with Session() as session:
             
             records = [ 
                  WeatherRaw( 
                       city = 
                  )
             ]
        """
@task
def transform_to_clean():
    pass

@task
def aggregate():
    pass


@flow
def weather_pipeline():
    fetch_and_save_raw()



if __name__ == "__main__":
    weather_pipeline()


