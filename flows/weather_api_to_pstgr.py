import requests
import psycopg2
import os
import sys 
import sqlalchemy

from dotenv import load_dotenv
from datetime import datetime,timedelta,timezone
from sqlalchemy import create_engine,text
from sqlalchemy.orm import Session

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.models import WeatherRaw,WeatherClean,get_engine
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

        country = data['sys']['country']

        
        engine = get_engine()
        ingested_at = datetime.now(timezone.utc)
        print(f'Ingestion time: {ingested_at}')
        with Session(engine) as session:
       
            records = [  WeatherRaw(city = city, country= country,
                                     raw_data = data, ingested_at=ingested_at)
                       ]
        
            session.add_all(records)
            session.commit()
        
        print(f"Saved data for {city}")

        return ingested_at
            


@task
def transform_to_clean(ingested_at):
    
    engine = get_engine() 
    print('a')
    with Session(engine) as session:
        records = session.query(WeatherRaw).\
        filter(WeatherRaw.ingested_at == ingested_at).first() 
    
    records = records.to_dict()

    id = records['id']
    city = records['city']
    country = records['country']
    temperature_c = records['raw_data']['main']['temp']
    feels_like_c  = records['raw_data']['main']['feels_like']
    humidity = records['raw_data']['main']['humidity']
    description = records['raw_data']['weather'][0]['description']


    with Session(engine) as session: 
        
        new_record = WeatherClean( id = id , city=city, country = country, \
                                  temperature_c = temperature_c , feels_like_c = feels_like_c,\
                                  humidity = humidity , description= description,\
                                 ingested_at= ingested_at
                                  )
        session.add(new_record)
        session.commit()

@task
def aggregate():
    pass


@flow
def weather_pipeline():
    ingested_at = fetch_and_save_raw()
    print(ingested_at)
    transform_to_clean(ingested_at)



if __name__ == "__main__":
    weather_pipeline.serve(name="weather_schedule", interval=timedelta(minutes=1))


