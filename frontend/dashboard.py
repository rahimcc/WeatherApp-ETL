import streamlit as st
import requests
from dotenv import load_dotenv
from datetime import timezone, datetime, timedelta
import time



API_URL = "http://localhost:8000"

 
def fetch_weather():
    response = requests.get(f'http://localhost:8000/weather/clean/latest')
    return response.json()

# 1. Page oconfig - always first 

st.set_page_config(
    page_title="Weather Dashboard",
    page_icon = '🌤',
    layout = "wide")


st.title("Weather Dashboard")
st.subheader("Current Weather")

latest = fetch_weather()


if latest: 
     
    print(latest)
   
    now = datetime.now(timezone.utc)
    print(f"Ingestion time: latest['ingested_at']")
    print(f'Now: {now}')

    ingested_at = datetime.fromisoformat(latest['ingested_at'])
    print(ingested_at)
    elapsed = (now - ingested_at).total_seconds()
    print(elapsed)

    wait_time = int(max(0,60-elapsed))


    col1, col2 , col3 , col4 = st.columns(4)

    with col1:
        
        st.metric(
            label="City",
            value=f'{latest['city']}'
        )

    with col2: 
        st.metric(
            label = 'Temperature',
            value = f'{latest['temperature_c']}°C'
        )

    with col3:
        st.metric(
            label = 'Humidity',
            value = f'{latest['humidity']}'
        )

    with col4:
        st.metric(
            label = "Description",
            value = f'{latest['description']}'
        )

  

    # --- Metric Cards --- 
    #st.subheader("Summary")

    #city, avg_temp, max_temp = st.columns(3)

    #city.metric("Temperature", "72°F", "+2°F")
    #avg_temp.metric("Humidity", "58%", "-5%")
    #max_temp.metric("Pressure", "1013 hPa", "0 hPa")


    raw_data_slot = st.empty()

   # raw_data_slot.caption(f'Data slot: {latest}')

    timeslot = st.empty()
    timeslot.caption(f'Current Time: {ingested_at}')


    countdown = st.empty()
    for i in range(30, 0, -1):
        countdown.caption(f"Next update in {i} seconds")
        time.sleep(1)

    countdown.caption("Fetching new data...")
    st.rerun()
