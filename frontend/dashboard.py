import streamlit as st
import requests
import pandas as pd
import numpy as np



API_URL = "http://localhost:8000"


@st.cache_data(ttl=300)
def fetch_weather(ttl=300):
    response = requests.get(f'http://localhost:8000/weather/summary')
    return pd.DataFrame(response.json())

# 1. Page oconfig - always first 

st.set_page_config(page_title="Weather Dashboard", layout = "wide")
st.title("Weather Dashboard")


df = fetch_weather()
st.subheader("Summary")

# --- Metric Cards --- 
#st.subheader("Summary")
city, avg_temp, max_temp = st.columns(3)

city.metric("Temperature", "72°F", "+2°F")
avg_temp.metric("Humidity", "58%", "-5%")
max_temp.metric("Pressure", "1013 hPa", "0 hPa")

st.divider() 


lin_df = pd.DataFrame(
    np.random.randn(20, 3),
    columns=["A", "B", "C"]
)

st.line_chart(lin_df)