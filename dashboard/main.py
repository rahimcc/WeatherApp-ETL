from fastapi import FastAPI, HTTPException
from sqlalchemy.orm import Session
from database.models import WeatherRaw, WeatherClean, get_session
from datetime import datetime
from routers import metrics



app = FastAPI()
app.include_router(metrics.router)


@app.get("/")
def root():
    return {"status":"weather api is running"}


@app.get("/weather/clean/latest")
def get_latest_raw():
    with get_session as session:
        record = session.query(WeatherClean).order_by(WeatherClean.\
                                                      ingested_at.desc()).first()

        if not record:
            raise HTTPException(status_code=404, detail= "No data found")
        
    return record.to_dict() 


