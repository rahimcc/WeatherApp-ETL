from fastapi import FastAPI, HTTPException
from sqlalchemy.orm import Session
from database.models import WeatherRaw, WeatherClean, get_engine
from datetime import datetime
import sys, os
#sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
pipeline_run = {'status': False }



app = FastAPI()

@app.get("/")
def root():
    return {"status":"Weather api is running"}


@app.get("/weather/clean/latest")
def get_latest_raw():
    engine = get_engine()
    with Session(engine) as session:
        record = session.query(WeatherClean).order_by(WeatherClean.\
                                                      ingested_at.desc()).first()

        if not record:
            raise HTTPException(status_code=404, detail= "No data found")
        
    return record.to_dict() 
