from sqlalchemy import create_engine, Column, Integer , Float , String , DateTime, JSON
from sqlalchemy.orm import declarative_base

from datetime import datetime
from dotenv import load_dotenv
import os



load_dotenv()
DB_URL = os.getenv("DATABASE_URL")
Base = declarative_base() 


class WeatherRaw(Base):

    __tablename__ = "weather_raw"

    id            = Column(Integer, primary_key=True)
    city          = Column(String(50))
    country       = Column(String(50))
    raw_data      = Column(JSON)
    ingested_at   = Column(DateTime, default=datetime.utcnow)




class WeatherClean(Base):
    __table__ = "weather_clean"

    id        = Column(Integer, primary_key=True)



class WeatherAgg(Base): 
    __table__ = "weather_agg"

    id        = Column(Integer, primary_key=True)



def get_engine():

    return create_engine(os.getenv("DATABASE_URL"))


def create_tables():

    engine = get_engine()

    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


if __name__ == "__main__":
    create_tables()
