from sqlalchemy import create_engine, Column, Integer , Float , String , DateTime, JSON, event
from sqlalchemy.orm import declarative_base, sessionmaker

from datetime import datetime , timezone
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
    ingested_at   = Column(DateTime, default= datetime.now(timezone.utc))


    def to_dict(self):
        return { 
            'id': self.id,
            'city': self.city, 
            'country': self.country,
            'raw_data': self.raw_data,
            'ingested_at': self.ingested_at
        }


class WeatherClean(Base):
    __tablename__   = "weather_clean"

    id              = Column(Integer, primary_key=True)
    city            = Column(String(50))
    country         = Column(String(50))
    temperature_c   = Column(Float)
    feels_like_c    = Column(Float)
    humidity        = Column(Float)
    description     = Column(String(50))
    ingested_at     = Column(DateTime(timezone=True))

    def to_dict(self):
        return { 
            'id': self.id,
            'city': self.city, 
            'country': self.country,
            'temperature_c': self.temperature_c, 
            'feels_like_c': self.feels_like_c,
            'humidity': self.humidity,
            'description': self.description,
            'ingested_at': str(self.ingested_at)
        }

class WeatherAgg(Base): 
    __tablename__ = "weather_agg"

    id        = Column(Integer, primary_key=True)




def get_engine():

    engine = create_engine(os.getenv("DATABASE_URL"))
    @event.listens_for(engine, "connect")
    def set_timezone(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("SET timezone = 'UTC'")
        cursor.close()
    return engine


def create_tables():

    engine = get_engine()
    Base.metadata.create_all(engine)

def get_session():
    engine = get_engine()
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal


if __name__ == "__main__":
    create_tables()
