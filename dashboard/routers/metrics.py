from fastapi import APIRouter
from database import query


router = APIRouter(prefix ="/weather")


@router.get("/summary")
def summary():
    return query("""
            SELECT 
                  city,
                  avg_temp_c,
                  max_temp_c
            FROM weather_daily_agg                 
""")