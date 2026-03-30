from sqlalchemy import create_engine,text
from dotenv import load_dotenv,find_dotenv
import os

load_dotenv(find_dotenv())

DATABASE_URL= os.getenv("DATABASE_URL")
print(DATABASE_URL)
engine = create_engine(DATABASE_URL)

def query(sql: str, params: dict = {}):

    with engine.connect() as conn: 
        result = conn.execute(text(sql),params)
        return [dict(row._mapping) for row in result]