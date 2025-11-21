from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD", "admin")
PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = os.getenv("PGPORT", "5432")

PGDATABASE_BRONZE = os.getenv("BRONZE_PGDATABASE", "bronze")
BRONZE_DATABASE_URL = (f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE_BRONZE}")

PGDATABASE_SILVER = os.getenv("SILVER_PGDATABASE", "silver")
SILVER_DATABASE_URL = (f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE_SILVER}")

def get_bronze_engine():
    return create_engine(BRONZE_DATABASE_URL, future=True)

def get_silver_engine():
    return create_engine(SILVER_DATABASE_URL, future=True)