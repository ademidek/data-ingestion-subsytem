import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

DB_NAME = os.getenv("PGDB", "esophageal_db")
DB_USER = os.getenv("PGUSER", "ademidek")
DB_PASS = os.getenv("PGPASS", "")
DB_HOST = os.getenv("PGHOST", "127.0.0.1")
DB_PORT = os.getenv("PGPORT", "5432")

def get_conn():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS or None,
        host=DB_HOST,
        port=DB_PORT,
    )
