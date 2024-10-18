import psycopg
import os

def connect():
    db_url = os.getenv('DATABASE_URL')
    conn = psycopg.connect(db_url)
    return conn


