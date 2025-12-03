# api/main.py
import os
from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

load_dotenv()

app = FastAPI()

DB_HOST = os.getenv("DEST_PG_HOST")
DB_PORT = int(os.getenv("DEST_PG_PORT", "5432"))
DB_NAME = os.getenv("DEST_PG_DB")
DB_USER = os.getenv("DEST_PG_USER")
DB_PASS = os.getenv("DEST_PG_PASSWORD")

def get_conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

@app.get("/results")
def get_results(limit: int = 100):
    """
    Retorna filas recientes de devices_summary_by_device_day.
    """
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT device_id, device_name, date, points_count, avg_speed, max_speed, avg_battery
            FROM public.devices_summary_by_device_day
            ORDER BY date DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
