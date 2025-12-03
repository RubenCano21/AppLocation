# supabase_utils.py
import requests
import json
from db_utils import SUPABASE_URL, SUPABASE_SERVICE_KEY, SUPABASE_TABLE, SUPABASE_FETCH_LIMIT

def fetch_all_supabase(limit=SUPABASE_FETCH_LIMIT):
    """
    Fetch rows from Supabase REST API.
    Nota: este método intenta traer hasta `limit` registros.
    Para datasets mas grandes implementa paginación con Range headers o filtros por timestamp.
    """
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    params = {
        "select": "*",
        "limit": str(limit)
    }

    resp = requests.get(url, headers=headers, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()

def insert_processed_to_supabase(table, records):
    """
    Inserta registros procesados de vuelta a Supabase via REST.
    `records` debe ser una lista de dicts.
    IMPORTANTE: usar con cuidado la Service Role Key.
    """
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }
    resp = requests.post(url, headers=headers, json=records, timeout=60)
    resp.raise_for_status()
    return resp.json()
