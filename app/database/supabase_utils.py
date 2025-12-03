# app/database/supabase_utils.py
import requests
from app.config import settings
import logging
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)


class SupabaseClient:
    """Cliente mejorado para interactuar con Supabase REST API"""

    def __init__(self):
        self.base_url = settings.SUPABASE_URL
        self.service_key = settings.SUPABASE_SERVICE_KEY
        self.table = settings.SUPABASE_TABLE
        self.headers = {
            "apikey": self.service_key,
            "Authorization": f"Bearer {self.service_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def fetch_all(self, limit: int = None, offset: int = 0) -> List[Dict]:
        """
        Fetch rows from Supabase REST API con paginación
        """
        if limit is None:
            limit = settings.SUPABASE_FETCH_LIMIT

        url = f"{self.base_url}/rest/v1/{self.table}"
        params = {
            "select": "*",
            "limit": str(limit),
            "offset": str(offset)
        }

        try:
            resp = requests.get(url, headers=self.headers, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            logger.info(f"Fetched {len(data)} records from Supabase (offset: {offset})")
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching from Supabase: {e}")
            raise

    def fetch_with_filters(
            self,
            filters: Optional[Dict] = None,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
            limit: int = None
    ) -> List[Dict]:
        """
        Fetch con filtros avanzados
        """
        if limit is None:
            limit = settings.SUPABASE_FETCH_LIMIT

        url = f"{self.base_url}/rest/v1/{self.table}"
        params = {"select": "*", "limit": str(limit)}

        # Agregar filtros de fecha
        if start_date:
            params["timestamp"] = f"gte.{start_date}"
        if end_date:
            params["timestamp"] = f"lte.{end_date}"

        # Agregar filtros personalizados
        if filters:
            for key, value in filters.items():
                params[key] = f"eq.{value}"

        try:
            resp = requests.get(url, headers=self.headers, params=params, timeout=60)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching with filters: {e}")
            raise

    def fetch_all_paginated(
            self,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None
    ) -> List[Dict]:
        """
        Fetch todos los registros con paginación automática
        """
        all_data = []
        offset = 0
        limit = settings.SUPABASE_FETCH_LIMIT

        while True:
            url = f"{self.base_url}/rest/v1/{self.table}"
            params = {
                "select": "*",
                "limit": str(limit),
                "offset": str(offset)
            }

            # Filtros de fecha
            if start_date:
                params["timestamp"] = f"gte.{start_date}"
            if end_date:
                params["timestamp"] = f"lte.{end_date}"

            try:
                resp = requests.get(url, headers=self.headers, params=params, timeout=60)
                resp.raise_for_status()
                data = resp.json()

                if not data:
                    break

                all_data.extend(data)
                logger.info(f"Fetched batch {offset // limit + 1}: {len(data)} records")

                if len(data) < limit:
                    break

                offset += limit

            except requests.exceptions.RequestException as e:
                logger.error(f"Error in pagination at offset {offset}: {e}")
                raise

        logger.info(f"Total records fetched: {len(all_data)}")
        return all_data

    def insert_records(self, table: str, records: List[Dict]) -> List[Dict]:
        """
        Inserta registros en una tabla de Supabase
        """
        url = f"{self.base_url}/rest/v1/{table}"
        headers = {**self.headers, "Prefer": "return=representation"}

        try:
            resp = requests.post(url, headers=headers, json=records, timeout=60)
            resp.raise_for_status()
            logger.info(f"Inserted {len(records)} records to {table}")
            return resp.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error inserting to Supabase: {e}")
            raise

    def count_records(self, filters: Optional[Dict] = None) -> int:
        """
        Cuenta registros en la tabla
        """
        url = f"{self.base_url}/rest/v1/{self.table}"
        headers = {**self.headers, "Prefer": "count=exact"}
        params = {"select": "*"}

        if filters:
            for key, value in filters.items():
                params[key] = f"eq.{value}"

        try:
            resp = requests.head(url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            count = int(resp.headers.get("Content-Range", "0-0/0").split("/")[1])
            logger.info(f"Total records in {self.table}: {count}")
            return count
        except requests.exceptions.RequestException as e:
            logger.error(f"Error counting records: {e}")
            raise


# Singleton instance
_supabase_client = None


def get_supabase_client() -> SupabaseClient:
    """Retorna instancia singleton del cliente Supabase"""
    global _supabase_client
    if _supabase_client is None:
        _supabase_client = SupabaseClient()
    return _supabase_client


# Funciones legacy para compatibilidad con tu código existente
def fetch_all_supabase(limit=None):
    """Legacy function - usa get_supabase_client().fetch_all() en su lugar"""
    client = get_supabase_client()
    return client.fetch_all(limit=limit)


def insert_processed_to_supabase(table, records):
    """Legacy function - usa get_supabase_client().insert_records() en su lugar"""
    client = get_supabase_client()
    return client.insert_records(table, records)