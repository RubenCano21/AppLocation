# test_extraction.py
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from app.database.supabase_utils import get_supabase_client
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_supabase():
    """Prueba simple de conexión a Supabase"""
    try:
        client = get_supabase_client()

        # Contar registros
        logger.info("Counting records in Supabase...")
        count = client.count_records()
        logger.info(f"Total records: {count}")

        # Obtener primeros 10 registros
        logger.info("\nFetching first 10 records...")
        data = client.fetch_all(limit=10)

        logger.info(f"Fetched {len(data)} records")

        if data:
            logger.info("\nFirst record:")
            for key, value in data[0].items():
                logger.info(f"  {key}: {value}")

            logger.info("\nAll fields:")
            logger.info(list(data[0].keys()))

        return data

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return None


if __name__ == "__main__":
    asyncio.run(test_supabase())