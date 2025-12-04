# test_supabase.py
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from app.database.supabase_utils import get_supabase_client
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_supabase():
    """Prueba conexión a Supabase y muestra datos"""
    try:
        client = get_supabase_client()

        print("\n" + "=" * 70)
        print("SUPABASE CONNECTION TEST")
        print("=" * 70)

        # Contar registros
        logger.info("Counting records in Supabase...")
        count = client.count_records()
        print(f"✅ Total records in Supabase: {count:,}")

        # Obtener primeros 10 registros
        logger.info("\nFetching first 10 records...")
        data = client.fetch_all(limit=10)

        print(f"✅ Fetched {len(data)} records")

        if data:
            print("\n" + "-" * 70)
            print("FIRST RECORD:")
            print("-" * 70)
            for key, value in data[0].items():
                print(f"  {key:20s}: {value}")

            print("\n" + "-" * 70)
            print("ALL FIELDS:")
            print("-" * 70)
            fields = list(data[0].keys())
            for i, field in enumerate(fields, 1):
                print(f"  {i:2d}. {field}")

            print("\n" + "-" * 70)
            print("DATA TYPES:")
            print("-" * 70)
            for key, value in data[0].items():
                print(f"  {key:20s}: {type(value).__name__}")

        print("\n" + "=" * 70)
        print("✅ SUPABASE CONNECTION SUCCESSFUL")
        print("=" * 70)

        return True

    except Exception as e:
        print("\n" + "=" * 70)
        print("❌ SUPABASE CONNECTION FAILED")
        print("=" * 70)
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    asyncio.run(test_supabase())