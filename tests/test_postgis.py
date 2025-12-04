# test_postgis.py
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from sqlalchemy import create_engine, text
from app.config import settings
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_postgis():
    """Prueba la instalación de PostGIS"""
    try:
        print("\n" + "=" * 70)
        print("POSTGIS TEST")
        print("=" * 70)

        engine = create_engine(settings.postgres_url)

        with engine.connect() as conn:
            # 1. Versión de PostgreSQL
            result = conn.execute(text("SELECT version();"))
            pg_version = result.fetchone()[0]
            print(f"\n✅ PostgreSQL: {pg_version[:80]}...")

            # 2. Verificar PostGIS
            try:
                result = conn.execute(text("SELECT PostGIS_Version();"))
                postgis_version = result.fetchone()[0]
                print(f"✅ PostGIS version: {postgis_version}")
            except Exception as e:
                print(f"❌ PostGIS NOT installed: {e}")
                print("\nTo install PostGIS, run in PgAdmin:")
                print("CREATE EXTENSION IF NOT EXISTS postgis;")
                return False

            # 3. Probar crear geometría
            print("\n" + "-" * 70)
            print("Testing geometry creation...")
            print("-" * 70)

            conn.execute(text("""
                              CREATE TABLE IF NOT EXISTS test_geometry
                              (
                                  id       SERIAL PRIMARY KEY,
                                  location GEOMETRY(Point, 4326)
                              );
                              """))

            conn.execute(text("""
                              INSERT INTO test_geometry (location)
                              VALUES (ST_GeomFromText('POINT(-63.1821 -17.7833)', 4326));
                              """))

            result = conn.execute(text("""
                                       SELECT id, ST_AsText(location) as location
                                       FROM test_geometry;
                                       """))

            for row in result:
                print(f"✅ Test geometry created: ID={row[0]}, Location={row[1]}")

            conn.execute(text("DROP TABLE IF EXISTS test_geometry;"))
            conn.commit()

            print("\n" + "=" * 70)
            print("✅ POSTGIS IS WORKING CORRECTLY")
            print("=" * 70)
            return True

    except Exception as e:
        print("\n" + "=" * 70)
        print("❌ POSTGIS TEST FAILED")
        print("=" * 70)
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    test_postgis()