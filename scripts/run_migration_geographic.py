"""
Ejecuta migración para agregar campos de distrito y provincia
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database.postgres_db import engine
from sqlalchemy import text
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def run_migration():
    print("\n" + "="*70)
    print("EJECUTANDO MIGRACIÓN: Agregar campos geográficos")
    print("="*70 + "\n")

    migration_file = Path(__file__).parent.parent / "migrations" / "add_geographic_location_fields.sql"

    try:
        with open(migration_file, 'r', encoding='utf-8') as f:
            sql = f.read()

        print(f"Archivo: {migration_file.name}")
        print("\nEjecutando SQL...")

        with engine.connect() as conn:
            # Ejecutar cada statement
            for statement in sql.split(';'):
                statement = statement.strip()
                if statement and not statement.startswith('--'):
                    conn.execute(text(statement))
                    conn.commit()

        print("\n✓ Migración ejecutada exitosamente")
        print("\nColumnas agregadas:")
        print("  - district_id")
        print("  - district_name")
        print("  - province_id")
        print("  - province_name")
        print("\n" + "="*70 + "\n")

    except Exception as e:
        print(f"\n✗ Error ejecutando migración: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_migration()

