"""
Ejecuta SQL directamente en PostgreSQL
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2
from app.config import settings


def run_sql():
    print("\n" + "="*70)
    print("AGREG ANDO COLUMNAS GEOGRÁFICAS")
    print("="*70 + "\n")

    try:
        conn = psycopg2.connect(
            host=settings.DEST_PG_HOST,
            port=settings.DEST_PG_PORT,
            database=settings.DEST_PG_DB,
            user=settings.DEST_PG_USER,
            password=settings.DEST_PG_PASSWORD
        )

        cur = conn.cursor()

        # Agregar columnas
        sqls = [
            "ALTER TABLE locations ADD COLUMN IF NOT EXISTS district_id INTEGER",
            "ALTER TABLE locations ADD COLUMN IF NOT EXISTS district_name VARCHAR(100)",
            "ALTER TABLE locations ADD COLUMN IF NOT EXISTS province_id INTEGER",
            "ALTER TABLE locations ADD COLUMN IF NOT EXISTS province_name VARCHAR(200)",
        ]

        for sql in sqls:
            print(f"Ejecutando: {sql}")
            cur.execute(sql)

        conn.commit()

        # Verificar
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='locations' 
            AND column_name IN ('district_id', 'district_name', 'province_id', 'province_name')
        """)

        columns = cur.fetchall()

        print("\n✓ Columnas agregadas:")
        for col in columns:
            print(f"  - {col[0]}")

        cur.close()
        conn.close()

        print("\n" + "="*70)
        print("✓ MIGRACIÓN COMPLETADA")
        print("="*70 + "\n")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_sql()

