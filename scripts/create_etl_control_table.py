"""
Crea la tabla de control ETL
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database.postgres_db import engine
from sqlalchemy import text


def create_etl_control_table():
    """Crea la tabla etl_control"""

    print("\n" + "="*70)
    print("CREANDO TABLA ETL_CONTROL")
    print("="*70 + "\n")

    sql = """
    CREATE TABLE IF NOT EXISTS etl_control (
        id SERIAL PRIMARY KEY,
        execution_date TIMESTAMP NOT NULL,
        last_processed_id BIGINT NOT NULL,
        records_processed INTEGER NOT NULL,
        status VARCHAR(20) NOT NULL,
        execution_time_seconds INTEGER,
        error_message VARCHAR(500)
    );
    
    CREATE INDEX IF NOT EXISTS idx_etl_control_date ON etl_control(execution_date DESC);
    CREATE INDEX IF NOT EXISTS idx_etl_control_status ON etl_control(status);
    """

    try:
        with engine.connect() as conn:
            for statement in sql.strip().split(';'):
                statement = statement.strip()
                if statement:
                    conn.execute(text(statement))
                    conn.commit()

        print("✓ Tabla etl_control creada exitosamente")
        print("\nEstructura:")
        print("  - id: SERIAL PRIMARY KEY")
        print("  - execution_date: TIMESTAMP")
        print("  - last_processed_id: BIGINT (último ID de Supabase procesado)")
        print("  - records_processed: INTEGER")
        print("  - status: VARCHAR(20) (SUCCESS, FAILED, RUNNING)")
        print("  - execution_time_seconds: INTEGER")
        print("  - error_message: VARCHAR(500)")

        print("\n" + "="*70 + "\n")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    create_etl_control_table()

