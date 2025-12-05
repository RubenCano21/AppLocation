"""
Test de ETL Incremental
Ejecuta el ETL una sola vez en modo incremental
"""
import asyncio
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.etl_service import ETLService
from app.database.postgres_db import SessionLocal
from app.models.etl_control import ETLControl
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


async def test_incremental_etl():
    """Prueba el ETL incremental"""

    print("\n" + "="*70)
    print("TEST: ETL INCREMENTAL")
    print("="*70)

    # Mostrar última ejecución
    db = SessionLocal()
    try:
        last_run = db.query(ETLControl).filter(
            ETLControl.status == 'SUCCESS'
        ).order_by(ETLControl.execution_date.desc()).first()

        if last_run:
            print(f"\nÚltima ejecución exitosa:")
            print(f"  Fecha: {last_run.execution_date}")
            print(f"  Último ID: {last_run.last_processed_id}")
            print(f"  Registros: {last_run.records_processed}")
            print(f"  Tiempo: {last_run.execution_time_seconds}s")
        else:
            print("\nNo hay ejecuciones previas, se cargará todo")
    finally:
        db.close()

    print("\n" + "-"*70)
    print("Iniciando ETL incremental...")
    print("-"*70 + "\n")

    # Ejecutar ETL
    etl_service = ETLService()
    try:
        result = await etl_service.run_full_etl(incremental=True)

        print("\n" + "="*70)
        print("RESULTADO")
        print("="*70)
        print(f"Estado: {result['status']}")
        print(f"Registros procesados: {result.get('records_processed', 0)}")
        print(f"Registros insertados: {result.get('records_inserted', 0)}")
        print(f"Tiempo: {result.get('execution_time', 0)}s")
        if 'last_id' in result:
            print(f"Último ID: {result['last_id']}")
        print("="*70 + "\n")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        etl_service.spark.stop()


if __name__ == "__main__":
    asyncio.run(test_incremental_etl())

