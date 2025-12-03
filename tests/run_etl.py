# run_etl.py
import asyncio
import sys
from pathlib import Path

# Agregar el directorio raíz al path
sys.path.insert(0, str(Path(__file__).parent))

from app.services.etl_service import ETLService
from app.database.postgres_db import init_db, test_connection
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    """
    Script principal para ejecutar el ETL
    """
    try:
        # 1. Verificar conexión a PostgreSQL
        logger.info("Checking PostgreSQL connection...")
        if not test_connection():
            logger.error("Cannot connect to PostgreSQL. Please check your configuration.")
            return

        # 2. Inicializar base de datos (crear tablas si no existen)
        logger.info("Initializing database...")
        init_db()

        # 3. Crear servicio ETL
        logger.info("Initializing ETL service...")
        etl_service = ETLService()

        # 4. Ejecutar ETL
        logger.info("\nStarting ETL pipeline...\n")

        result = await etl_service.run_full_etl(
            start_date=None,  # Puedes filtrar: "2024-01-01"
            end_date=None,  # Puedes filtrar: "2024-12-31"
            force_refresh=False  # True para sobrescribir datos existentes
        )

        # 5. Mostrar resultados
        logger.info("\n" + "=" * 70)
        logger.info("ETL RESULTS")
        logger.info("=" * 70)
        logger.info(f"Status: {result['status']}")
        logger.info(f"Records processed: {result['records_processed']}")
        logger.info(f"Records inserted: {result['records_inserted']}")
        logger.info(f"Execution time: {result['execution_time']} seconds")

        if result['status'] == 'success':
            logger.info(f"Grid cells: {result['grid_cells']}")
            logger.info(f"Unique devices: {result['unique_devices']}")

        if result.get('message'):
            logger.info(f"Message: {result['message']}")

        logger.info("=" * 70)

        # 6. Limpiar recursos
        etl_service.cleanup()

    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
    finally:
        logger.info("\nETL script finished.")


if __name__ == "__main__":
    asyncio.run(main())