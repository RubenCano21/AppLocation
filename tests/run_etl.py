# run_etl.py
import asyncio
import sys
from pathlib import Path

# Agregar el directorio raíz al path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.etl_service import ETLService
from app.database.postgres_db import init_db, test_connection
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    try:
        # Verificar PostgreSQL
        logger.info("Checking PostgreSQL connection...")
        if not test_connection():
            logger.error("Cannot connect to PostgreSQL")
            return

        # Inicializar base de datos
        logger.info("Initializing database...")
        init_db()

        # Ejecutar ETL
        logger.info("Starting ETL pipeline...\n")
        etl_service = ETLService()

        result = await etl_service.run_full_etl(
            start_date=None,
            end_date=None,
            force_refresh=False
        )

        # Mostrar resultados
        logger.info("\n" + "=" * 70)
        logger.info("ETL RESULTS")
        logger.info("=" * 70)
        logger.info(f"Status: {result['status']}")
        logger.info(f"Records processed: {result.get('records_processed', 0):,}")
        logger.info(f"Records inserted: {result.get('records_inserted', 0):,}")
        logger.info(f"Execution time: {result['execution_time']:.2f}s")
        logger.info("=" * 70)

        # Cleanup
        etl_service.cleanup()

    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())