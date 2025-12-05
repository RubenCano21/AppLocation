"""
Scheduler para ejecutar ETL automáticamente cada cierto tiempo
"""
import asyncio
import schedule
import time
from datetime import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.etl_service import ETLService
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def run_incremental_etl():
    """Ejecuta el ETL incremental"""
    logger.info("="*70)
    logger.info(f"SCHEDULED ETL EXECUTION - {datetime.now()}")
    logger.info("="*70)

    try:
        etl_service = ETLService()
        result = await etl_service.run_full_etl(incremental=True)

        logger.info(f"ETL Result: {result['status']}")
        logger.info(f"Records processed: {result.get('records_processed', 0)}")
        logger.info(f"Execution time: {result.get('execution_time', 0)}s")

    except Exception as e:
        logger.error(f"Error in scheduled ETL: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Cerrar Spark
        try:
            etl_service.spark.stop()
            logger.info("Spark stopped")
        except:
            pass


def job():
    """Función que ejecuta el ETL de forma síncrona"""
    asyncio.run(run_incremental_etl())


def start_scheduler(interval_minutes: int = 30):
    """
    Inicia el scheduler para ejecutar ETL automáticamente

    Args:
        interval_minutes: Intervalo en minutos entre ejecuciones
    """
    logger.info("="*70)
    logger.info("ETL SCHEDULER STARTED")
    logger.info("="*70)
    logger.info(f"Interval: Every {interval_minutes} minutes")
    logger.info(f"Mode: Incremental (only new data)")
    logger.info("="*70)

    # Programar la tarea
    schedule.every(interval_minutes).minutes.do(job)

    # Ejecutar inmediatamente al inicio (opcional)
    logger.info("\nRunning initial ETL...")
    job()

    # Loop principal
    logger.info(f"\nScheduler running. Press Ctrl+C to stop.")
    logger.info(f"Next run: {schedule.next_run()}\n")

    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  # Verificar cada minuto

            # Mostrar próxima ejecución
            next_run = schedule.next_run()
            if next_run:
                time_until = (next_run - datetime.now()).total_seconds()
                if time_until > 0 and time_until % 300 == 0:  # Cada 5 minutos
                    logger.info(f"Next ETL run in {int(time_until/60)} minutes")

        except KeyboardInterrupt:
            logger.info("\n\nScheduler stopped by user")
            break
        except Exception as e:
            logger.error(f"Error in scheduler loop: {e}")
            time.sleep(60)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='ETL Scheduler')
    parser.add_argument(
        '--interval',
        type=int,
        default=30,
        help='Interval in minutes between ETL runs (default: 30)'
    )

    args = parser.parse_args()

    start_scheduler(interval_minutes=args.interval)

