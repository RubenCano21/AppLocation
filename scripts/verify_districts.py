"""
Script para verificar los distritos cargados en la base de datos
"""
import sys
from pathlib import Path

# Agregar el directorio raíz al path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database.postgres_db import SessionLocal
from app.models.db_models import District
from app.services.district_service import DistrictService
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def verify_districts():
    """Verifica los distritos cargados"""
    db = SessionLocal()

    try:
        # Obtener todos los distritos
        districts = DistrictService.get_all_districts(db)

        if not districts:
            logger.warning("⚠ No districts found in database")
            return

        logger.info(f"\n{'='*60}")
        logger.info(f"DISTRICTS IN DATABASE: {len(districts)}")
        logger.info(f"{'='*60}\n")

        total_area = 0
        for district in districts:
            logger.info(f"District {district.district_number}: {district.district_name}")
            logger.info(f"  Area: {district.area_km2} km²")
            logger.info(f"  Perimeter: {district.perimeter_km} km")
            logger.info(f"  Created: {district.created_at}")
            logger.info("")

            if district.area_km2:
                total_area += district.area_km2

        logger.info(f"{'='*60}")
        logger.info(f"TOTAL AREA: {round(total_area, 2)} km²")
        logger.info(f"{'='*60}\n")

        # Obtener estadísticas
        logger.info("Getting statistics for each district...")
        stats = DistrictService.get_all_districts_statistics(db)

        logger.info(f"\n{'='*60}")
        logger.info("DISTRICT STATISTICS")
        logger.info(f"{'='*60}\n")

        for stat in stats:
            logger.info(f"District {stat.get('district_number')}: {stat.get('district_name')}")
            logger.info(f"  Total Locations: {stat.get('total_locations', 0)}")
            logger.info(f"  Unique Devices: {stat.get('unique_devices', 0)}")
            logger.info(f"  Avg Battery: {stat.get('avg_battery', 'N/A')}")
            logger.info(f"  Avg Signal: {stat.get('avg_signal', 'N/A')}")
            logger.info("")

    except Exception as e:
        logger.error(f"✗ Error verifying districts: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


if __name__ == "__main__":
    verify_districts()

