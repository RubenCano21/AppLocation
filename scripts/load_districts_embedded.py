"""
Plantilla para pegar el contenido del GeoJSON directamente
Si tienes problemas para encontrar el archivo, puedes pegar el contenido aquí
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy.orm import Session
from geoalchemy2.shape import from_shape
from shapely.geometry import shape, MultiPolygon, Polygon
from app.database.postgres_db import SessionLocal, init_db
from app.models.db_models import District
import logging
import json

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# PEGA AQUÍ EL CONTENIDO DE TU ARCHIVO GEOJSON
# Reemplaza None con el diccionario completo del GeoJSON
# ============================================================================

GEOJSON_DATA = None  # <-- PEGA TU GEOJSON AQUÍ

# Ejemplo de cómo debe verse:
# GEOJSON_DATA = {
#     "type": "FeatureCollection",
#     "features": [
#         {
#             "type": "Feature",
#             "properties": {
#                 "DISTRITO": 1,
#                 "NOMBRE": "Distrito 1"
#             },
#             "geometry": {
#                 "type": "MultiPolygon",
#                 "coordinates": [[[[...]]]]
#             }
#         },
#         ...
#     ]
# }

# ============================================================================


def calculate_area_perimeter(geom):
    """Calcula área y perímetro aproximados en km"""
    try:
        area_degrees = geom.area
        area_km2 = area_degrees * 111 * 106
        perimeter_degrees = geom.length
        perimeter_km = perimeter_degrees * 108.5
        return round(area_km2, 2), round(perimeter_km, 2)
    except:
        return None, None


def extract_district_info(properties: dict, idx: int):
    """Extrae número y nombre del distrito"""
    import re

    district_num = (
        properties.get('DISTRITO') or
        properties.get('distrito') or
        properties.get('district') or
        (idx + 1)
    )

    district_name = (
        properties.get('NOMBRE') or
        properties.get('nombre') or
        properties.get('name') or
        f"Distrito {district_num}"
    )

    if isinstance(district_num, str):
        match = re.search(r'\d+', str(district_num))
        if match:
            district_num = int(match.group())
        else:
            district_num = idx + 1

    return int(district_num), str(district_name)


def load_from_embedded_data():
    """Carga los distritos desde el GeoJSON embebido"""

    if GEOJSON_DATA is None:
        logger.error("="*60)
        logger.error("ERROR: No se ha definido GEOJSON_DATA")
        logger.error("="*60)
        logger.error("\nPor favor:")
        logger.error("1. Abre el archivo santa-cruz-distritos.geojson")
        logger.error("2. Copia todo su contenido")
        logger.error("3. Pégalo en la variable GEOJSON_DATA de este script")
        logger.error("4. Asegúrate de que tenga la estructura correcta")
        logger.error("\nO mejor aún, usa:")
        logger.error("  python scripts/load_districts_simple.py ruta/al/archivo.geojson")
        return

    logger.info("="*60)
    logger.info("CARGANDO DISTRITOS DESDE DATOS EMBEBIDOS")
    logger.info("="*60)

    # Validar estructura
    if GEOJSON_DATA.get('type') != 'FeatureCollection':
        logger.error("✗ El GeoJSON no es un FeatureCollection válido")
        return

    features = GEOJSON_DATA.get('features', [])
    logger.info(f"✓ {len(features)} features encontrados")

    # Inicializar DB
    logger.info("\nInicializando base de datos...")
    init_db()

    db = SessionLocal()

    try:
        # Limpiar tabla
        logger.info("Limpiando tabla de distritos...")
        deleted = db.query(District).delete()
        db.commit()
        logger.info(f"✓ {deleted} registros eliminados")

        # Procesar features
        logger.info("\nProcesando distritos...")
        logger.info("-"*60)

        count = 0
        for idx, feature in enumerate(features):
            try:
                properties = feature.get('properties', {})
                geometry = feature.get('geometry')

                if not geometry:
                    continue

                district_num, district_name = extract_district_info(properties, idx)
                geom_shape = shape(geometry)

                if isinstance(geom_shape, Polygon):
                    geom_shape = MultiPolygon([geom_shape])
                elif not isinstance(geom_shape, MultiPolygon):
                    continue

                area_km2, perimeter_km = calculate_area_perimeter(geom_shape)

                district = District(
                    district_number=district_num,
                    district_name=district_name,
                    geometry=from_shape(geom_shape, srid=4326),
                    area_km2=area_km2,
                    perimeter_km=perimeter_km
                )

                db.add(district)
                count += 1
                logger.info(f"✓ Distrito {district_num}: {district_name} ({area_km2} km²)")

            except Exception as e:
                logger.error(f"✗ Error en feature {idx}: {e}")
                continue

        db.commit()
        logger.info("-"*60)
        logger.info(f"\n✓ {count} distritos cargados exitosamente")

        # Resumen
        districts = db.query(District).order_by(District.district_number).all()
        total_area = sum(d.area_km2 for d in districts if d.area_km2)

        logger.info("\n" + "="*60)
        logger.info("RESUMEN")
        logger.info("="*60)
        logger.info(f"Total de distritos: {len(districts)}")
        logger.info(f"Área total: {round(total_area, 2)} km²")
        logger.info("")

        for d in districts:
            logger.info(f"  • Distrito {d.district_number}: {d.district_name}")

        logger.info("\n✓ PROCESO COMPLETADO")
        logger.info("="*60)

    except Exception as e:
        logger.error(f"\n✗ Error: {e}")
        db.rollback()
        import traceback
        traceback.print_exc()
    finally:
        db.close()


if __name__ == "__main__":
    load_from_embedded_data()

