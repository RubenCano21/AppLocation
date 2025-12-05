"""
Script para cargar los distritos de Santa Cruz desde GeoJSON a la base de datos
"""
import json
import sys
import os
from pathlib import Path

# Agregar el directorio raíz al path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy.orm import Session
from geoalchemy2.shape import from_shape
from shapely.geometry import shape, MultiPolygon, Polygon
from app.database.postgres_db import SessionLocal, init_db
from app.models.db_models import District
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def calculate_area_perimeter(geom):
    """Calcula área y perímetro en km usando proyección aproximada"""
    try:
        # Para cálculos precisos, deberíamos proyectar a UTM
        # Aquí usamos una aproximación simple
        # 1 grado ≈ 111 km en latitud
        # En Santa Cruz (aprox -17°), 1 grado de longitud ≈ 106 km

        # Convertir de grados cuadrados a km²
        area_degrees = geom.area
        # Aproximación para Santa Cruz
        area_km2 = area_degrees * 111 * 106

        # Perímetro
        perimeter_degrees = geom.length
        perimeter_km = perimeter_degrees * 108.5  # Promedio

        return round(area_km2, 2), round(perimeter_km, 2)
    except Exception as e:
        logger.warning(f"Error calculating area/perimeter: {e}")
        return None, None


def load_districts_from_geojson(geojson_path: str, db: Session):
    """
    Carga los distritos desde un archivo GeoJSON

    Args:
        geojson_path: Ruta al archivo GeoJSON
        db: Sesión de base de datos
    """
    try:
        # Leer archivo GeoJSON
        with open(geojson_path, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)

        logger.info(f"✓ GeoJSON file loaded: {geojson_path}")

        # Validar estructura
        if geojson_data.get('type') != 'FeatureCollection':
            raise ValueError("GeoJSON must be a FeatureCollection")

        features = geojson_data.get('features', [])
        logger.info(f"Found {len(features)} features")

        # Limpiar datos existentes
        deleted = db.query(District).delete()
        logger.info(f"Deleted {deleted} existing districts")

        # Procesar cada feature
        districts_added = 0
        for idx, feature in enumerate(features):
            try:
                properties = feature.get('properties', {})
                geometry = feature.get('geometry')

                if not geometry:
                    logger.warning(f"Feature {idx} has no geometry, skipping")
                    continue

                # Extraer número y nombre del distrito
                # Ajusta estos campos según la estructura real de tu GeoJSON
                district_number = properties.get('DISTRITO') or properties.get('distrito') or properties.get('district') or (idx + 1)
                district_name = properties.get('NOMBRE') or properties.get('nombre') or properties.get('name') or f"Distrito {district_number}"

                # Convertir a int si es necesario
                if isinstance(district_number, str):
                    district_number = int(district_number.replace('Distrito ', '').replace('DISTRITO ', '').strip())

                # Convertir geometría a Shapely
                geom_shape = shape(geometry)

                # Asegurar que sea MultiPolygon
                if isinstance(geom_shape, Polygon):
                    geom_shape = MultiPolygon([geom_shape])
                elif not isinstance(geom_shape, MultiPolygon):
                    logger.warning(f"Feature {idx} has unsupported geometry type: {geom_shape.geom_type}")
                    continue

                # Calcular área y perímetro
                area_km2, perimeter_km = calculate_area_perimeter(geom_shape)

                # Crear objeto District
                district = District(
                    district_number=district_number,
                    district_name=district_name,
                    geometry=from_shape(geom_shape, srid=4326),
                    area_km2=area_km2,
                    perimeter_km=perimeter_km
                )

                db.add(district)
                districts_added += 1
                logger.info(f"✓ Added District {district_number}: {district_name} ({area_km2} km²)")

            except Exception as e:
                logger.error(f"Error processing feature {idx}: {e}")
                continue

        # Commit
        db.commit()
        logger.info(f"✓ Successfully loaded {districts_added} districts")

        return districts_added

    except Exception as e:
        logger.error(f"✗ Error loading districts: {e}")
        db.rollback()
        raise


def main():
    """Función principal"""
    # Ruta al archivo GeoJSON
    geojson_path = input("Ingrese la ruta al archivo GeoJSON de distritos: ").strip()

    if not os.path.exists(geojson_path):
        logger.error(f"✗ File not found: {geojson_path}")
        return

    # Inicializar base de datos
    logger.info("Initializing database...")
    init_db()

    # Crear sesión
    db = SessionLocal()

    try:
        # Cargar distritos
        count = load_districts_from_geojson(geojson_path, db)
        logger.info(f"✓ Process completed: {count} districts loaded")

        # Mostrar resumen
        districts = db.query(District).order_by(District.district_number).all()
        logger.info("\n=== DISTRICTS SUMMARY ===")
        for district in districts:
            logger.info(f"  {district.district_number}: {district.district_name} - {district.area_km2} km²")

    except Exception as e:
        logger.error(f"✗ Error: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    main()

