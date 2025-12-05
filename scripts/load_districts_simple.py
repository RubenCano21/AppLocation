"""
Script simplificado para cargar distritos desde un archivo GeoJSON
Uso: python scripts/load_districts_simple.py [ruta_al_archivo.geojson]
"""
import json
import sys
from pathlib import Path

# Agregar el directorio raíz al path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy.orm import Session
from geoalchemy2.shape import from_shape
from shapely.geometry import shape, MultiPolygon, Polygon
from app.database.postgres_db import SessionLocal, init_db
from app.models.db_models import District
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def load_geojson(file_path: str):
    """Carga el archivo GeoJSON"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def extract_district_info(properties: dict, idx: int):
    """Extrae número y nombre del distrito de las propiedades"""
    # Intentar diferentes campos comunes
    district_num = (
        properties.get('DISTRITO') or
        properties.get('distrito') or
        properties.get('district') or
        properties.get('Distrito') or
        properties.get('DISTRICT_N') or
        properties.get('id') or
        (idx + 1)
    )

    district_name = (
        properties.get('NOMBRE') or
        properties.get('nombre') or
        properties.get('name') or
        properties.get('Nombre') or
        properties.get('NAME') or
        f"Distrito {district_num}"
    )

    # Convertir a int si es string
    if isinstance(district_num, str):
        # Eliminar texto y extraer número
        import re
        match = re.search(r'\d+', str(district_num))
        if match:
            district_num = int(match.group())
        else:
            district_num = idx + 1

    return int(district_num), str(district_name)


def calculate_area_perimeter(geom):
    """Calcula área y perímetro aproximados en km"""
    try:
        # Aproximación para Santa Cruz de la Sierra
        # Latitud aprox: -17.8°
        # 1° lat ≈ 111 km
        # 1° lon ≈ 106 km (a esta latitud)

        area_degrees = geom.area
        area_km2 = area_degrees * 111 * 106

        perimeter_degrees = geom.length
        perimeter_km = perimeter_degrees * 108.5

        return round(area_km2, 2), round(perimeter_km, 2)
    except:
        return None, None


def load_districts(geojson_path: str):
    """Carga los distritos en la base de datos"""

    logger.info("="*60)
    logger.info("CARGANDO DISTRITOS DE SANTA CRUZ")
    logger.info("="*60)

    # Verificar archivo
    if not Path(geojson_path).exists():
        logger.error(f"✗ Archivo no encontrado: {geojson_path}")
        return

    logger.info(f"✓ Archivo encontrado: {geojson_path}")

    # Cargar GeoJSON
    logger.info("Cargando archivo GeoJSON...")
    data = load_geojson(geojson_path)

    if data.get('type') != 'FeatureCollection':
        logger.error("✗ El archivo no es un FeatureCollection válido")
        return

    features = data.get('features', [])
    logger.info(f"✓ {len(features)} features encontrados")

    # Inicializar base de datos
    logger.info("\nInicializando base de datos...")
    init_db()
    logger.info("✓ Base de datos inicializada")

    # Crear sesión
    db = SessionLocal()

    try:
        # Limpiar tabla existente
        logger.info("\nLimpiando tabla de distritos...")
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
                    logger.warning(f"⚠ Feature {idx} sin geometría, omitiendo")
                    continue

                # Extraer información
                district_num, district_name = extract_district_info(properties, idx)

                # Convertir geometría
                geom_shape = shape(geometry)

                # Asegurar MultiPolygon
                if isinstance(geom_shape, Polygon):
                    geom_shape = MultiPolygon([geom_shape])
                elif not isinstance(geom_shape, MultiPolygon):
                    logger.warning(f"⚠ Geometría no soportada: {geom_shape.geom_type}")
                    continue

                # Calcular área y perímetro
                area_km2, perimeter_km = calculate_area_perimeter(geom_shape)

                # Crear registro
                district = District(
                    district_number=district_num,
                    district_name=district_name,
                    geometry=from_shape(geom_shape, srid=4326),
                    area_km2=area_km2,
                    perimeter_km=perimeter_km
                )

                db.add(district)
                count += 1

                logger.info(f"✓ Distrito {district_num}: {district_name}")
                logger.info(f"  Área: {area_km2} km²")

            except Exception as e:
                logger.error(f"✗ Error en feature {idx}: {e}")
                continue

        # Guardar cambios
        logger.info("-"*60)
        logger.info("\nGuardando en base de datos...")
        db.commit()
        logger.info(f"✓ {count} distritos cargados exitosamente")

        # Resumen
        logger.info("\n" + "="*60)
        logger.info("RESUMEN")
        logger.info("="*60)

        districts = db.query(District).order_by(District.district_number).all()
        total_area = sum(d.area_km2 for d in districts if d.area_km2)

        logger.info(f"Total de distritos: {len(districts)}")
        logger.info(f"Área total: {round(total_area, 2)} km²")
        logger.info("")

        for d in districts:
            logger.info(f"  • Distrito {d.district_number}: {d.district_name} ({d.area_km2} km²)")

        logger.info("\n" + "="*60)
        logger.info("✓ PROCESO COMPLETADO")
        logger.info("="*60)

    except Exception as e:
        logger.error(f"\n✗ Error: {e}")
        db.rollback()
        import traceback
        traceback.print_exc()
    finally:
        db.close()


def main():
    """Función principal"""
    if len(sys.argv) > 1:
        geojson_path = sys.argv[1]
    else:
        # Buscar archivo en ubicaciones comunes
        possible_paths = [
            "santa-cruz-distritos.geojson",
            "data/santa-cruz-distritos.geojson",
            "../santa-cruz-distritos.geojson",
        ]

        geojson_path = None
        for path in possible_paths:
            if Path(path).exists():
                geojson_path = path
                break

        if not geojson_path:
            logger.info("Ingrese la ruta al archivo GeoJSON:")
            logger.info("Ejemplo: santa-cruz-distritos.geojson")
            logger.info("O use: python scripts/load_districts_simple.py ruta/al/archivo.geojson")
            geojson_path = input("\nRuta: ").strip()

    load_districts(geojson_path)


if __name__ == "__main__":
    main()

