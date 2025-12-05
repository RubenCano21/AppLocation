"""
Script automático para cargar los distritos de Santa Cruz
Lee el archivo santa-cruz-distritos.geojson del proyecto
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

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def calculate_area_perimeter(geom):
    """Calcula área y perímetro aproximados en km para Santa Cruz"""
    try:
        # Santa Cruz está aproximadamente a -17.8° de latitud
        # 1° lat ≈ 111 km
        # 1° lon ≈ 106 km (a esta latitud)

        area_degrees = geom.area
        area_km2 = area_degrees * 111 * 106

        perimeter_degrees = geom.length
        perimeter_km = perimeter_degrees * 108.5

        return round(area_km2, 2), round(perimeter_km, 2)
    except:
        return None, None


def load_districts():
    """Carga los distritos desde el archivo GeoJSON"""

    print("\n" + "="*70)
    print("     CARGA DE DISTRITOS DE SANTA CRUZ DE LA SIERRA")
    print("="*70 + "\n")

    # Buscar archivo GeoJSON
    geojson_path = Path(__file__).parent.parent / "santa-cruz-distritos.geojson"

    if not geojson_path.exists():
        logger.error(f"Archivo no encontrado: {geojson_path}")
        logger.info("\nColoca el archivo 'santa-cruz-distritos.geojson' en:")
        logger.info(f"  {geojson_path.parent}")
        return False

    logger.info(f"✓ Archivo encontrado: {geojson_path.name}\n")

    # Cargar GeoJSON
    try:
        with open(geojson_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"Error leyendo archivo GeoJSON: {e}")
        return False

    if data.get('type') != 'FeatureCollection':
        logger.error("El archivo no es un FeatureCollection válido")
        return False

    features = data.get('features', [])
    logger.info(f"✓ {len(features)} features encontrados en el archivo\n")

    # Inicializar base de datos
    logger.info("Inicializando base de datos...")
    try:
        init_db()
        logger.info("✓ Base de datos inicializada\n")
    except Exception as e:
        logger.error(f"Error inicializando base de datos: {e}")
        return False

    # Crear sesión
    db = SessionLocal()

    try:
        # Limpiar tabla existente
        logger.info("Limpiando tabla de distritos...")
        deleted = db.query(District).delete()
        db.commit()
        logger.info(f"✓ {deleted} registros anteriores eliminados\n")

        # Procesar features
        logger.info("Procesando distritos...")
        print("-" * 70)

        count = 0
        errors = 0

        for idx, feature in enumerate(features):
            try:
                properties = feature.get('properties', {})
                geometry = feature.get('geometry')

                if not geometry:
                    logger.warning(f"Feature {idx} sin geometría, omitiendo")
                    errors += 1
                    continue

                # Extraer información del distrito
                # El campo 'distrito' viene como string "001", "002", etc.
                distrito_str = properties.get('distrito', str(idx + 1))
                district_number = int(distrito_str)

                # Usar el nombre de la ciudad como nombre del distrito
                nombreciud = properties.get('nombreciud', 'Santa Cruz de la Sierra')
                district_name = f"Distrito {district_number}"

                # Si hay información de población, la agregamos
                poblacion = properties.get('poblacion', '')
                viviendas = properties.get('viviendas', '')

                # Convertir geometría a Shapely
                geom_shape = shape(geometry)

                # Asegurar que sea MultiPolygon
                if isinstance(geom_shape, Polygon):
                    geom_shape = MultiPolygon([geom_shape])
                elif not isinstance(geom_shape, MultiPolygon):
                    logger.warning(f"Geometría no soportada en feature {idx}: {geom_shape.geom_type}")
                    errors += 1
                    continue

                # Validar que la geometría sea válida
                if not geom_shape.is_valid:
                    logger.warning(f"Geometría inválida en distrito {district_number}, intentando reparar...")
                    geom_shape = geom_shape.buffer(0)

                # Calcular área y perímetro
                area_km2, perimeter_km = calculate_area_perimeter(geom_shape)

                # Crear registro
                district = District(
                    district_number=district_number,
                    district_name=district_name,
                    geometry=from_shape(geom_shape, srid=4326),
                    area_km2=area_km2,
                    perimeter_km=perimeter_km
                )

                db.add(district)
                count += 1

                # Mostrar información
                pop_info = f" | Población: {poblacion}" if poblacion else ""
                logger.info(f"  ✓ Distrito {district_number:2d}: {area_km2:6.2f} km²{pop_info}")

            except Exception as e:
                logger.error(f"  ✗ Error en feature {idx}: {e}")
                errors += 1
                continue

        print("-" * 70)

        # Guardar cambios
        logger.info("\nGuardando en base de datos...")
        db.commit()
        logger.info(f"✓ Cambios guardados correctamente\n")

        # Resumen final
        print("=" * 70)
        print("                         RESUMEN")
        print("=" * 70)

        districts = db.query(District).order_by(District.district_number).all()
        total_area = sum(d.area_km2 for d in districts if d.area_km2)

        logger.info(f"Distritos cargados: {len(districts)}")
        logger.info(f"Errores encontrados: {errors}")
        logger.info(f"Área total: {total_area:.2f} km²\n")

        # Tabla de distritos
        print("Distritos:")
        for d in districts:
            print(f"  • Distrito {d.district_number:2d}: {d.district_name:20s} - {d.area_km2:6.2f} km²")

        print("\n" + "=" * 70)
        print("✓ PROCESO COMPLETADO EXITOSAMENTE")
        print("=" * 70 + "\n")

        logger.info("Próximos pasos:")
        logger.info("  1. Verificar datos: python scripts/verify_districts.py")
        logger.info("  2. Ejecutar tests: python tests/test_districts.py")
        logger.info("  3. Iniciar API: uvicorn app.main:app --reload\n")

        return True

    except Exception as e:
        logger.error(f"\n✗ Error durante la carga: {e}")
        db.rollback()
        import traceback
        traceback.print_exc()
        return False
    finally:
        db.close()


if __name__ == "__main__":
    success = load_districts()
    sys.exit(0 if success else 1)

