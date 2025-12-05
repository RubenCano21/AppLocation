"""
Test rápido para verificar que el sistema de distritos funciona correctamente
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database.postgres_db import SessionLocal, init_db
from app.models.db_models import District
from app.services.district_service import DistrictService
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def test_database_connection():
    """Test 1: Conexión a la base de datos"""
    logger.info("\n" + "="*60)
    logger.info("TEST 1: Conexión a la base de datos")
    logger.info("="*60)

    try:
        db = SessionLocal()
        # Intentar una consulta simple
        result = db.execute("SELECT 1").fetchone()
        db.close()
        logger.info("✓ Conexión exitosa")
        return True
    except Exception as e:
        logger.error(f"✗ Error de conexión: {e}")
        return False


def test_postgis_extension():
    """Test 2: Verificar extensión PostGIS"""
    logger.info("\n" + "="*60)
    logger.info("TEST 2: Extensión PostGIS")
    logger.info("="*60)

    try:
        db = SessionLocal()
        result = db.execute("SELECT PostGIS_Version()").fetchone()
        version = result[0] if result else "Unknown"
        db.close()
        logger.info(f"✓ PostGIS instalado: {version}")
        return True
    except Exception as e:
        logger.error(f"✗ PostGIS no disponible: {e}")
        return False


def test_table_exists():
    """Test 3: Verificar que la tabla districts existe"""
    logger.info("\n" + "="*60)
    logger.info("TEST 3: Tabla 'districts'")
    logger.info("="*60)

    try:
        db = SessionLocal()
        result = db.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'districts'
            )
        """).fetchone()

        exists = result[0] if result else False
        db.close()

        if exists:
            logger.info("✓ Tabla 'districts' existe")
            return True
        else:
            logger.warning("⚠ Tabla 'districts' no existe")
            logger.info("  Ejecuta: python -c \"from app.database.postgres_db import init_db; init_db()\"")
            return False
    except Exception as e:
        logger.error(f"✗ Error verificando tabla: {e}")
        return False


def test_districts_loaded():
    """Test 4: Verificar que hay distritos cargados"""
    logger.info("\n" + "="*60)
    logger.info("TEST 4: Distritos cargados")
    logger.info("="*60)

    try:
        db = SessionLocal()
        count = db.query(District).count()
        db.close()

        if count > 0:
            logger.info(f"✓ {count} distritos encontrados")
            return True
        else:
            logger.warning("⚠ No hay distritos cargados")
            logger.info("  Ejecuta: python scripts/load_districts_simple.py")
            return False
    except Exception as e:
        logger.error(f"✗ Error consultando distritos: {e}")
        return False


def test_service_methods():
    """Test 5: Verificar métodos del servicio"""
    logger.info("\n" + "="*60)
    logger.info("TEST 5: Métodos del servicio")
    logger.info("="*60)

    try:
        db = SessionLocal()

        # Test 1: get_all_districts
        districts = DistrictService.get_all_districts(db)
        logger.info(f"  • get_all_districts(): {len(districts)} distritos")

        if districts:
            # Test 2: get_district_by_number
            first_district = districts[0]
            district = DistrictService.get_district_by_number(db, first_district.district_number)
            logger.info(f"  • get_district_by_number({first_district.district_number}): {district.district_name if district else 'None'}")

            # Test 3: get_districts_geojson
            geojson = DistrictService.get_districts_geojson(db)
            logger.info(f"  • get_districts_geojson(): {len(geojson.get('features', []))} features")

            # Test 4: get_district_by_point (Plaza 24 de Septiembre)
            district = DistrictService.get_district_by_point(db, -17.7833, -63.1821)
            logger.info(f"  • get_district_by_point(-17.7833, -63.1821): {district.district_name if district else 'None'}")

        db.close()
        logger.info("✓ Todos los métodos funcionan correctamente")
        return True

    except Exception as e:
        logger.error(f"✗ Error en métodos del servicio: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_spatial_queries():
    """Test 6: Verificar consultas espaciales"""
    logger.info("\n" + "="*60)
    logger.info("TEST 6: Consultas espaciales")
    logger.info("="*60)

    try:
        db = SessionLocal()

        # Probar consulta espacial básica
        result = db.execute("""
            SELECT district_number, district_name, 
                   ST_Area(geometry) as area_degrees,
                   ST_AsText(ST_Centroid(geometry)) as centroid
            FROM districts 
            LIMIT 1
        """).fetchone()

        if result:
            logger.info(f"  • Distrito {result[0]}: {result[1]}")
            logger.info(f"  • Área: {result[2]} grados²")
            logger.info(f"  • Centroide: {result[3]}")
            logger.info("✓ Consultas espaciales funcionan")
            db.close()
            return True
        else:
            logger.warning("⚠ No se pudieron ejecutar consultas espaciales")
            db.close()
            return False

    except Exception as e:
        logger.error(f"✗ Error en consultas espaciales: {e}")
        return False


def run_all_tests():
    """Ejecuta todos los tests"""
    logger.info("\n" + "#"*60)
    logger.info("SISTEMA DE DISTRITOS - TESTS DE VERIFICACIÓN")
    logger.info("#"*60)

    results = []

    # Ejecutar tests
    results.append(("Conexión DB", test_database_connection()))
    results.append(("PostGIS", test_postgis_extension()))
    results.append(("Tabla districts", test_table_exists()))
    results.append(("Datos cargados", test_districts_loaded()))
    results.append(("Servicios", test_service_methods()))
    results.append(("Consultas espaciales", test_spatial_queries()))

    # Resumen
    logger.info("\n" + "="*60)
    logger.info("RESUMEN DE TESTS")
    logger.info("="*60)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        logger.info(f"  {status}: {name}")

    logger.info("")
    logger.info(f"Total: {passed}/{total} tests pasaron")

    if passed == total:
        logger.info("\n✓ ¡Todos los tests pasaron! El sistema está listo para usar.")
    else:
        logger.info("\n⚠ Algunos tests fallaron. Revisa los errores arriba.")

    logger.info("="*60)

    return passed == total


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)

