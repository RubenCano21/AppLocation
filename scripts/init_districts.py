"""
Script de inicialización rápida del sistema de distritos
Crea las tablas y prepara todo para cargar datos
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database.postgres_db import init_db, SessionLocal
from app.models.db_models import District
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def main():
    """Inicializa el sistema de distritos"""
    logger.info("="*60)
    logger.info("INICIALIZACIÓN DEL SISTEMA DE DISTRITOS")
    logger.info("="*60)

    # 1. Inicializar base de datos
    logger.info("\n1. Inicializando base de datos...")
    try:
        init_db()
        logger.info("   ✓ Base de datos inicializada")
    except Exception as e:
        logger.error(f"   ✗ Error: {e}")
        return

    # 2. Verificar tabla districts
    logger.info("\n2. Verificando tabla 'districts'...")
    try:
        db = SessionLocal()
        count = db.query(District).count()
        logger.info(f"   ✓ Tabla existe. Distritos actuales: {count}")
        db.close()
    except Exception as e:
        logger.error(f"   ✗ Error: {e}")
        return

    # 3. Instrucciones
    logger.info("\n" + "="*60)
    logger.info("✓ SISTEMA LISTO")
    logger.info("="*60)

    if count == 0:
        logger.info("\nPróximo paso: Cargar los datos de distritos")
        logger.info("\nEjecuta:")
        logger.info("  python scripts/load_districts_simple.py ruta/al/archivo.geojson")
        logger.info("\nO coloca el archivo 'santa-cruz-distritos.geojson' en la raíz")
        logger.info("del proyecto y ejecuta:")
        logger.info("  python scripts/load_districts_simple.py")
    else:
        logger.info(f"\n✓ Ya tienes {count} distritos cargados")
        logger.info("\nPara verificar los datos:")
        logger.info("  python scripts/verify_districts.py")
        logger.info("\nPara ejecutar tests:")
        logger.info("  python tests/test_districts.py")
        logger.info("\nPara iniciar la API:")
        logger.info("  uvicorn app.main:app --reload")

    logger.info("\n" + "="*60)


if __name__ == "__main__":
    main()

