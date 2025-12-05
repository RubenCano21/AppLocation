"""
Servicio para asignar ubicación geográfica (distrito y provincia) a puntos
"""
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.models.db_models import District, Province
import logging

logger = logging.getLogger(__name__)


def get_district_and_province_for_point(db: Session, latitude: float, longitude: float) -> dict:
    """
    Encuentra el distrito y provincia que contiene un punto específico

    Args:
        db: Sesión de base de datos
        latitude: Latitud del punto
        longitude: Longitud del punto

    Returns:
        dict con district_id, district_name, province_id, province_name
    """
    try:
        point_wkt = f'POINT({longitude} {latitude})'

        # Buscar distrito
        district = db.execute(text(f"""
            SELECT id, district_number, district_name 
            FROM districts 
            WHERE ST_Contains(geometry, ST_GeomFromText('{point_wkt}', 4326))
            LIMIT 1
        """)).fetchone()

        # Buscar provincia
        province = db.execute(text(f"""
            SELECT id, province_name 
            FROM provinces 
            WHERE ST_Contains(geometry, ST_GeomFromText('{point_wkt}', 4326))
            LIMIT 1
        """)).fetchone()

        result = {
            'district_id': district[0] if district else None,
            'district_name': district[2] if district else None,
            'province_id': province[0] if province else None,
            'province_name': province[1] if province else None,
        }

        return result

    except Exception as e:
        logger.error(f"Error finding location for point ({latitude}, {longitude}): {e}")
        return {
            'district_id': None,
            'district_name': None,
            'province_id': None,
            'province_name': None,
        }


def bulk_assign_geographic_location(db: Session, batch_size: int = 1000):
    """
    Asigna distrito y provincia a todas las ubicaciones que no lo tienen

    Args:
        db: Sesión de base de datos
        batch_size: Tamaño del lote para procesar
    """
    try:
        logger.info("Iniciando asignación masiva de ubicaciones geográficas...")

        # Actualizar usando SQL directo para mejor performance
        sql = text("""
            UPDATE locations l
            SET 
                district_id = d.id,
                district_name = d.district_name,
                province_id = p.id,
                province_name = p.province_name
            FROM districts d, provinces p
            WHERE 
                ST_Contains(d.geometry, l.location_geom)
                AND ST_Contains(p.geometry, l.location_geom)
                AND (l.district_id IS NULL OR l.province_id IS NULL)
        """)

        result = db.execute(sql)
        db.commit()

        rows_updated = result.rowcount
        logger.info(f"✓ {rows_updated:,} ubicaciones actualizadas con distrito y provincia")

        return rows_updated

    except Exception as e:
        logger.error(f"Error en asignación masiva: {e}")
        db.rollback()
        raise

