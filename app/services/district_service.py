"""
Servicio para gestión de distritos
"""
from sqlalchemy.orm import Session
from sqlalchemy import text, func
from app.models.db_models import District, Location
from geoalchemy2.functions import ST_AsGeoJSON, ST_Contains, ST_Intersects, ST_Distance
from typing import List, Dict, Optional
import json
import logging

logger = logging.getLogger(__name__)


class DistrictService:
    """Servicio para operaciones con distritos"""

    @staticmethod
    def get_all_districts(db: Session) -> List[District]:
        """Obtiene todos los distritos"""
        return db.query(District).order_by(District.district_number).all()

    @staticmethod
    def get_district_by_number(db: Session, district_number: int) -> Optional[District]:
        """Obtiene un distrito por su número"""
        return db.query(District).filter(District.district_number == district_number).first()

    @staticmethod
    def get_district_by_id(db: Session, district_id: int) -> Optional[District]:
        """Obtiene un distrito por su ID"""
        return db.query(District).filter(District.id == district_id).first()

    @staticmethod
    def get_districts_geojson(db: Session) -> Dict:
        """
        Retorna todos los distritos en formato GeoJSON
        """
        try:
            districts = db.query(
                District.id,
                District.district_number,
                District.district_name,
                District.area_km2,
                District.perimeter_km,
                ST_AsGeoJSON(District.geometry).label('geometry')
            ).all()

            features = []
            for district in districts:
                feature = {
                    "type": "Feature",
                    "properties": {
                        "id": district.id,
                        "district_number": district.district_number,
                        "district_name": district.district_name,
                        "area_km2": district.area_km2,
                        "perimeter_km": district.perimeter_km
                    },
                    "geometry": json.loads(district.geometry)
                }
                features.append(feature)

            geojson = {
                "type": "FeatureCollection",
                "features": features
            }

            return geojson

        except Exception as e:
            logger.error(f"Error getting districts GeoJSON: {e}")
            raise

    @staticmethod
    def get_district_by_point(db: Session, latitude: float, longitude: float) -> Optional[District]:
        """
        Encuentra el distrito que contiene un punto específico

        Args:
            db: Sesión de base de datos
            latitude: Latitud del punto
            longitude: Longitud del punto

        Returns:
            Distrito que contiene el punto o None
        """
        try:
            point_wkt = f'POINT({longitude} {latitude})'

            district = db.query(District).filter(
                ST_Contains(District.geometry, text(f"ST_GeomFromText('{point_wkt}', 4326)"))
            ).first()

            return district

        except Exception as e:
            logger.error(f"Error finding district by point: {e}")
            return None

    @staticmethod
    def count_locations_by_district(db: Session, district_number: int) -> int:
        """
        Cuenta las ubicaciones dentro de un distrito específico

        Args:
            db: Sesión de base de datos
            district_number: Número del distrito

        Returns:
            Cantidad de ubicaciones en el distrito
        """
        try:
            district = DistrictService.get_district_by_number(db, district_number)
            if not district:
                return 0

            count = db.query(func.count(Location.id)).filter(
                ST_Contains(district.geometry, Location.location_geom)
            ).scalar()

            return count or 0

        except Exception as e:
            logger.error(f"Error counting locations in district: {e}")
            return 0

    @staticmethod
    def get_locations_in_district(
        db: Session,
        district_number: int,
        limit: int = 1000,
        offset: int = 0
    ) -> List[Location]:
        """
        Obtiene las ubicaciones dentro de un distrito

        Args:
            db: Sesión de base de datos
            district_number: Número del distrito
            limit: Límite de resultados
            offset: Offset para paginación

        Returns:
            Lista de ubicaciones en el distrito
        """
        try:
            district = DistrictService.get_district_by_number(db, district_number)
            if not district:
                return []

            locations = db.query(Location).filter(
                ST_Contains(district.geometry, Location.location_geom)
            ).limit(limit).offset(offset).all()

            return locations

        except Exception as e:
            logger.error(f"Error getting locations in district: {e}")
            return []

    @staticmethod
    def get_district_statistics(db: Session, district_number: int) -> Dict:
        """
        Obtiene estadísticas de un distrito

        Args:
            db: Sesión de base de datos
            district_number: Número del distrito

        Returns:
            Diccionario con estadísticas del distrito
        """
        try:
            district = DistrictService.get_district_by_number(db, district_number)
            if not district:
                return {}

            # Consulta de estadísticas
            stats = db.query(
                func.count(Location.id).label('total_locations'),
                func.count(func.distinct(Location.device_id)).label('unique_devices'),
                func.avg(Location.battery).label('avg_battery'),
                func.avg(Location.signal).label('avg_signal'),
                func.avg(Location.speed).label('avg_speed'),
            ).filter(
                ST_Contains(district.geometry, Location.location_geom)
            ).first()

            return {
                "district_number": district.district_number,
                "district_name": district.district_name,
                "area_km2": district.area_km2,
                "perimeter_km": district.perimeter_km,
                "total_locations": stats.total_locations or 0,
                "unique_devices": stats.unique_devices or 0,
                "avg_battery": round(stats.avg_battery, 2) if stats.avg_battery else None,
                "avg_signal": round(stats.avg_signal, 2) if stats.avg_signal else None,
                "avg_speed": round(stats.avg_speed, 2) if stats.avg_speed else None,
            }

        except Exception as e:
            logger.error(f"Error getting district statistics: {e}")
            return {}

    @staticmethod
    def get_all_districts_statistics(db: Session) -> List[Dict]:
        """
        Obtiene estadísticas de todos los distritos

        Returns:
            Lista de diccionarios con estadísticas por distrito
        """
        try:
            districts = DistrictService.get_all_districts(db)
            statistics = []

            for district in districts:
                stats = DistrictService.get_district_statistics(db, district.district_number)
                statistics.append(stats)

            return statistics

        except Exception as e:
            logger.error(f"Error getting all districts statistics: {e}")
            return []

