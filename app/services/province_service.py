"""
Servicio para gestión de provincias
"""
from sqlalchemy.orm import Session
from sqlalchemy import func
from app.models.db_models import Province, Location
from geoalchemy2.functions import ST_AsGeoJSON, ST_Contains
from typing import List, Optional
import json


class ProvinceService:

    @staticmethod
    def get_all_provinces(db: Session) -> List[Province]:
        return db.query(Province).all()

    @staticmethod
    def get_province_by_id(db: Session, province_id: int) -> Optional[Province]:
        return db.query(Province).filter(Province.id == province_id).first()

    @staticmethod
    def get_province_by_point(db: Session, latitude: float, longitude: float) -> Optional[Province]:
        from sqlalchemy import text
        point_wkt = f'POINT({longitude} {latitude})'
        return db.query(Province).filter(
            ST_Contains(Province.geometry, text(f"ST_GeomFromText('{point_wkt}', 4326)"))
        ).first()

    @staticmethod
    def count_locations_by_province(db: Session, province_id: int) -> int:
        province = ProvinceService.get_province_by_id(db, province_id)
        if not province:
            return 0
        count = db.query(func.count(Location.id)).filter(
            ST_Contains(province.geometry, Location.location_geom)
        ).scalar()
        return count or 0

