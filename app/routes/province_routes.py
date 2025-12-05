"""
Rutas para provincias
"""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database.postgres_db import get_db
from app.services.province_service import ProvinceService

router = APIRouter(prefix="/provinces", tags=["Provinces"])


@router.get("/")
async def get_all_provinces(db: Session = Depends(get_db)):
    provinces = ProvinceService.get_all_provinces(db)
    return [
        {
            "id": p.id,
            "province_name": p.province_name,
            "municipality": p.municipality,
            "area_km2": p.area_km2,
        }
        for p in provinces
    ]


@router.get("/{province_id}")
async def get_province(province_id: int, db: Session = Depends(get_db)):
    province = ProvinceService.get_province_by_id(db, province_id)
    if not province:
        return {"error": "Province not found"}
    return {
        "id": province.id,
        "province_name": province.province_name,
        "municipality": province.municipality,
        "department": province.department,
        "area_km2": province.area_km2,
    }

