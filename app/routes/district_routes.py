"""
Rutas para gestión de distritos
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from app.database.postgres_db import get_db
from app.services.district_service import DistrictService
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/districts", tags=["Districts"])


@router.get("/", response_model=List[Dict])
async def get_all_districts(db: Session = Depends(get_db)):
    """
    Obtiene todos los distritos
    """
    try:
        districts = DistrictService.get_all_districts(db)
        return [
            {
                "id": d.id,
                "district_number": d.district_number,
                "district_name": d.district_name,
                "area_km2": d.area_km2,
                "perimeter_km": d.perimeter_km,
                "created_at": d.created_at.isoformat() if d.created_at else None,
            }
            for d in districts
        ]
    except Exception as e:
        logger.error(f"Error getting districts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/geojson")
async def get_districts_geojson(db: Session = Depends(get_db)):
    """
    Obtiene todos los distritos en formato GeoJSON
    """
    try:
        return DistrictService.get_districts_geojson(db)
    except Exception as e:
        logger.error(f"Error getting districts GeoJSON: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{district_number}")
async def get_district_by_number(
    district_number: int,
    db: Session = Depends(get_db)
):
    """
    Obtiene un distrito por su número
    """
    try:
        district = DistrictService.get_district_by_number(db, district_number)
        if not district:
            raise HTTPException(status_code=404, detail=f"District {district_number} not found")

        return {
            "id": district.id,
            "district_number": district.district_number,
            "district_name": district.district_name,
            "area_km2": district.area_km2,
            "perimeter_km": district.perimeter_km,
            "created_at": district.created_at.isoformat() if district.created_at else None,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting district {district_number}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{district_number}/statistics")
async def get_district_statistics(
    district_number: int,
    db: Session = Depends(get_db)
):
    """
    Obtiene estadísticas de un distrito específico
    """
    try:
        stats = DistrictService.get_district_statistics(db, district_number)
        if not stats:
            raise HTTPException(status_code=404, detail=f"District {district_number} not found")

        return stats
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting district statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics/all")
async def get_all_districts_statistics(db: Session = Depends(get_db)):
    """
    Obtiene estadísticas de todos los distritos
    """
    try:
        return DistrictService.get_all_districts_statistics(db)
    except Exception as e:
        logger.error(f"Error getting all districts statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/point/find")
async def find_district_by_point(
    lat: float = Query(..., description="Latitud"),
    lon: float = Query(..., description="Longitud"),
    db: Session = Depends(get_db)
):
    """
    Encuentra el distrito que contiene un punto específico
    """
    try:
        district = DistrictService.get_district_by_point(db, lat, lon)
        if not district:
            return {"message": "No district found for this point"}

        return {
            "id": district.id,
            "district_number": district.district_number,
            "district_name": district.district_name,
            "area_km2": district.area_km2,
        }
    except Exception as e:
        logger.error(f"Error finding district by point: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{district_number}/locations")
async def get_locations_in_district(
    district_number: int,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    """
    Obtiene las ubicaciones dentro de un distrito
    """
    try:
        locations = DistrictService.get_locations_in_district(
            db, district_number, limit, offset
        )

        return {
            "district_number": district_number,
            "count": len(locations),
            "limit": limit,
            "offset": offset,
            "locations": [
                {
                    "id": loc.id,
                    "latitude": loc.latitude,
                    "longitude": loc.longitude,
                    "device_id": loc.device_id,
                    "timestamp": loc.timestamp.isoformat() if loc.timestamp else None,
                    "signal": loc.signal,
                    "battery": loc.battery,
                    "network_generation": loc.network_generation,
                }
                for loc in locations
            ]
        }
    except Exception as e:
        logger.error(f"Error getting locations in district: {e}")
        raise HTTPException(status_code=500, detail=str(e))

