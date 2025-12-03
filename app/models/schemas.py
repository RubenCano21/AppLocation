# app/models/schemas.py
from pydantic import BaseModel, Field, validator
from typing import Optional, Dict, List
from datetime import datetime


class LocationBase(BaseModel):
    """Schema base para Location"""
    device_name: Optional[str] = None
    device_id: Optional[str] = None
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    altitude: Optional[float] = None
    speed: Optional[float] = Field(None, ge=0)
    battery: Optional[float] = Field(None, ge=0, le=100)
    signal: Optional[float] = None
    sim_operator: Optional[str] = None
    network_type: Optional[str] = None
    timestamp: Optional[datetime] = None


class LocationCreate(LocationBase):
    """Schema para crear Location"""
    pass


class LocationResponse(LocationBase):
    """Schema para respuesta de Location"""
    id: int
    network_type_normalized: Optional[str]
    battery_status: Optional[str]
    signal_quality: Optional[str]
    processed_at: datetime

    class Config:
        from_attributes = True


class LocationListResponse(BaseModel):
    """Schema para lista de locations"""
    total: int
    locations: List[LocationResponse]
    page: int
    page_size: int


class DistrictStatsResponse(BaseModel):
    """Schema para estadísticas por distrito"""
    district_name: str
    district_code: Optional[str]
    point_count: int
    unique_devices: int
    density: float
    avg_battery: Optional[float]
    avg_signal: Optional[float]
    avg_altitude: Optional[float]
    avg_speed: Optional[float]
    network_distribution: Dict[str, int]
    operator_distribution: Dict[str, int]
    area_km2: Optional[float]
    last_updated: datetime


class GridCellResponse(BaseModel):
    """Schema para celda de grilla"""
    lat_grid: float
    lon_grid: float
    point_count: int
    unique_devices: int
    avg_battery: Optional[float]
    avg_signal: Optional[float]
    network_distribution: Dict[str, int]
    operator_distribution: Dict[str, int]


class DeviceStatsResponse(BaseModel):
    """Schema para estadísticas de dispositivo"""
    device_id: str
    device_name: Optional[str]
    total_records: int
    first_seen: datetime
    last_seen: datetime
    avg_battery: Optional[float]
    avg_signal: Optional[float]
    avg_speed: Optional[float]
    most_common_lat: Optional[float]
    most_common_lon: Optional[float]
    network_distribution: Dict[str, int]
    operator_distribution: Dict[str, int]


class ETLRequest(BaseModel):
    """Schema para request de ETL"""
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    batch_size: int = Field(default=10000, ge=1000, le=100000)
    force_refresh: bool = False


class ETLResponse(BaseModel):
    """Schema para respuesta de ETL"""
    status: str
    records_processed: int
    records_inserted: int
    execution_time: float
    message: Optional[str] = None


class NetworkDistribution(BaseModel):
    """Schema para distribución de red"""
    network_type: str
    count: int
    percentage: float


class OperatorDistribution(BaseModel):
    """Schema para distribución de operadores"""
    operator: str
    count: int
    percentage: float


class GeneralStats(BaseModel):
    """Schema para estadísticas generales"""
    total_points: int
    unique_devices: int
    avg_battery: float
    avg_signal: float
    avg_speed: float
    network_distribution: List[NetworkDistribution]
    operator_distribution: List[OperatorDistribution]
    date_range: Dict[str, str]


class HeatmapRequest(BaseModel):
    """Schema para request de heatmap"""
    grid_size: float = Field(default=0.01, ge=0.001, le=0.1)
    min_lat: Optional[float] = Field(None, ge=-90, le=90)
    max_lat: Optional[float] = Field(None, ge=-90, le=90)
    min_lon: Optional[float] = Field(None, ge=-180, le=180)
    max_lon: Optional[float] = Field(None, ge=-180, le=180)
    network_type: Optional[str] = None
    operator: Optional[str] = None