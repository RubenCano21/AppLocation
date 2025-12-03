# app/models/db_models.py
from sqlalchemy import Column, Integer, Float, String, DateTime, JSON, Index, BigInteger
from geoalchemy2 import Geometry
from app.database.postgres_db import Base
from datetime import datetime


class Location(Base):
    """Tabla principal de ubicaciones transformadas desde Supabase"""
    __tablename__ = "locations"

    # ID original de Supabase
    id = Column(BigInteger, primary_key=True)

    # Información del dispositivo
    device_name = Column(String(100))
    device_id = Column(String(100), index=True)

    # Coordenadas originales
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    altitude = Column(Float)
    speed = Column(Float)

    # Geometría PostGIS
    location = Column(Geometry('POINT', srid=4326))

    # Información de red y dispositivo
    battery = Column(Float)
    signal = Column(Float)
    sim_operator = Column(String(100))
    network_type = Column(String(50))

    # Campos transformados
    network_type_normalized = Column(String(20))  # 2G, 3G, 4G, 5G, unknown
    battery_status = Column(String(20))  # critical, low, medium, high
    signal_quality = Column(String(20))  # poor, fair, good, excellent

    # Timestamp original
    timestamp = Column(DateTime, index=True)

    # Timestamp de procesamiento
    processed_at = Column(DateTime, default=datetime.utcnow)

    # Grilla para análisis espacial
    lat_grid = Column(Float)
    lon_grid = Column(Float)

    # Índices espaciales y de búsqueda
    __table_args__ = (
        Index('idx_location_gist', 'location', postgresql_using='gist'),
        Index('idx_timestamp', 'timestamp'),
        Index('idx_device_id', 'device_id'),
        Index('idx_network_type', 'network_type_normalized'),
        Index('idx_battery_status', 'battery_status'),
        Index('idx_grid', 'lat_grid', 'lon_grid'),
    )


class DistrictAnalysis(Base):
    """Análisis agregado por distrito/barrio"""
    __tablename__ = "district_analysis"

    id = Column(Integer, primary_key=True, index=True)
    district_name = Column(String(100), nullable=False, unique=True)
    district_code = Column(String(50))

    # Geometría del distrito
    polygon = Column(Geometry('POLYGON', srid=4326))
    area_km2 = Column(Float)

    # Estadísticas de puntos
    point_count = Column(Integer, default=0)
    density = Column(Float)  # puntos por km²
    unique_devices = Column(Integer, default=0)

    # Promedios
    avg_battery = Column(Float)
    avg_signal = Column(Float)
    avg_altitude = Column(Float)
    avg_speed = Column(Float)

    # Distribuciones (JSON)
    network_distribution = Column(JSON)  # {"4G": 120, "5G": 80, "3G": 10}
    operator_distribution = Column(JSON)  # {"Entel": 150, "Tigo": 100}
    battery_distribution = Column(JSON)  # {"high": 80, "medium": 100, ...}

    # Timestamps
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index('idx_district_polygon_gist', 'polygon', postgresql_using='gist'),
    )


class GridAnalysis(Base):
    """Análisis por grilla (heatmap)"""
    __tablename__ = "grid_analysis"

    id = Column(Integer, primary_key=True, index=True)

    # Coordenadas de la celda de la grilla
    lat_grid = Column(Float, nullable=False)
    lon_grid = Column(Float, nullable=False)
    grid_size = Column(Float, default=0.01)  # ~1km

    # Geometría de la celda
    cell_polygon = Column(Geometry('POLYGON', srid=4326))

    # Estadísticas agregadas
    point_count = Column(Integer, default=0)
    unique_devices = Column(Integer, default=0)

    # Promedios
    avg_battery = Column(Float)
    avg_signal = Column(Float)
    avg_altitude = Column(Float)
    avg_speed = Column(Float)

    # Distribuciones
    network_distribution = Column(JSON)
    operator_distribution = Column(JSON)

    # Timestamp
    last_updated = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index('idx_grid_unique', 'lat_grid', 'lon_grid', unique=True),
        Index('idx_grid_cell_gist', 'cell_polygon', postgresql_using='gist'),
    )


class DeviceStatistics(Base):
    """Estadísticas por dispositivo"""
    __tablename__ = "device_statistics"

    id = Column(Integer, primary_key=True, index=True)
    device_id = Column(String(100), unique=True, nullable=False)
    device_name = Column(String(100))

    # Contadores
    total_records = Column(Integer, default=0)
    first_seen = Column(DateTime)
    last_seen = Column(DateTime)

    # Promedios
    avg_battery = Column(Float)
    avg_signal = Column(Float)
    avg_speed = Column(Float)

    # Ubicación más común
    most_common_lat = Column(Float)
    most_common_lon = Column(Float)

    # Distribuciones
    network_distribution = Column(JSON)
    operator_distribution = Column(JSON)

    # Timestamps
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index('idx_device_id', 'device_id'),
        Index('idx_last_seen', 'last_seen'),
    )