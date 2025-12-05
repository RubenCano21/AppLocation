# app/models/db_models.py
from sqlalchemy import Column, Integer, Float, String, DateTime, JSON, Index, BigInteger, ForeignKey
from sqlalchemy.orm import relationship
from geoalchemy2 import Geometry
from app.database.postgres_db import Base
from datetime import datetime
from app.models.etl_control import ETLControl


# ==================== TABLAS DE DIMENSIÓN ====================

class DimTime(Base):
    """Dimensión de Tiempo"""
    __tablename__ = "dim_time"

    id = Column(Integer, primary_key=True, index=True)
    period = Column(String(20), nullable=False)  # MAÑANA, TARDE, NOCHE

    __table_args__ = (

        Index('idx_dim_time_period', 'period'),
    )


class DimAltitude(Base):
    """Dimensión de Altitud"""
    __tablename__ = "dim_altitude"

    id = Column(Integer, primary_key=True, index=True)
    altitude_range = Column(String(20), nullable=False)  # BAJA, MEDIA, ALTA

    __table_args__ = (
        Index('idx_dim_altitude_range', 'altitude_range'),
    )


class DimBattery(Base):
    """Dimensión de Batería"""
    __tablename__ = "dim_battery"

    id = Column(Integer, primary_key=True, index=True)
    battery_level = Column(String(20), nullable=False)  # CRITICO, BAJO, MEDIO, ALTO

    __table_args__ = (
        Index('idx_dim_battery_level', 'battery_level'),
    )


class DimNetwork(Base):
    """Dimensión de Tipo de Red"""
    __tablename__ = "dim_network"

    id = Column(Integer, primary_key=True, index=True)
    network_normalized = Column(String(20))  # 2G, 3G, 4G, 5G

    __table_args__ = (
        Index('idx_dim_network_normalized', 'network_normalized'),
    )


class DimOperator(Base):
    """Dimensión de Operador"""
    __tablename__ = "dim_operator"

    id = Column(Integer, primary_key=True, index=True)
    operator_name = Column(String(100), unique=True, nullable=False)

    __table_args__ = (
        Index('idx_dim_operator_name', 'operator_name'),
    )


class DimDevice(Base):
    """Dimensión de Dispositivo"""
    __tablename__ = "dim_device"

    id = Column(Integer, primary_key=True, index=True)
    device_id = Column(String(100), unique=True, nullable=False)
    device_name = Column(String(100))

    __table_args__ = (
        Index('idx_dim_device_id', 'device_id'),
    )


# ==================== TABLA DE HECHOS ====================

class FactLocation(Base):
    """Tabla de hechos con referencias a dimensiones"""
    __tablename__ = "fact_locations"

    id = Column(BigInteger, primary_key=True)

    # Foreign Keys a dimensiones
    time_id = Column(Integer, ForeignKey('dim_time.id'), nullable=False)
    altitude_id = Column(Integer, ForeignKey('dim_altitude.id'))
    battery_id = Column(Integer, ForeignKey('dim_battery.id'))
    network_id = Column(Integer, ForeignKey('dim_network.id'))
    operator_id = Column(Integer, ForeignKey('dim_operator.id'))
    device_id = Column(Integer, ForeignKey('dim_device.id'), nullable=False)

    # Datos de medición (hechos)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    altitude_value = Column(Float)
    speed = Column(Float)
    battery_value = Column(Float)
    signal = Column(Float)

    # Geometría
    location = Column(Geometry('POINT', srid=4326))

    # Timestamps
    timestamp = Column(DateTime, nullable=False, index=True)

    # Relaciones
    time = relationship("DimTime")
    altitude_dim = relationship("DimAltitude")
    battery_dim = relationship("DimBattery")
    network = relationship("DimNetwork")
    operator = relationship("DimOperator")
    device = relationship("DimDevice")

    __table_args__ = (
        Index('idx_fact_location_gist', 'location', postgresql_using='gist'),
        Index('idx_fact_timestamp', 'timestamp'),
        Index('idx_fact_time_id', 'time_id'),
        Index('idx_fact_device_id', 'device_id'),
    )


class Location(Base):
    """
    Tabla única optimizada con todos los datos transformados
    """
    __tablename__ = "locations"

    # ID original de Supabase
    id = Column(BigInteger, primary_key=True)

    # Información del dispositivo
    device_name = Column(String(100))
    device_id = Column(String(100), index=True, nullable=False)

    # ============ COORDENADAS ORIGINALES (sin modificar) ============
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)

    # ============ VALORES NUMÉRICOS ORIGINALES ============
    altitude = Column(Float)  # metros sobre el nivel del mar
    speed = Column(Float)  # m/s
    battery = Column(Float)  # porcentaje 0-100
    signal = Column(Float)  # dBm

    # ============ DATOS DE RED ============
    network_type = Column(String(50))  # Tipo original (ej: "LTE", "5G NR")
    network_generation = Column(String(20))  # Clasificado: 2G, 3G, 4G, 5G, UNKNOWN
    sim_operator = Column(String(100))

    # ============ CLASIFICACIONES TRANSFORMADAS ============
    period = Column(String(20), index=True)  # MAÑANA, TARDE, NOCHE
    altitude_range = Column(String(20))  # BAJA, MEDIA, ALTA
    battery_level = Column(String(20), index=True)  # CRITICO, BAJO, MEDIO, ALTO
    signal_quality = Column(String(20))  # POBRE, REGULAR, BUENA, EXCELENTE
    speed_range = Column(String(30))  # DETENIDO, CAMINANDO, CORRIENDO, TRANSPORTE PÚBLICO, VEHÍCULO

    # ============ DATOS TEMPORALES ============
    timestamp = Column(DateTime, nullable=False, index=True)  # Fecha y hora completa
    date = Column(DateTime, index=True)  # Solo fecha

    # ============ GEOMETRÍA POSTGIS ============
    location_geom = Column(Geometry('POINT', srid=4326))

    # ============ UBICACIÓN GEOGRÁFICA ============
    district_id = Column(Integer, ForeignKey('districts.id'), nullable=True)
    district_name = Column(String(100), index=True)  # Nombre del distrito
    province_id = Column(Integer, ForeignKey('provinces.id'), nullable=True)
    province_name = Column(String(200), index=True)  # Nombre de la provincia

    # ============ GRILLA PARA HEATMAP ============
    lat_grid = Column(Float)
    lon_grid = Column(Float)

    # ============ METADATA ============
    processed_at = Column(DateTime, default=datetime.utcnow)

    # Índices para optimizar consultas
    __table_args__ = (
        Index('idx_location_geom_gist', 'location_geom', postgresql_using='gist'),
        Index('idx_timestamp', 'timestamp'),
        Index('idx_date', 'date'),
        Index('idx_device_id', 'device_id'),
        Index('idx_period', 'period'),
        Index('idx_battery_level', 'battery_level'),
        Index('idx_grid', 'lat_grid', 'lon_grid'),
        Index('idx_district_name', 'district_name'),
        Index('idx_province_name', 'province_name'),
    )


class GridAnalysis(Base):
    """
    Tabla agregada para heatmap (análisis por grilla)
    """
    __tablename__ = "grid_analysis"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Coordenadas de la celda
    lat_grid = Column(Float, nullable=False)
    lon_grid = Column(Float, nullable=False)
    grid_size = Column(Float, default=0.01)

    # Geometría de la celda
    cell_geom = Column(Geometry('POLYGON', srid=4326))

    # Estadísticas agregadas
    point_count = Column(Integer, default=0)
    unique_devices = Column(Integer, default=0)
    avg_battery = Column(Float)
    avg_signal = Column(Float)
    avg_altitude = Column(Float)
    avg_speed = Column(Float)

    # Timestamp
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index('idx_grid_unique', 'lat_grid', 'lon_grid', unique=True),
        Index('idx_grid_cell_gist', 'cell_geom', postgresql_using='gist'),
    )


class District(Base):
    """
    Tabla de distritos de Santa Cruz de la Sierra
    """
    __tablename__ = "districts"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Información del distrito
    district_number = Column(Integer, nullable=False, unique=True)  # Número del distrito (1-14)
    district_name = Column(String(100), nullable=False)  # Nombre del distrito

    # Geometría del polígono
    geometry = Column(Geometry('MULTIPOLYGON', srid=4326), nullable=False)

    # Área y perímetro (calculados)
    area_km2 = Column(Float)  # Área en kilómetros cuadrados
    perimeter_km = Column(Float)  # Perímetro en kilómetros

    # Metadatos
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index('idx_district_geometry', 'geometry', postgresql_using='gist'),
        Index('idx_district_number', 'district_number'),
    )


class Province(Base):
    """
    Tabla de provincias del departamento de Santa Cruz
    """
    __tablename__ = "provinces"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Información de la provincia
    province_name = Column(String(200), nullable=False)
    municipality = Column(String(200))
    department = Column(String(100))

    # Geometría del polígono
    geometry = Column(Geometry('MULTIPOLYGON', srid=4326), nullable=False)

    # Área y perímetro
    area_km2 = Column(Float)
    perimeter_km = Column(Float)

    # Metadatos
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index('idx_province_geometry', 'geometry', postgresql_using='gist'),
        Index('idx_province_name', 'province_name'),
    )

