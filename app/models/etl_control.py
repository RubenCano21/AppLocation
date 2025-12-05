"""
Modelo para control de ejecuciones ETL
"""
from sqlalchemy import Column, Integer, String, DateTime, BigInteger
from app.database.postgres_db import Base
from datetime import datetime


class ETLControl(Base):
    """Tabla de control de ejecuciones ETL"""
    __tablename__ = "etl_control"

    id = Column(Integer, primary_key=True, autoincrement=True)
    execution_date = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_processed_id = Column(BigInteger, nullable=False)  # Último ID procesado de Supabase
    records_processed = Column(Integer, nullable=False)
    status = Column(String(20), nullable=False)  # SUCCESS, FAILED, RUNNING
    execution_time_seconds = Column(Integer)
    error_message = Column(String(500))

