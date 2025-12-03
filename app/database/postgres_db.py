# app/database/postgres_db.py
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from app.config import settings
import logging

logger = logging.getLogger(__name__)

engine = create_engine(
    settings.postgres_url,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Inicializa la base de datos y habilita PostGIS"""
    try:
        with engine.connect() as conn:
            # Habilitar extensión PostGIS
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
            conn.commit()
            logger.info("✓ PostGIS extension enabled")

            # Verificar versión
            result = conn.execute(text("SELECT PostGIS_Version();"))
            version = result.fetchone()[0]
            logger.info(f"✓ PostGIS version: {version}")

        # Crear todas las tablas
        Base.metadata.create_all(bind=engine)
        logger.info("✓ Database tables created successfully")

    except Exception as e:
        logger.error(f"✗ Error initializing database: {e}")
        raise


def test_connection():
    """Prueba la conexión a PostgreSQL"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            version = result.fetchone()[0]
            logger.info(f"✓ PostgreSQL connected: {version[:50]}...")

            # Verificar PostGIS
            try:
                result = conn.execute(text("SELECT PostGIS_Version();"))
                postgis_version = result.fetchone()[0]
                logger.info(f"✓ PostGIS version: {postgis_version}")
            except:
                logger.warning("⚠ PostGIS not installed")

            return True
    except Exception as e:
        logger.error(f"✗ Database connection failed: {e}")
        return False