# app/config.py
from pydantic_settings import BaseSettings
from functools import lru_cache
from pathlib import Path

# Obtener el directorio raíz del proyecto
BASE_DIR = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    # Supabase
    SUPABASE_URL: str
    SUPABASE_SERVICE_KEY: str
    SUPABASE_TABLE: str
    SUPABASE_FETCH_LIMIT: int = 50000

    # PostgreSQL Destino
    DEST_PG_HOST: str
    DEST_PG_PORT: int
    DEST_PG_DB: str
    DEST_PG_USER: str
    DEST_PG_PASSWORD: str

    # FastAPI
    FASTAPI_HOST: str = "0.0.0.0"
    FASTAPI_PORT: int = 8000

    # Spark
    SPARK_APP_NAME: str = "SparkBigData"
    SPARK_MASTER: str = "local[*]"

    # Computed properties
    @property
    def postgres_url(self) -> str:
        return f"postgresql://{self.DEST_PG_USER}:{self.DEST_PG_PASSWORD}@{self.DEST_PG_HOST}:{self.DEST_PG_PORT}/{self.DEST_PG_DB}"

    @property
    def postgres_jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.DEST_PG_HOST}:{self.DEST_PG_PORT}/{self.DEST_PG_DB}"

    class Config:
        env_file = str(BASE_DIR / ".env")
        env_file_encoding = 'utf-8'
        case_sensitive = True
        extra = "allow"  # Permitir campos extra


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()