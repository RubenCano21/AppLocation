# db_utils.py
import os
from dotenv import load_dotenv

load_dotenv()

# Supabase (origen)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "locations")
SUPABASE_FETCH_LIMIT = int(os.getenv("SUPABASE_FETCH_LIMIT", "50000"))

# Destino Postgres
DEST_PG_HOST = os.getenv("DEST_PG_HOST")
DEST_PG_PORT = int(os.getenv("DEST_PG_PORT", "5432"))
DEST_PG_DB = os.getenv("DEST_PG_DB")
DEST_PG_USER = os.getenv("DEST_PG_USER")
DEST_PG_PASSWORD = os.getenv("DEST_PG_PASSWORD")

# FastAPI
FASTAPI_HOST = os.getenv("FASTAPI_HOST", "0.0.0.0")
FASTAPI_PORT = int(os.getenv("FASTAPI_PORT", "8000"))

# Spark
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "SparkBigData")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
