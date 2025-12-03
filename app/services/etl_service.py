# app/services/etl_service.py
from pyspark.sql import SparkSession
from app.config import settings
from app.database.supabase_utils import get_supabase_client
from app.spark.transformations import (
    transform_locations,
    aggregate_by_grid,
    aggregate_by_device,
    calculate_statistics
)
import logging
import time
from typing import Optional, Dict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ETLService:
    def __init__(self):
        self.supabase = get_supabase_client()
        self.spark = self._init_spark()

    def _init_spark(self) -> SparkSession:
        """Inicializa sesión de Spark con configuración PostgreSQL"""
        try:
            spark = SparkSession.builder \
                .appName(settings.SPARK_APP_NAME) \
                .master(settings.SPARK_MASTER) \
                .config("spark.driver.memory", "2g") \
                .config("spark.executor.memory", "2g") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.driver.extraClassPath", "/path/to/postgresql-42.6.0.jar") \
                .getOrCreate()

            spark.sparkContext.setLogLevel("WARN")
            logger.info("✓ Spark session initialized successfully")
            return spark
        except Exception as e:
            logger.error(f"✗ Error initializing Spark: {e}")
            raise

    async def extract_from_supabase(
            self,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None
    ) -> list:
        """
        Extrae datos de Supabase con paginación automática
        """
        try:
            logger.info("=" * 70)
            logger.info("STEP 1: EXTRACTING DATA FROM SUPABASE")
            logger.info("=" * 70)

            # Primero, contar total de registros
            total_count = self.supabase.count_records()
            logger.info(f"Total records in Supabase: {total_count}")

            # Extraer con paginación
            all_data = self.supabase.fetch_all_paginated(
                start_date=start_date,
                end_date=end_date
            )

            logger.info(f"✓ Successfully extracted {len(all_data)} records from Supabase")

            # Mostrar muestra de datos
            if all_data:
                logger.info("Sample record:")
                logger.info(all_data[0])

            return all_data

        except Exception as e:
            logger.error(f"✗ Error extracting from Supabase: {e}")
            raise

    def transform_with_spark(self, data: list) -> tuple:
        """
        Transforma datos usando PySpark
        Retorna: (df_transformed, df_grid, df_devices, statistics)
        """
        try:
            if not data:
                logger.warning("⚠ No data to transform")
                return None, None, None, None

            logger.info("=" * 70)
            logger.info("STEP 2: TRANSFORMING DATA WITH SPARK")
            logger.info("=" * 70)

            # Crear DataFrame de Spark
            logger.info("Creating Spark DataFrame...")
            df = self.spark.createDataFrame(data)
            initial_count = df.count()
            logger.info(f"Initial record count: {initial_count}")

            # Mostrar schema
            logger.info("DataFrame Schema:")
            df.printSchema()

            # Aplicar transformaciones principales
            logger.info("\nApplying main transformations...")
            df_transformed = transform_locations(df)
            final_count = df_transformed.count()
            logger.info(f"✓ Transformed records: {final_count}")

            if final_count < initial_count:
                logger.warning(f"⚠ Filtered out {initial_count - final_count} invalid records")

            # Mostrar muestra de datos transformados
            logger.info("\nSample transformed data:")
            df_transformed.select(
                "id", "device_id", "latitude", "longitude",
                "network_type", "network_type_normalized",
                "battery", "battery_status"
            ).show(5, truncate=False)

            # Agregación por grilla
            logger.info("\nAggregating by grid (0.01° ~ 1km)...")
            df_grid = aggregate_by_grid(df_transformed, grid_size=0.01)
            logger.info(f"✓ Grid cells created: {df_grid.count()}")

            # Mostrar muestra de grilla
            logger.info("\nSample grid data:")
            df_grid.select(
                "lat_grid", "lon_grid", "point_count",
                "unique_devices", "avg_battery"
            ).show(5, truncate=False)

            # Agregación por dispositivo
            logger.info("\nAggregating by device...")
            df_devices = aggregate_by_device(df_transformed)
            logger.info(f"✓ Unique devices: {df_devices.count()}")

            # Mostrar muestra de dispositivos
            logger.info("\nSample device statistics:")
            df_devices.show(5, truncate=False)

            # Calcular estadísticas generales
            logger.info("\nCalculating general statistics...")
            statistics = calculate_statistics(df_transformed)

            logger.info("\n" + "=" * 70)
            logger.info("GENERAL STATISTICS")
            logger.info("=" * 70)
            logger.info(f"Total points: {statistics['total_points']}")
            logger.info(f"Unique devices: {statistics['unique_devices']}")
            logger.info(f"Average battery: {statistics['avg_battery']}%")
            logger.info(f"Average signal: {statistics['avg_signal']} dBm")
            logger.info(f"Average speed: {statistics['avg_speed']} m/s")
            logger.info(f"Date range: {statistics['date_range']['start']} to {statistics['date_range']['end']}")

            logger.info("\nNetwork distribution:")
            for net in statistics['network_distribution']:
                logger.info(f"  - {net['network']}: {net['count']}")

            logger.info("\nOperator distribution:")
            for op in statistics['operator_distribution']:
                logger.info(f"  - {op['operator']}: {op['count']}")

            return df_transformed, df_grid, df_devices, statistics

        except Exception as e:
            logger.error(f"✗ Error transforming data: {e}")
            raise

    def load_to_postgres(self, df, table_name: str, mode: str = "append"):
        """
        Carga DataFrame de Spark a PostgreSQL
        """
        try:
            logger.info(f"\nLoading to PostgreSQL table '{table_name}'...")

            # Contar registros antes de cargar
            record_count = df.count()
            logger.info(f"Records to load: {record_count}")

            # Cargar a PostgreSQL
            df.write \
                .format("jdbc") \
                .option("url", settings.postgres_jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", settings.DEST_PG_USER) \
                .option("password", settings.DEST_PG_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode(mode) \
                .save()

            logger.info(f"✓ Data loaded to '{table_name}' successfully (mode: {mode})")
            return record_count

        except Exception as e:
            logger.error(f"✗ Error loading to PostgreSQL table '{table_name}': {e}")
            raise

    async def run_full_etl(
            self,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
            force_refresh: bool = False
    ) -> Dict:
        """
        Ejecuta el pipeline ETL completo: Extract -> Transform -> Load
        """
        start_time = time.time()

        try:
            logger.info("\n" + "=" * 70)
            logger.info("STARTING ETL PIPELINE")
            logger.info("=" * 70)
            logger.info(f"Start date filter: {start_date or 'None'}")
            logger.info(f"End date filter: {end_date or 'None'}")
            logger.info(f"Force refresh: {force_refresh}")
            logger.info("=" * 70 + "\n")

            # 1. EXTRACT
            raw_data = await self.extract_from_supabase(start_date, end_date)

            if not raw_data:
                return {
                    "status": "warning",
                    "message": "No data found in Supabase",
                    "records_processed": 0,
                    "records_inserted": 0,
                    "execution_time": 0
                }

            # 2. TRANSFORM
            df_transformed, df_grid, df_devices, statistics = self.transform_with_spark(raw_data)

            if df_transformed is None:
                raise Exception("Transformation returned None")

            # 3. LOAD
            logger.info("\n" + "=" * 70)
            logger.info("STEP 3: LOADING TO POSTGRESQL")
            logger.info("=" * 70)

            mode = "overwrite" if force_refresh else "append"

            # Cargar tabla principal de ubicaciones
            records_locations = self.load_to_postgres(
                df_transformed.select(
                    "id", "device_name", "device_id",
                    "latitude", "longitude", "altitude", "speed",
                    "battery", "signal", "sim_operator", "network_type",
                    "network_type_normalized", "battery_status", "signal_quality",
                    "location", "timestamp", "lat_grid", "lon_grid"
                ),
                "locations",
                mode=mode
            )

            # Cargar análisis de grilla (siempre overwrite)
            records_grid = self.load_to_postgres(
                df_grid,
                "grid_analysis",
                mode="overwrite"
            )

            # Cargar estadísticas de dispositivos (siempre overwrite)
            records_devices = self.load_to_postgres(
                df_devices,
                "device_statistics",
                mode="overwrite"
            )

            execution_time = time.time() - start_time

            logger.info("\n" + "=" * 70)
            logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 70)
            logger.info(f"Total execution time: {execution_time:.2f} seconds")
            logger.info(f"Locations loaded: {records_locations}")
            logger.info(f"Grid cells loaded: {records_grid}")
            logger.info(f"Device statistics loaded: {records_devices}")
            logger.info("=" * 70 + "\n")

            return {
                "status": "success",
                "records_processed": len(raw_data),
                "records_inserted": records_locations,
                "grid_cells": records_grid,
                "unique_devices": records_devices,
                "execution_time": round(execution_time, 2),
                "statistics": statistics,
                "message": "ETL pipeline completed successfully"
            }

        except Exception as e:
            execution_time = time.time() - start_time
            logger.error("\n" + "=" * 70)
            logger.error("ETL PIPELINE FAILED")
            logger.error("=" * 70)
            logger.error(f"Error: {e}")
            logger.error(f"Execution time before failure: {execution_time:.2f} seconds")
            logger.error("=" * 70 + "\n")

            return {
                "status": "error",
                "records_processed": 0,
                "records_inserted": 0,
                "execution_time": round(execution_time, 2),
                "message": str(e)
            }

    def cleanup(self):
        """Limpia recursos de Spark"""
        if self.spark:
            self.spark.stop()
            logger.info("✓ Spark session stopped")