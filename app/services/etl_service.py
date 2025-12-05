# app/services/etl_service.py
from pyspark.sql import SparkSession
from app.config import settings
from app.database.supabase_utils import get_supabase_client
from app.spark.transformations import transform_locations, aggregate_by_grid, get_statistics
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
        """Inicializa sesión de Spark"""
        try:
            import os
            import sys
            from pathlib import Path

            # Configurar Python para Spark (importante en Windows)
            python_path = sys.executable
            os.environ['PYSPARK_PYTHON'] = python_path
            os.environ['PYSPARK_DRIVER_PYTHON'] = python_path

            # Ruta absoluta del driver PostgreSQL
            project_root = Path(__file__).parent.parent.parent
            jdbc_driver = str(project_root / "postgresql-42.7.0.jar")

            spark = SparkSession.builder \
                .appName(settings.SPARK_APP_NAME) \
                .master(settings.SPARK_MASTER) \
                .config("spark.driver.memory", "2g") \
                .config("spark.executor.memory", "2g") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.jars", jdbc_driver) \
                .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
                .config("spark.python.worker.faulthandler.enabled", "true") \
                .config("spark.python.worker.reuse", "false") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
                .config("spark.sql.shuffle.partitions", "8") \
                .config("spark.default.parallelism", "4") \
                .getOrCreate()

            spark.sparkContext.setLogLevel("WARN")
            logger.info("✓ Spark session initialized")
            logger.info(f"Using Python: {python_path}")
            logger.info(f"JDBC Driver: {jdbc_driver}")
            return spark
        except Exception as e:
            logger.error(f"✗ Error initializing Spark: {e}")
            raise

    async def extract_from_supabase(
            self,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
            last_id: Optional[int] = None
    ) -> list:
        """Extrae datos de Supabase"""
        try:
            logger.info("=" * 70)
            logger.info("EXTRACTING FROM SUPABASE")
            logger.info("=" * 70)

            if last_id:
                logger.info(f"Incremental load from ID > {last_id}")

            all_data = self.supabase.fetch_all_paginated(
                start_date=start_date,
                end_date=end_date,
                last_id=last_id
            )

            logger.info(f"✓ Extracted {len(all_data):,} records")
            return all_data

        except Exception as e:
            logger.error(f"✗ Error extracting: {e}")
            raise

    def transform_with_spark(self, data: list) -> tuple:
        """Transforma datos con Spark"""
        try:
            if not data:
                logger.warning("⚠ No data to transform")
                return None, None, None

            logger.info("=" * 70)
            logger.info("TRANSFORMING WITH SPARK")
            logger.info("=" * 70)

            # Normalizar tipos de datos para evitar conflictos DoubleType/LongType
            logger.info("Normalizing data types...")
            normalized_data = []
            for record in data:
                normalized_record = record.copy()
                # Convertir campos numéricos a tipos consistentes
                if 'latitude' in normalized_record and normalized_record['latitude'] is not None:
                    normalized_record['latitude'] = float(normalized_record['latitude'])
                if 'longitude' in normalized_record and normalized_record['longitude'] is not None:
                    normalized_record['longitude'] = float(normalized_record['longitude'])
                if 'altitude' in normalized_record and normalized_record['altitude'] is not None:
                    normalized_record['altitude'] = float(normalized_record['altitude'])
                if 'speed' in normalized_record and normalized_record['speed'] is not None:
                    normalized_record['speed'] = float(normalized_record['speed'])
                if 'battery' in normalized_record and normalized_record['battery'] is not None:
                    normalized_record['battery'] = int(normalized_record['battery'])
                if 'signal' in normalized_record and normalized_record['signal'] is not None:
                    normalized_record['signal'] = int(normalized_record['signal'])
                if 'id' in normalized_record and normalized_record['id'] is not None:
                    normalized_record['id'] = int(normalized_record['id'])
                normalized_data.append(normalized_record)

            # Crear DataFrame
            df = self.spark.createDataFrame(normalized_data)
            logger.info(f"Input records: {df.count():,}")

            # Transformar
            df_transformed = transform_locations(df)

            # Mostrar muestra
            logger.info("\nSample transformed data:")
            df_transformed.select(
                "id", "latitude", "longitude", "period",
                "altitude_range", "battery_level", "network_generation"
            ).show(5, truncate=False)

            # Agregar por grilla
            df_grid = aggregate_by_grid(df_transformed, grid_size=0.01)

            # Calcular estadísticas
            statistics = get_statistics(df_transformed)

            logger.info("\n" + "=" * 70)
            logger.info("STATISTICS")
            logger.info("=" * 70)
            logger.info(f"Total points: {statistics['total_points']:,}")
            logger.info(f"Unique devices: {statistics['unique_devices']:,}")
            logger.info(f"Avg battery: {statistics['avg_battery']}%")
            logger.info(f"Avg signal: {statistics['avg_signal']} dBm")

            logger.info("\nPeriod distribution:")
            for item in statistics['period_distribution']:
                logger.info(f"  {item['period']}: {item['count']:,}")

            logger.info("\nAltitude distribution:")
            for item in statistics['altitude_distribution']:
                logger.info(f"  {item['range']}: {item['count']:,}")

            logger.info("\nBattery distribution:")
            for item in statistics['battery_distribution']:
                logger.info(f"  {item['level']}: {item['count']:,}")

            return df_transformed, df_grid, statistics

        except Exception as e:
            logger.error(f"✗ Error transforming: {e}")
            raise

    def load_to_postgres(self, df, table_name: str, mode: str = "append"):
        """Carga datos a PostgreSQL"""
        try:
            logger.info(f"\nLoading to '{table_name}'...")

            record_count = df.count()
            logger.info(f"Records to load: {record_count:,}")

            df.write \
                .format("jdbc") \
                .option("url", settings.postgres_jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", settings.DEST_PG_USER) \
                .option("password", settings.DEST_PG_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode(mode) \
                .save()

            logger.info(f"✓ Loaded {record_count:,} records (mode: {mode})")
            return record_count

        except Exception as e:
            logger.error(f"✗ Error loading: {e}")
            raise

    async def run_full_etl(
            self,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
            incremental: bool = True
    ) -> Dict:
        """Ejecuta ETL completo con modo incremental"""
        start_time = time.time()

        try:
            logger.info("\n" + "=" * 70)
            logger.info(f"ETL PIPELINE - {'INCREMENTAL' if incremental else 'FULL REFRESH'}")
            logger.info("=" * 70)

            # Obtener último ID procesado si es incremental
            last_id = None
            if incremental:
                from app.database.postgres_db import SessionLocal
                from app.models.etl_control import ETLControl
                db = SessionLocal()
                try:
                    last_run = db.query(ETLControl).filter(
                        ETLControl.status == 'SUCCESS'
                    ).order_by(ETLControl.execution_date.desc()).first()

                    if last_run:
                        last_id = last_run.last_processed_id
                        logger.info(f"Last processed ID: {last_id}")
                    else:
                        logger.info("No previous run found, loading all data")
                finally:
                    db.close()

            # 1. Extract
            raw_data = await self.extract_from_supabase(start_date, end_date, last_id)

            if not raw_data:
                logger.info("No new data to process")
                return {"status": "warning", "message": "No new data"}

            # 2. Transform
            df_transformed, df_grid, statistics = self.transform_with_spark(raw_data)

            if df_transformed is None:
                raise Exception("Transformation failed")

            # 3. Load
            logger.info("\n" + "=" * 70)
            logger.info("LOADING TO POSTGRESQL")
            logger.info("=" * 70)

            # Usar append para carga incremental, overwrite si no es incremental
            mode = "append" if incremental else "overwrite"
            logger.info(f"Load mode: {mode}")

            # Debug: mostrar columnas disponibles
            logger.info(f"Available columns in DataFrame: {df_transformed.columns}")

            # Agregar columna processed_at si no existe
            from pyspark.sql.functions import current_timestamp
            if 'processed_at' not in df_transformed.columns:
                df_transformed = df_transformed.withColumn('processed_at', current_timestamp())

            # Cargar tabla principal
            records_loaded = self.load_to_postgres(
                df_transformed,
                "locations",
                mode=mode
            )

            # Cargar grilla (siempre overwrite porque es agregación)
            records_grid = self.load_to_postgres(df_grid, "grid_analysis", mode="overwrite")

            # 4. Asignar distrito y provincia a cada punto
            logger.info("\n" + "=" * 70)
            logger.info("ASIGNANDO UBICACIÓN GEOGRÁFICA")
            logger.info("=" * 70)

            from app.database.postgres_db import SessionLocal
            from app.services.location_service import bulk_assign_geographic_location

            db = SessionLocal()
            try:
                rows_updated = bulk_assign_geographic_location(db)
                logger.info(f"✓ {rows_updated:,} puntos asignados a distrito y provincia")
            except Exception as e:
                logger.error(f"✗ Error asignando ubicaciones: {e}")
            finally:
                db.close()

            # 5. Registrar ejecución exitosa
            max_id = max([r['id'] for r in raw_data])
            db = SessionLocal()
            try:
                from app.models.etl_control import ETLControl
                from datetime import datetime
                control = ETLControl(
                    execution_date=datetime.utcnow(),
                    last_processed_id=max_id,
                    records_processed=records_loaded,
                    status='SUCCESS',
                    execution_time_seconds=int(time.time() - start_time)
                )
                db.add(control)
                db.commit()
                logger.info(f"✓ ETL control registered (last_id: {max_id})")
            except Exception as e:
                logger.error(f"Error registering control: {e}")
            finally:
                db.close()

            execution_time = time.time() - start_time

            logger.info("\n" + "=" * 70)
            logger.info("✓ ETL COMPLETED")
            logger.info("=" * 70)
            logger.info(f"Mode: {'INCREMENTAL' if incremental else 'FULL'}")
            logger.info(f"Time: {execution_time:.2f}s")
            logger.info(f"Records: {records_loaded:,}")
            logger.info(f"Grid cells: {records_grid:,}")
            logger.info(f"Last ID: {max_id}")
            logger.info("=" * 70 + "\n")

            return {
                "status": "success",
                "records_processed": len(raw_data),
                "records_inserted": records_loaded,
                "grid_cells": records_grid,
                "execution_time": round(execution_time, 2),
                "last_id": max_id,
                "statistics": statistics
            }

        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"\n✗ ETL FAILED: {e}")
            return {
                "status": "error",
                "message": str(e),
                "execution_time": round(execution_time, 2)
            }

    def cleanup(self):
        """Limpia recursos"""
        if self.spark:
            self.spark.stop()
            logger.info("✓ Spark stopped")