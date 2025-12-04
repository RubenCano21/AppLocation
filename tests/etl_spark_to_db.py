#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ETL con Spark - Versión optimizada para cargar a BD destino
Usa el pipeline ETL original pero con configuraciones mejoradas
"""

import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.etl_service import ETLService
from app.services.dimension_service import DimensionService
from app.database.postgres_db import get_db
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def run_spark_etl_to_database():
    """ETL completo usando Spark para cargar a BD destino"""

    print("\n" + "╔" + "═"*80 + "╗")
    print("║" + " "*15 + "ETL SPARK - DESCENTRALIZACIÓN + CARGA A BD DESTINO" + " "*14 + "║")
    print("╚" + "═"*80 + "╝")

    start_time = time.time()
    etl_service = None

    try:
        print("\n📊 PASO 1: Verificar/Poblar Dimensión de Tiempo")
        print("="*70)

        try:
            dimension_service = DimensionService()
            result = dimension_service.populate_dim_time()
            print(f"✅ Dimensión dim_time: {result}")
        except Exception as e:
            print(f"⚠️ Error poblando dim_time (continuando): {e}")

        print("\n🔧 PASO 2: Inicializar Spark ETL Service")
        print("="*70)

        etl_service = ETLService()
        print("✅ Spark ETL Service inicializado")

        print("\n📥 PASO 3: Extracción desde Supabase")
        print("="*70)
        print("  📡 Fuente: Supabase (API REST)")
        print("  🔄 Método: Paginación basada en cursor")
        print("  📦 Procesando...")

        # Extraer datos con paginación mejorada
        data = await etl_service.extract_from_supabase()

        if not data:
            print("❌ No se obtuvieron datos")
            return

        print(f"✅ Extracción completa: {len(data):,} registros")

        print("\n🔄 PASO 4: Transformación con Spark")
        print("="*70)
        print("  ⚙️ Procesando con PySpark...")
        print("  🕐 Aplicando descentralización de timestamp...")
        print("  🧹 Limpiando y validando datos...")
        print("  📊 Generando agregaciones...")

        # Transformar con Spark
        df_transformed, df_grid, df_devices, statistics = etl_service.transform_with_spark(data)

        if df_transformed is None:
            print("❌ Error en transformación Spark")
            return

        print("✅ Transformación Spark completada")
        print(f"📊 Registros transformados: {statistics['total_points']:,}")
        print(f"📊 Dispositivos únicos: {statistics['unique_devices']}")
        print(f"📊 Celdas de grilla: {df_grid.count()}")

        print("\n💾 PASO 5: Carga a PostgreSQL")
        print("="*70)
        print("  🗄️ Destino: PostgreSQL")
        print("  🔄 Método: SQLAlchemy + pandas")
        print("  📋 Tablas a crear: locations, location_grid, device_stats")

        # Cargar tabla principal
        print("\n🔸 Cargando tabla principal 'locations'...")
        locations_count = etl_service.load_to_postgres(
            df_transformed,
            "locations",
            mode="overwrite"
        )

        print("\n🔸 Cargando agregaciones geográficas 'location_grid'...")
        grid_count = etl_service.load_to_postgres(
            df_grid,
            "location_grid",
            mode="overwrite"
        )

        print("\n🔸 Cargando estadísticas de dispositivos 'device_stats'...")
        device_count = etl_service.load_to_postgres(
            df_devices,
            "device_stats",
            mode="overwrite"
        )

        elapsed = time.time() - start_time

        print(f"\n" + "🎉" + "="*78 + "🎉")
        print("                   ¡ETL SPARK COMPLETADO EXITOSAMENTE!")
        print("="*80)
        print(f"📊 Datos extraídos: {len(data):,}")
        print(f"📊 Registros en locations: {locations_count:,}")
        print(f"📊 Celdas en location_grid: {grid_count:,}")
        print(f"📊 Dispositivos en device_stats: {device_count:,}")
        print(f"⏱️  Tiempo total: {elapsed:.1f} segundos")
        print(f"🚀 Velocidad: {len(data)/elapsed:.0f} registros/segundo")
        print()
        print("✅ CAMPOS DE DESCENTRALIZACIÓN DE TIMESTAMP:")
        print("   📅 date - Fecha (YYYY-MM-DD)")
        print("   🕐 hour_value - Hora del día (0-23)")
        print("   📊 time_period - Período textual (MAÑANA/TARDE/NOCHE)")
        print("   🔤 time_period_code - Código del período (MOR/AFT/NIG)")
        print("   🔗 time_id - Foreign Key a dim_time.id")
        print()
        print("📊 ESTADÍSTICAS GENERALES:")
        print(f"   • Puntos totales: {statistics['total_points']:,}")
        print(f"   • Dispositivos únicos: {statistics['unique_devices']}")
        print(f"   • Batería promedio: {statistics['avg_battery']:.1f}%")
        print(f"   • Señal promedio: {statistics['avg_signal']:.1f} dBm")
        print(f"   • Velocidad promedio: {statistics['avg_speed']:.2f} m/s")
        print(f"   • Período de datos: {statistics['date_range']['start']}")
        print(f"     hasta {statistics['date_range']['end']}")
        print()
        print("🗄️ TABLAS CREADAS EN BD DESTINO:")
        print("   📋 locations - Tabla principal con timestamp descentralizado")
        print("   📋 location_grid - Agregaciones por celdas geográficas")
        print("   📋 device_stats - Estadísticas resumidas por dispositivo")
        print("   📋 dim_time - Dimensión de tiempo (0-23 horas)")
        print()
        print("🔥 CONSULTAS DE ALTO RENDIMIENTO DISPONIBLES:")
        print("   ⚡ Filtros por período optimizados")
        print("   ⚡ Agregaciones temporales eficientes")
        print("   ⚡ Joins rápidos con dimensión de tiempo")
        print("="*80)

    except Exception as e:
        elapsed = time.time() - start_time
        print(f"\n❌ ERROR EN ETL SPARK")
        print("="*50)
        print(f"❌ Error: {e}")
        print(f"⏱️ Tiempo antes del fallo: {elapsed:.1f}s")

        import traceback
        traceback.print_exc()

    finally:
        if etl_service and hasattr(etl_service, 'spark') and etl_service.spark:
            etl_service.spark.stop()
            print("\n🔌 Sesión Spark finalizada")

if __name__ == "__main__":
    asyncio.run(run_spark_etl_to_database())
