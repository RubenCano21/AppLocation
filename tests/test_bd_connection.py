#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test de conexión a BD y carga de datos con descentralización
Versión sin Spark - Solo pandas
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database.supabase_utils import get_supabase_client
from app.config import settings
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def descentralizar_timestamp_pandas(df):
    """
    Aplicar descentralización de timestamp usando pandas
    """
    # Convertir timestamp
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Extraer componentes
    df['date'] = df['timestamp'].dt.date
    df['hour_value'] = df['timestamp'].dt.hour

    # Clasificar período
    conditions = [
        (df['hour_value'] >= 6) & (df['hour_value'] < 12),
        (df['hour_value'] >= 12) & (df['hour_value'] < 19),
    ]
    choices = ['MAÑANA', 'TARDE']
    df['time_period'] = np.select(conditions, choices, default='NOCHE')

    # Código del período
    conditions_code = [
        (df['hour_value'] >= 6) & (df['hour_value'] < 12),
        (df['hour_value'] >= 12) & (df['hour_value'] < 19),
    ]
    choices_code = ['MOR', 'AFT']
    df['time_period_code'] = np.select(conditions_code, choices_code, default='NIG')

    # time_id
    df['time_id'] = df['hour_value']

    return df

def test_bd_connection():
    """Test de conexión y carga a BD"""

    print("\n" + "="*70)
    print("    TEST DE CONEXIÓN BD Y DESCENTRALIZACIÓN")
    print("="*70)

    try:
        print("\n📥 PASO 1: Extraer muestra de datos")
        print("-" * 40)

        supabase_client = get_supabase_client()
        sample_data = supabase_client.fetch_all(limit=100)  # Solo 100 registros

        if not sample_data:
            print("❌ No hay datos en Supabase")
            return

        print(f"✅ Extraídos {len(sample_data)} registros")

        print("\n📊 PASO 2: Convertir a DataFrame y aplicar descentralización")
        print("-" * 40)

        df = pd.DataFrame(sample_data)
        print(f"📋 Columnas originales: {list(df.columns)}")
        print(f"📊 Registro de ejemplo: {df.iloc[0].to_dict()}")

        # Aplicar descentralización
        df = descentralizar_timestamp_pandas(df)

        print("✅ Descentralización aplicada")
        print(f"📋 Nuevas columnas: {list(df.columns)}")

        # Mostrar muestra de datos descentralizados
        print("\n📊 Muestra de datos descentralizados:")
        sample_cols = ['id', 'timestamp', 'date', 'hour_value', 'time_period', 'time_period_code', 'time_id']
        available_cols = [col for col in sample_cols if col in df.columns]
        print(df[available_cols].head().to_string())

        print("\n📊 Distribución por período:")
        period_dist = df['time_period'].value_counts()
        for period, count in period_dist.items():
            print(f"  {period}: {count}")

        print("\n💾 PASO 3: Conectar a BD destino")
        print("-" * 40)

        # Crear conexión
        engine = create_engine(settings.postgres_url)

        # Test de conexión
        from sqlalchemy import text
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test"))
            test_value = result.scalar()
            print(f"✅ Conexión BD exitosa (test: {test_value})")

        print("\n💾 PASO 4: Cargar datos a BD")
        print("-" * 40)

        # Cargar datos
        table_name = "locations_test_pandas"
        df.to_sql(
            table_name,
            engine,
            if_exists='replace',
            index=False,
            chunksize=50
        )

        print(f"✅ Datos cargados en tabla '{table_name}'")

        # Verificar carga
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            count = result.scalar()
            print(f"📊 Registros en BD: {count}")

            # Verificar columnas de descentralización
            result = conn.execute(text(f"SELECT date, hour_value, time_period, time_period_code, time_id FROM {table_name} LIMIT 5"))
            rows = result.fetchall()
            print(f"\n📋 Muestra de datos descentralizados en BD:")
            for row in rows:
                print(f"  {row}")

        engine.dispose()

        print("\n🎉 ¡ÉXITO COMPLETO!")
        print("="*70)
        print("✅ Extracción de Supabase: OK")
        print("✅ Descentralización de timestamp: OK")
        print("✅ Conexión a BD destino: OK")
        print("✅ Carga de datos: OK")
        print("✅ Verificación de datos: OK")
        print()
        print("🚀 La funcionalidad está lista para el ETL completo")
        print("="*70)

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_bd_connection()
