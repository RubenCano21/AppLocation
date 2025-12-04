#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script de verificación de datos en BD destino
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import settings
from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO)

def verificar_datos_bd():
    """Verificar qué datos hay en la BD destino"""

    print("\n" + "="*60)
    print("    VERIFICACIÓN DE DATOS EN BD DESTINO")
    print("="*60)

    try:
        print("\n🔗 Conectando a PostgreSQL...")
        engine = create_engine(settings.postgres_url)

        with engine.connect() as conn:
            # Verificar versión de PostgreSQL
            result = conn.execute(text("SELECT version()"))
            version = result.scalar()
            print(f"✅ Conectado a: {version[:60]}...")

            # Listar tablas relacionadas con locations
            print("\n📋 Tablas disponibles:")
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name LIKE '%location%'
                ORDER BY table_name
            """))

            tables = result.fetchall()
            if tables:
                for table in tables:
                    print(f"  📋 {table[0]}")

                    # Contar registros en cada tabla
                    try:
                        count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table[0]}"))
                        count = count_result.scalar()
                        print(f"      📊 {count:,} registros")
                    except Exception as e:
                        print(f"      ❌ Error contando: {e}")
            else:
                print("  ❌ No hay tablas de locations")

            # Si existe la tabla locations, verificar campos de descentralización
            try:
                print("\n🔍 Verificando campos de descentralización en 'locations'...")
                result = conn.execute(text("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = 'locations'
                    AND column_name IN ('date', 'hour_value', 'time_period', 'time_period_code', 'time_id')
                    ORDER BY column_name
                """))

                desc_fields = result.fetchall()
                if desc_fields:
                    print("  ✅ Campos de descentralización encontrados:")
                    for field in desc_fields:
                        print(f"    📊 {field[0]} ({field[1]})")

                    # Mostrar distribución por período
                    print("\n📊 Distribución por período de tiempo:")
                    result = conn.execute(text("""
                        SELECT time_period, COUNT(*) as cantidad
                        FROM locations 
                        GROUP BY time_period 
                        ORDER BY cantidad DESC
                    """))

                    for row in result.fetchall():
                        period, count = row
                        print(f"    📈 {period}: {count:,}")

                    # Mostrar muestra de datos
                    print("\n📋 Muestra de datos con descentralización:")
                    result = conn.execute(text("""
                        SELECT id, timestamp, date, hour_value, time_period, time_period_code
                        FROM locations 
                        ORDER BY id 
                        LIMIT 3
                    """))

                    for row in result.fetchall():
                        print(f"    ID:{row[0]} | {row[1]} | {row[2]} | H:{row[3]} | {row[4]}({row[5]})")

                else:
                    print("  ⚠️ No se encontraron campos de descentralización")

            except Exception as e:
                print(f"  ❌ Error verificando descentralización: {e}")

        engine.dispose()

        print(f"\n✅ VERIFICACIÓN COMPLETA")
        print("="*60)

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    verificar_datos_bd()
