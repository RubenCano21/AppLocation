#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test para agrupar registros por rangos de velocidad (speed_range)
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import settings
from app.database.supabase_utils import get_supabase_client
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def classify_speed(speed):
    """Clasifica la velocidad en rangos"""
    # NULL o sin datos -> DETENIDO
    if speed is None:
        return 'DETENIDO'

    try:
        speed_val = float(speed)
        # Convertir de m/s a km/h para clasificar
        speed_kmh = speed_val * 3.6

        if speed_kmh == 0:
            return 'DETENIDO'
        elif speed_kmh <= 5:  # 0.1 - 5 km/h
            return 'CAMINANDO'
        elif speed_kmh <= 10:  # 5 - 10 km/h
            return 'CORRIENDO'
        elif speed_kmh <= 60:  # 10 - 60 km/h
            return 'TRANSPORTE PÚBLICO'
        else:  # > 60 km/h
            return 'VEHÍCULO'
    except (ValueError, TypeError):
        return 'DETENIDO'


def test_group_by_speed_range():
    """Agrupa registros por rango de velocidad"""

    logger.info("\n" + "=" * 70)
    logger.info("TEST: AGRUPAR POR RANGO DE VELOCIDAD")
    logger.info("=" * 70)

    # Conectar a Supabase
    logger.info("\n📥 Extrayendo datos de Supabase...")
    supabase = get_supabase_client()

    # Extraer todos los datos
    all_data = supabase.fetch_all_paginated()
    logger.info(f"✓ Total de registros: {len(all_data):,}")

    # Agrupar por rango de velocidad
    logger.info("\n📊 Agrupando por rango de velocidad...")
    speed_groups = {}

    for record in all_data:
        speed = record.get('speed')
        speed_range = classify_speed(speed)

        if speed_range not in speed_groups:
            speed_groups[speed_range] = []

        speed_groups[speed_range].append(record)

    # Mostrar resultados
    logger.info("\n" + "=" * 70)
    logger.info("RESULTADOS POR RANGO DE VELOCIDAD")
    logger.info("=" * 70)

    # Orden específico de rangos
    range_order = [
        'DETENIDO',
        'CAMINANDO',
        'CORRIENDO',
        'TRANSPORTE PÚBLICO',
        'VEHÍCULO'
    ]

    total_records = 0
    for speed_range in range_order:
        if speed_range in speed_groups:
            records = speed_groups[speed_range]
            count = len(records)
            total_records += count
            percentage = (count / len(all_data)) * 100

            logger.info(f"\n🚗 {speed_range}")
            logger.info(f"   Registros: {count:,} ({percentage:.2f}%)")

            # Definir rango en m/s y km/h
            if speed_range == 'DETENIDO':
                logger.info(f"   Rango: 0 km/h (incluye NULL)")
            elif speed_range == 'CAMINANDO':
                logger.info(f"   Rango: 0.1 - 5 km/h (0.03 - 1.39 m/s)")
            elif speed_range == 'CORRIENDO':
                logger.info(f"   Rango: 5 - 10 km/h (1.39 - 2.78 m/s)")
            elif speed_range == 'TRANSPORTE PÚBLICO':
                logger.info(f"   Rango: 10 - 60 km/h (2.78 - 16.67 m/s)")
            elif speed_range == 'VEHÍCULO':
                logger.info(f"   Rango: > 60 km/h (> 16.67 m/s)")

            # Mostrar muestra de datos
            if records:
                sample = records[0]
                speed_val = sample.get('speed', 0)
                logger.info(f"   Muestra: Device={sample.get('device_name', 'N/A')}, "
                           f"Speed={speed_val} m/s ({speed_val*3.6:.2f} km/h)")

    # Resumen
    logger.info("\n" + "=" * 70)
    logger.info("RESUMEN")
    logger.info("=" * 70)
    logger.info(f"Total de rangos: {len(speed_groups)}")
    logger.info(f"Total de registros: {total_records:,}")
    logger.info(f"Verificación: {total_records == len(all_data)}")

    # Estadísticas adicionales
    logger.info("\n" + "=" * 70)
    logger.info("ESTADÍSTICAS")
    logger.info("=" * 70)

    stationary = speed_groups.get('DETENIDO', [])
    moving = len(all_data) - len(stationary) - len(speed_groups.get('SIN DATOS', []))

    logger.info(f"Registros detenidos: {len(stationary):,} ({len(stationary)/len(all_data)*100:.2f}%)")
    logger.info(f"Registros en movimiento: {moving:,} ({moving/len(all_data)*100:.2f}%)")

    logger.info("\n" + "=" * 70)
    logger.info("✅ TEST COMPLETADO")
    logger.info("=" * 70)

    return speed_groups


if __name__ == "__main__":
    test_group_by_speed_range()

