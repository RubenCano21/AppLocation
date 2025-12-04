#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test para analizar el campo speed y determinar rangos apropiados
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


def analyze_speed():
    """Analiza el campo speed para determinar rangos"""

    logger.info("\n" + "=" * 70)
    logger.info("TEST: ANÁLISIS DE VELOCIDAD (SPEED)")
    logger.info("=" * 70)

    # Conectar a Supabase
    logger.info("\n📥 Extrayendo datos de Supabase...")
    supabase = get_supabase_client()

    # Extraer todos los datos
    all_data = supabase.fetch_all_paginated()
    logger.info(f"✓ Total de registros: {len(all_data):,}")

    # Analizar velocidades
    logger.info("\n📊 Analizando velocidades...")
    speeds = []
    null_count = 0
    zero_count = 0

    for record in all_data:
        speed = record.get('speed')
        if speed is None:
            null_count += 1
        else:
            try:
                speed_val = float(speed)
                speeds.append(speed_val)
                if speed_val == 0:
                    zero_count += 1
            except (ValueError, TypeError):
                null_count += 1

    # Estadísticas básicas
    logger.info("\n" + "=" * 70)
    logger.info("ESTADÍSTICAS BÁSICAS")
    logger.info("=" * 70)
    logger.info(f"Total de registros: {len(all_data):,}")
    logger.info(f"Valores NULL/inválidos: {null_count:,} ({(null_count/len(all_data)*100):.2f}%)")
    logger.info(f"Valores válidos: {len(speeds):,} ({(len(speeds)/len(all_data)*100):.2f}%)")
    logger.info(f"Valores en cero: {zero_count:,} ({(zero_count/len(all_data)*100):.2f}%)")

    if speeds:
        speeds_sorted = sorted(speeds)
        min_speed = min(speeds)
        max_speed = max(speeds)
        avg_speed = sum(speeds) / len(speeds)
        median_speed = speeds_sorted[len(speeds_sorted) // 2]

        logger.info("\n" + "=" * 70)
        logger.info("VALORES EXTREMOS")
        logger.info("=" * 70)
        logger.info(f"Velocidad mínima: {min_speed:.2f} m/s")
        logger.info(f"Velocidad máxima: {max_speed:.2f} m/s")
        logger.info(f"Velocidad promedio: {avg_speed:.2f} m/s")
        logger.info(f"Velocidad mediana: {median_speed:.2f} m/s")

        # Percentiles
        p25 = speeds_sorted[int(len(speeds_sorted) * 0.25)]
        p75 = speeds_sorted[int(len(speeds_sorted) * 0.75)]
        p90 = speeds_sorted[int(len(speeds_sorted) * 0.90)]
        p95 = speeds_sorted[int(len(speeds_sorted) * 0.95)]
        p99 = speeds_sorted[int(len(speeds_sorted) * 0.99)]

        logger.info("\n" + "=" * 70)
        logger.info("PERCENTILES")
        logger.info("=" * 70)
        logger.info(f"Percentil 25: {p25:.2f} m/s")
        logger.info(f"Percentil 75: {p75:.2f} m/s")
        logger.info(f"Percentil 90: {p90:.2f} m/s")
        logger.info(f"Percentil 95: {p95:.2f} m/s")
        logger.info(f"Percentil 99: {p99:.2f} m/s")

        # Distribución por rangos (análisis preliminar)
        logger.info("\n" + "=" * 70)
        logger.info("DISTRIBUCIÓN PRELIMINAR")
        logger.info("=" * 70)

        ranges = {
            'Detenido (0 m/s)': 0,
            'Muy lento (0.1-2 m/s)': 0,
            'Lento (2-5 m/s)': 0,
            'Caminando (5-10 m/s)': 0,
            'Moderado (10-20 m/s)': 0,
            'Rápido (20-30 m/s)': 0,
            'Muy rápido (>30 m/s)': 0
        }

        for speed in speeds:
            if speed == 0:
                ranges['Detenido (0 m/s)'] += 1
            elif speed < 2:
                ranges['Muy lento (0.1-2 m/s)'] += 1
            elif speed < 5:
                ranges['Lento (2-5 m/s)'] += 1
            elif speed < 10:
                ranges['Caminando (5-10 m/s)'] += 1
            elif speed < 20:
                ranges['Moderado (10-20 m/s)'] += 1
            elif speed < 30:
                ranges['Rápido (20-30 m/s)'] += 1
            else:
                ranges['Muy rápido (>30 m/s)'] += 1

        for range_name, count in ranges.items():
            percentage = (count / len(speeds)) * 100
            logger.info(f"🚗 {range_name}: {count:,} ({percentage:.2f}%)")

        # Top 10 velocidades más altas
        logger.info("\n" + "=" * 70)
        logger.info("TOP 10 VELOCIDADES MÁS ALTAS")
        logger.info("=" * 70)

        top_speeds = []
        for record in all_data:
            speed = record.get('speed')
            if speed is not None and speed > 0:
                try:
                    speed_val = float(speed)
                    top_speeds.append({
                        'speed': speed_val,
                        'device': record.get('device_name', 'N/A'),
                        'network': record.get('network_type', 'N/A'),
                        'timestamp': record.get('timestamp', 'N/A')
                    })
                except (ValueError, TypeError):
                    pass

        top_speeds_sorted = sorted(top_speeds, key=lambda x: x['speed'], reverse=True)[:10]

        for i, item in enumerate(top_speeds_sorted, 1):
            logger.info(f"{i}. {item['speed']:.2f} m/s ({item['speed']*3.6:.2f} km/h) - "
                       f"Device: {item['device']}, Network: {item['network']}")

        # Sugerencia de rangos
        logger.info("\n" + "=" * 70)
        logger.info("💡 SUGERENCIA DE RANGOS")
        logger.info("=" * 70)
        logger.info("Basado en el análisis, se sugieren los siguientes rangos:")
        logger.info("")
        logger.info("1. DETENIDO: 0 m/s (0 km/h)")
        logger.info("2. CAMINANDO: 0.1 - 2 m/s (0.36 - 7.2 km/h)")
        logger.info("3. BICICLETA: 2 - 8 m/s (7.2 - 28.8 km/h)")
        logger.info("4. VEHÍCULO LENTO: 8 - 15 m/s (28.8 - 54 km/h)")
        logger.info("5. VEHÍCULO MODERADO: 15 - 25 m/s (54 - 90 km/h)")
        logger.info("6. VEHÍCULO RÁPIDO: > 25 m/s (> 90 km/h)")
        logger.info("")
        logger.info("(Nota: Valores muy altos pueden ser errores del GPS)")

    else:
        logger.warning("⚠️ No hay valores de velocidad válidos")

    logger.info("\n" + "=" * 70)
    logger.info("✅ ANÁLISIS COMPLETADO")
    logger.info("=" * 70)

    return speeds


if __name__ == "__main__":
    analyze_speed()

