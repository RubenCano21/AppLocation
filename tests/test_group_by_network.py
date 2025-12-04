#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test para agrupar registros por network_type
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


def test_group_by_network():
    """Agrupa registros por network_type"""

    logger.info("\n" + "=" * 70)
    logger.info("TEST: AGRUPAR POR NETWORK_TYPE")
    logger.info("=" * 70)

    # Conectar a Supabase
    logger.info("\n📥 Extrayendo datos de Supabase...")
    supabase = get_supabase_client()

    # Extraer todos los datos
    all_data = supabase.fetch_all_paginated()
    logger.info(f"✓ Total de registros: {len(all_data):,}")

    # Agrupar por network_type
    logger.info("\n📊 Agrupando por network_type...")
    network_groups = {}

    for record in all_data:
        network = record.get('network_type')
        if network is None or network == '':
            network = 'NULL'
        else:
            network = network.strip()

        if network not in network_groups:
            network_groups[network] = []

        network_groups[network].append(record)

    # Mostrar resultados
    logger.info("\n" + "=" * 70)
    logger.info("RESULTADOS POR TIPO DE RED")
    logger.info("=" * 70)

    # Ordenar por cantidad de registros (descendente)
    sorted_networks = sorted(network_groups.items(), key=lambda x: len(x[1]), reverse=True)

    total_records = 0
    for network, records in sorted_networks:
        count = len(records)
        total_records += count
        percentage = (count / len(all_data)) * 100

        logger.info(f"\n📡 {network}")
        logger.info(f"   Registros: {count:,} ({percentage:.2f}%)")

        # Mostrar muestra de datos
        if records:
            sample = records[0]
            logger.info(f"   Muestra: Device={sample.get('device_name', 'N/A')}, "
                       f"Operator={sample.get('sim_operator', 'N/A')}")

    # Resumen
    logger.info("\n" + "=" * 70)
    logger.info("RESUMEN")
    logger.info("=" * 70)
    logger.info(f"Total de tipos de red diferentes: {len(network_groups)}")
    logger.info(f"Total de registros: {total_records:,}")
    logger.info(f"Verificación: {total_records == len(all_data)}")

    # Top 10
    logger.info(f"\n🏆 TOP 10 TIPOS DE RED:")
    for i, (network, records) in enumerate(sorted_networks[:10], 1):
        percentage = (len(records) / len(all_data)) * 100
        logger.info(f"{i}. {network}: {len(records):,} registros ({percentage:.2f}%)")

    # Agrupar por generación (WiFi, 2G, 3G, 4G, 5G)
    logger.info("\n" + "=" * 70)
    logger.info("AGRUPACIÓN POR GENERACIÓN")
    logger.info("=" * 70)

    generation_groups = {
        'WiFi': 0,
        '5G': 0,
        '4G': 0,
        '3G': 0,
        '2G': 0,
        'SIN DATOS': 0
    }

    for network, records in network_groups.items():
        network_upper = network.upper()
        count = len(records)

        if 'WIFI' in network_upper or 'WI-FI' in network_upper:
            generation_groups['WiFi'] += count
        elif '5G' in network_upper:
            generation_groups['5G'] += count
        elif '4G' in network_upper or 'LTE' in network_upper or ('MOBILE' in network_upper and len(network_upper) < 15):
            generation_groups['4G'] += count
        elif '3G' in network_upper or 'HSDPA' in network_upper or 'HSPA' in network_upper or 'UMTS' in network_upper or 'WCDMA' in network_upper:
            generation_groups['3G'] += count
        elif '2G' in network_upper or 'EDGE' in network_upper or 'GPRS' in network_upper or 'GSM' in network_upper:
            generation_groups['2G'] += count
        else:
            generation_groups['SIN DATOS'] += count

    # Ordenar por cantidad
    sorted_generations = sorted(generation_groups.items(), key=lambda x: x[1], reverse=True)

    for generation, count in sorted_generations:
        if count > 0:
            percentage = (count / len(all_data)) * 100
            logger.info(f"📶 {generation}: {count:,} registros ({percentage:.2f}%)")

    logger.info("\n" + "=" * 70)
    logger.info("✅ TEST COMPLETADO")
    logger.info("=" * 70)

    return network_groups, generation_groups


if __name__ == "__main__":
    test_group_by_network()

