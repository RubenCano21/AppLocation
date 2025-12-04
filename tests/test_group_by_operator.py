#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test para agrupar registros por sim_operator
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


def normalize_operator(operator):
    """Normaliza el nombre del operador para agrupar variantes"""
    # NULL, vacío, Unknown -> SIN SEÑAL
    if operator is None or operator == '' or (isinstance(operator, str) and operator.strip() == ''):
        return 'SIN SEÑAL'

    operator = operator.strip()
    operator_upper = operator.upper()

    # Unknown, Sin señal -> SIN SEÑAL
    if operator_upper in ['UNKNOWN', 'SIN SEÑAL', 'SIN SEAL', 'N/A', 'NA']:
        return 'SIN SEÑAL'

    # Normalizar ENTEL y sus variantes
    if any(x in operator_upper for x in ['ENTEL', 'BOMOV', '+18VACUNATE', 'LADISTANCIANOSCUIDA', 'MOVIL GSM', 'T-MOBILE']):
        return 'ENTEL'

    # TIGO
    if 'TIGO' in operator_upper:
        return 'TIGO'

    # VIVA
    if 'VIVA' in operator_upper:
        return 'VIVA'

    # Sin señal (variantes)
    if 'SIN' in operator_upper or 'SEAL' in operator_upper:
        return 'SIN SEÑAL'

    # Si no coincide con ninguno, devolver en mayúsculas
    return operator_upper


def test_group_by_operator():
    """Agrupa registros por sim_operator"""

    logger.info("\n" + "=" * 70)
    logger.info("TEST: AGRUPAR POR SIM_OPERATOR (NORMALIZADO)")
    logger.info("=" * 70)

    # Conectar a Supabase
    logger.info("\n📥 Extrayendo datos de Supabase...")
    supabase = get_supabase_client()

    # Extraer todos los datos
    all_data = supabase.fetch_all_paginated()
    logger.info(f"✓ Total de registros: {len(all_data):,}")

    # Agrupar por sim_operator
    logger.info("\n📊 Agrupando por sim_operator (normalizando variantes)...")
    operator_groups = {}
    operator_mapping = {}  # Para rastrear qué variantes se agruparon

    for record in all_data:
        operator_original = record.get('sim_operator')
        operator_normalized = normalize_operator(operator_original)

        if operator_normalized not in operator_groups:
            operator_groups[operator_normalized] = []
            operator_mapping[operator_normalized] = set()

        operator_groups[operator_normalized].append(record)

        # Registrar la variante original
        if operator_original is None or operator_original == '':
            operator_mapping[operator_normalized].add('NULL')
        else:
            operator_mapping[operator_normalized].add(operator_original)

    # Mostrar resultados
    logger.info("\n" + "=" * 70)
    logger.info("RESULTADOS POR OPERADOR")
    logger.info("=" * 70)

    # Ordenar por cantidad de registros (descendente)
    sorted_operators = sorted(operator_groups.items(), key=lambda x: len(x[1]), reverse=True)

    total_records = 0
    for operator, records in sorted_operators:
        count = len(records)
        total_records += count
        percentage = (count / len(all_data)) * 100

        logger.info(f"\n📱 {operator}")
        logger.info(f"   Registros: {count:,} ({percentage:.2f}%)")

        # Mostrar variantes agrupadas
        variants = operator_mapping.get(operator, set())
        if len(variants) > 1 or (len(variants) == 1 and operator not in variants):
            logger.info(f"   Variantes agrupadas: {', '.join(sorted(variants))}")

        # Mostrar muestra de datos
        if records:
            sample = records[0]
            logger.info(f"   Muestra: Device={sample.get('device_name', 'N/A')}, "
                       f"Network={sample.get('network_type', 'N/A')}")

    # Resumen
    logger.info("\n" + "=" * 70)
    logger.info("RESUMEN")
    logger.info("=" * 70)
    logger.info(f"Total de operadores diferentes: {len(operator_groups)}")
    logger.info(f"Total de registros: {total_records:,}")
    logger.info(f"Verificación: {total_records == len(all_data)}")

    # Top operadores
    logger.info(f"\n🏆 TOP {len(sorted_operators)} OPERADORES (NORMALIZADOS):")
    for i, (operator, records) in enumerate(sorted_operators, 1):
        percentage = (len(records) / len(all_data)) * 100
        logger.info(f"{i}. {operator}: {len(records):,} registros ({percentage:.2f}%)")

    logger.info("\n" + "=" * 70)
    logger.info("✅ TEST COMPLETADO")
    logger.info("=" * 70)

    return operator_groups


if __name__ == "__main__":
    test_group_by_operator()

