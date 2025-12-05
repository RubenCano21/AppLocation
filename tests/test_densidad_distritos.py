"""
Test de densidad: Muestra cuántos puntos hay en cada distrito
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database.postgres_db import SessionLocal
from app.services.district_service import DistrictService


def crear_barra(valor, max_valor, ancho=40):
    """Crea una barra visual para representar valores"""
    if max_valor == 0:
        return ""
    proporcion = valor / max_valor
    bloques = int(proporcion * ancho)
    return "█" * bloques


def test_densidad_distritos():
    print("\n" + "="*80)
    print(" "*20 + "DENSIDAD DE PUNTOS POR DISTRITO")
    print("="*80)

    db = SessionLocal()

    stats = DistrictService.get_all_districts_statistics(db)

    # Calcular máximos para las barras
    max_ubicaciones = max(s['total_locations'] for s in stats)
    max_densidad = max(s['total_locations'] / s['area_km2'] if s['area_km2'] > 0 else 0 for s in stats)

    print("\n1. UBICACIONES POR DISTRITO")
    print("-"*80)
    print(f"{'Distrito':<12} {'Ubicaciones':<15} {'Gráfico':<40}")
    print("-"*80)

    for stat in sorted(stats, key=lambda x: x['total_locations'], reverse=True):
        barra = crear_barra(stat['total_locations'], max_ubicaciones)
        print(f"{stat['district_name']:<12} {stat['total_locations']:>10,}     {barra}")

    print("\n2. DENSIDAD (Ubicaciones por km²)")
    print("-"*80)
    print(f"{'Distrito':<12} {'Densidad':<15} {'Gráfico':<40}")
    print("-"*80)

    densidades = []
    for stat in stats:
        if stat['area_km2'] > 0:
            densidad = stat['total_locations'] / stat['area_km2']
            densidades.append({
                'distrito': stat['district_name'],
                'densidad': densidad,
                'ubicaciones': stat['total_locations'],
                'area': stat['area_km2']
            })

    for d in sorted(densidades, key=lambda x: x['densidad'], reverse=True):
        barra = crear_barra(d['densidad'], max_densidad)
        print(f"{d['distrito']:<12} {d['densidad']:>10.2f}     {barra}")

    print("\n3. TOP 5 DISTRITOS MÁS DENSOS")
    print("-"*80)
    for i, d in enumerate(sorted(densidades, key=lambda x: x['densidad'], reverse=True)[:5], 1):
        print(f"{i}. {d['distrito']}")
        print(f"   Densidad: {d['densidad']:.2f} ubicaciones/km²")
        print(f"   Total ubicaciones: {d['ubicaciones']:,}")
        print(f"   Área: {d['area']:.2f} km²")
        print()

    print("4. ESTADÍSTICAS GENERALES")
    print("-"*80)

    total_ubicaciones = sum(s['total_locations'] for s in stats)
    total_area = sum(s['area_km2'] for s in stats)
    densidad_promedio = total_ubicaciones / total_area if total_area > 0 else 0

    print(f"Total de ubicaciones: {total_ubicaciones:,}")
    print(f"Área total: {total_area:.2f} km²")
    print(f"Densidad promedio: {densidad_promedio:.2f} ubicaciones/km²")
    print(f"Distritos con datos: {sum(1 for s in stats if s['total_locations'] > 0)}/16")

    db.close()

    print("\n" + "="*80)
    print("✓ Test completado")
    print("="*80 + "\n")


if __name__ == "__main__":
    test_densidad_distritos()

