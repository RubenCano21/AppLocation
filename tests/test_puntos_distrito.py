"""
Test para verificar en qué distrito se encuentra cada punto de ubicación
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database.postgres_db import SessionLocal
from app.services.district_service import DistrictService
from app.models.db_models import Location
from sqlalchemy import func


def test_puntos_por_distrito():
    print("\n" + "="*70)
    print("TEST: Puntos por Distrito")
    print("="*70)

    db = SessionLocal()

    # Obtener una muestra de ubicaciones
    print("\nAnalizando muestra de 20 ubicaciones aleatorias...\n")

    sample_locations = db.query(Location).order_by(func.random()).limit(20).all()

    results = {
        'encontrados': 0,
        'no_encontrados': 0,
        'por_distrito': {}
    }

    for loc in sample_locations:
        district = DistrictService.get_district_by_point(db, loc.latitude, loc.longitude)

        if district:
            results['encontrados'] += 1
            distrito_nombre = district.district_name

            if distrito_nombre not in results['por_distrito']:
                results['por_distrito'][distrito_nombre] = []

            results['por_distrito'][distrito_nombre].append({
                'id': loc.id,
                'lat': loc.latitude,
                'lon': loc.longitude,
                'device': loc.device_id
            })

            print(f"✓ Ubicación {loc.id}: ({loc.latitude:.4f}, {loc.longitude:.4f})")
            print(f"  → {distrito_nombre}")
        else:
            results['no_encontrados'] += 1
            print(f"✗ Ubicación {loc.id}: ({loc.latitude:.4f}, {loc.longitude:.4f})")
            print(f"  → Fuera de distritos")

    # Resumen
    print("\n" + "="*70)
    print("RESUMEN")
    print("="*70)
    print(f"Ubicaciones dentro de distritos: {results['encontrados']}")
    print(f"Ubicaciones fuera de distritos: {results['no_encontrados']}")

    if results['por_distrito']:
        print(f"\nDistribución por distrito:")
        for distrito, ubicaciones in sorted(results['por_distrito'].items()):
            print(f"  • {distrito}: {len(ubicaciones)} ubicaciones")

    # Ahora mostrar estadísticas completas de todas las ubicaciones
    print("\n" + "="*70)
    print("ESTADÍSTICAS COMPLETAS DE TODAS LAS UBICACIONES")
    print("="*70)

    stats = DistrictService.get_all_districts_statistics(db)

    print(f"\n{'Distrito':<15} {'Ubicaciones':<15} {'Dispositivos':<15} {'Área (km²)':<12}")
    print("-"*70)

    total_ubicaciones = 0
    for stat in stats:
        if stat['total_locations'] > 0:
            print(f"{stat['district_name']:<15} "
                  f"{stat['total_locations']:<15,} "
                  f"{stat['unique_devices']:<15} "
                  f"{stat['area_km2']:<12.2f}")
            total_ubicaciones += stat['total_locations']

    print("-"*70)
    print(f"{'TOTAL':<15} {total_ubicaciones:<15,}")

    db.close()

    print("\n✓ Test completado\n")


if __name__ == "__main__":
    test_puntos_por_distrito()

