"""
Test para verificar que los puntos tienen distrito y provincia asignados
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database.postgres_db import SessionLocal
from app.models.db_models import Location
from sqlalchemy import func


def test_geographic_assignment():
    print("\n" + "="*70)
    print("VERIFICACIÓN: Distrito y Provincia por Punto")
    print("="*70 + "\n")

    db = SessionLocal()

    try:
        # Total de ubicaciones
        total = db.query(Location).count()
        print(f"Total de ubicaciones: {total:,}")

        # Con distrito asignado
        con_distrito = db.query(Location).filter(Location.district_id != None).count()
        print(f"Con distrito: {con_distrito:,} ({con_distrito/total*100:.1f}%)")

        # Con provincia asignada
        con_provincia = db.query(Location).filter(Location.province_id != None).count()
        print(f"Con provincia: {con_provincia:,} ({con_provincia/total*100:.1f}%)")

        # Muestra de 10 puntos aleatorios
        print("\n" + "-"*70)
        print("MUESTRA DE PUNTOS CON UBICACIÓN GEOGRÁFICA")
        print("-"*70 + "\n")

        sample = db.query(Location).filter(
            Location.district_name != None
        ).order_by(func.random()).limit(10).all()

        for loc in sample:
            print(f"ID: {loc.id}")
            print(f"  Coordenadas: ({loc.latitude:.4f}, {loc.longitude:.4f})")
            print(f"  Distrito: {loc.district_name}")
            print(f"  Provincia: {loc.province_name}")
            print(f"  Dispositivo: {loc.device_id}")
            print()

        # Distribución por distrito
        print("-"*70)
        print("TOP 10 DISTRITOS CON MÁS UBICACIONES")
        print("-"*70 + "\n")

        distrito_stats = db.query(
            Location.district_name,
            func.count(Location.id).label('count'),
            func.avg(Location.battery).label('avg_battery'),
            func.avg(Location.signal).label('avg_signal')
        ).filter(
            Location.district_name != None
        ).group_by(
            Location.district_name
        ).order_by(
            func.count(Location.id).desc()
        ).limit(10).all()

        print(f"{'Distrito':<15} {'Ubicaciones':<15} {'Bat. Prom.':<12} {'Señal Prom.'}")
        print("-"*70)
        for distrito, count, battery, signal in distrito_stats:
            bat_str = f"{battery:.1f}%" if battery else "N/A"
            sig_str = f"{signal:.1f} dBm" if signal else "N/A"
            print(f"{distrito:<15} {count:>10,}     {bat_str:<12} {sig_str}")

        print("\n" + "="*70)
        print("✓ VERIFICACIÓN COMPLETADA")
        print("="*70 + "\n")

        print("RESUMEN:")
        print(f"  • {con_distrito:,} puntos con distrito ({con_distrito/total*100:.1f}%)")
        print(f"  • {con_provincia:,} puntos con provincia ({con_provincia/total*100:.1f}%)")
        print(f"  • {total - con_distrito:,} puntos fuera de distritos")
        print()

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


if __name__ == "__main__":
    test_geographic_assignment()

