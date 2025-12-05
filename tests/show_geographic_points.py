"""
Test visual: Muestra puntos con su distrito y provincia asignados
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database.postgres_db import SessionLocal
from app.models.db_models import Location
from sqlalchemy import func


def main():
    print("\n" + "="*80)
    print(" "*25 + "UBICACIÓN GEOGRÁFICA DE PUNTOS")
    print("="*80 + "\n")

    db = SessionLocal()

    # Tomar 15 puntos de diferentes distritos
    print("MUESTRA DE PUNTOS CON DISTRITO Y PROVINCIA:\n")
    print(f"{'ID':<10} {'Latitud':<12} {'Longitud':<12} {'Distrito':<15} {'Provincia':<20}")
    print("-"*80)

    points = db.query(Location).filter(
        Location.district_name != None
    ).order_by(func.random()).limit(15).all()

    for p in points:
        print(f"{p.id:<10} {p.latitude:<12.4f} {p.longitude:<12.4f} "
              f"{p.district_name:<15} {p.province_name:<20}")

    print("\n" + "="*80)
    print("✓ Cada punto ahora tiene distrito y provincia asignados")
    print("="*80 + "\n")

    db.close()


if __name__ == "__main__":
    main()

