"""
Script para asignar distrito y provincia a todos los puntos existentes
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database.postgres_db import SessionLocal, init_db
from app.services.location_service import bulk_assign_geographic_location
from app.models.db_models import Location
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def main():
    print("\n" + "="*70)
    print("ASIGNACIÓN DE DISTRITO Y PROVINCIA A UBICACIONES")
    print("="*70)

    # Inicializar BD
    print("\nInicializando base de datos...")
    init_db()

    db = SessionLocal()

    try:
        # Verificar total de ubicaciones
        total = db.query(Location).count()
        print(f"\nTotal de ubicaciones: {total:,}")

        # Ubicaciones sin distrito
        sin_distrito = db.query(Location).filter(Location.district_id == None).count()
        print(f"Sin distrito: {sin_distrito:,}")

        # Ubicaciones sin provincia
        sin_provincia = db.query(Location).filter(Location.province_id == None).count()
        print(f"Sin provincia: {sin_provincia:,}")

        if sin_distrito == 0 and sin_provincia == 0:
            print("\n✓ Todas las ubicaciones ya tienen distrito y provincia asignados")
            return

        print("\n" + "-"*70)
        print("Iniciando asignación...")
        print("-"*70)

        # Asignar ubicaciones
        rows_updated = bulk_assign_geographic_location(db)

        print("\n" + "="*70)
        print("RESULTADOS")
        print("="*70)

        # Verificar después
        con_distrito = db.query(Location).filter(Location.district_id != None).count()
        con_provincia = db.query(Location).filter(Location.province_id != None).count()

        print(f"Ubicaciones procesadas: {rows_updated:,}")
        print(f"Con distrito: {con_distrito:,} ({con_distrito/total*100:.1f}%)")
        print(f"Con provincia: {con_provincia:,} ({con_provincia/total*100:.1f}%)")

        # Mostrar distribución por distrito
        print("\n" + "-"*70)
        print("DISTRIBUCIÓN POR DISTRITO")
        print("-"*70)

        from sqlalchemy import func
        distrito_stats = db.query(
            Location.district_name,
            func.count(Location.id).label('count')
        ).filter(
            Location.district_name != None
        ).group_by(
            Location.district_name
        ).order_by(
            func.count(Location.id).desc()
        ).all()

        for distrito, count in distrito_stats:
            print(f"  {distrito}: {count:,} ubicaciones")

        # Mostrar distribución por provincia
        print("\n" + "-"*70)
        print("DISTRIBUCIÓN POR PROVINCIA")
        print("-"*70)

        provincia_stats = db.query(
            Location.province_name,
            func.count(Location.id).label('count')
        ).filter(
            Location.province_name != None
        ).group_by(
            Location.province_name
        ).order_by(
            func.count(Location.id).desc()
        ).limit(10).all()

        for provincia, count in provincia_stats:
            print(f"  {provincia}: {count:,} ubicaciones")

        print("\n" + "="*70)
        print("✓ PROCESO COMPLETADO")
        print("="*70 + "\n")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


if __name__ == "__main__":
    main()

