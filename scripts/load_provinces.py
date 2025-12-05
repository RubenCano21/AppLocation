"""
Carga de provincias de Santa Cruz desde GeoJSON
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from geoalchemy2.shape import from_shape
from shapely.geometry import shape, MultiPolygon, Polygon
from app.database.postgres_db import SessionLocal, init_db
from app.models.db_models import Province


def calculate_area_perimeter(geom):
    try:
        area_km2 = geom.area * 111 * 106
        perimeter_km = geom.length * 108.5
        return round(area_km2, 2), round(perimeter_km, 2)
    except:
        return None, None


def load_provinces():
    geojson_path = Path(__file__).parent.parent / "santa_cruz_provincias.geojson"

    if not geojson_path.exists():
        geojson_path = Path("D:/UAGRM/SOPORTE/santa_cruz_provincias.geojson")

    if not geojson_path.exists():
        print(f"ERROR: Archivo no encontrado")
        return False

    print(f"Cargando: {geojson_path.name}")

    with open(geojson_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    features = data.get('features', [])
    print(f"Features: {len(features)}")

    init_db()
    db = SessionLocal()

    try:
        deleted = db.query(Province).delete()
        db.commit()
        print(f"Registros eliminados: {deleted}")

        count = 0
        for idx, feature in enumerate(features):
            try:
                props = feature.get('properties', {})
                geometry = feature.get('geometry')

                if not geometry:
                    continue

                # Detectar campos
                province_name = (
                    props.get('provincia') or
                    props.get('PROVINCIA') or
                    props.get('nombre') or
                    props.get('NOMBRE') or
                    props.get('name') or
                    f"Provincia {idx+1}"
                )

                municipality = (
                    props.get('municipio') or
                    props.get('MUNICIPIO') or
                    props.get('municipality')
                )

                department = (
                    props.get('departamen') or
                    props.get('DEPARTAMEN') or
                    props.get('departamento') or
                    'Santa Cruz'
                )

                geom_shape = shape(geometry)

                if isinstance(geom_shape, Polygon):
                    geom_shape = MultiPolygon([geom_shape])
                elif not isinstance(geom_shape, MultiPolygon):
                    continue

                if not geom_shape.is_valid:
                    geom_shape = geom_shape.buffer(0)

                area_km2, perimeter_km = calculate_area_perimeter(geom_shape)

                province = Province(
                    province_name=province_name,
                    municipality=municipality,
                    department=department,
                    geometry=from_shape(geom_shape, srid=4326),
                    area_km2=area_km2,
                    perimeter_km=perimeter_km
                )

                db.add(province)
                count += 1
                print(f"✓ {province_name}: {area_km2} km²")

            except Exception as e:
                print(f"✗ Error en feature {idx}: {e}")
                continue

        db.commit()
        print(f"\n✓ {count} provincias cargadas")

        provinces = db.query(Province).all()
        total_area = sum(p.area_km2 for p in provinces if p.area_km2)
        print(f"Área total: {total_area:.2f} km²")

        return True

    except Exception as e:
        print(f"ERROR: {e}")
        db.rollback()
        return False
    finally:
        db.close()


if __name__ == "__main__":
    load_provinces()

