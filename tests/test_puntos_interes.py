"""
Test de puntos de interés en Santa Cruz - Verificar en qué distrito están
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database.postgres_db import SessionLocal
from app.services.district_service import DistrictService


def test_puntos_interes():
    print("\n" + "="*70)
    print("TEST: Puntos de Interés de Santa Cruz por Distrito")
    print("="*70)

    db = SessionLocal()

    # Puntos de interés conocidos de Santa Cruz
    puntos_interes = [
        # Centro
        (-17.7833, -63.1821, "Plaza 24 de Septiembre"),
        (-17.7863, -63.1812, "Catedral Metropolitana"),
        (-17.7831, -63.1808, "Casa de la Cultura"),
        (-17.7840, -63.1790, "Prefectura"),

        # Zona Norte
        (-17.7500, -63.1800, "Zona Norte - Las Brisas"),
        (-17.7300, -63.1700, "Zona Norte - Equipetrol"),

        # Zona Sur
        (-17.8200, -63.1800, "Zona Sur - Plan 3000"),
        (-17.8400, -63.1700, "Zona Sur - Urubó"),

        # Zona Este
        (-17.7800, -63.1500, "Zona Este - Cristo Redentor"),
        (-17.7900, -63.1400, "Zona Este - Los Pozos"),

        # Zona Oeste
        (-17.7800, -63.2200, "Zona Oeste - Pampa de la Isla"),
        (-17.8000, -63.2400, "Zona Oeste - Doble Vía La Guardia"),

        # Aeropuerto
        (-17.6448, -63.1358, "Aeropuerto Viru Viru"),

        # Universidad
        (-17.7782, -63.2021, "UAGRM - Universidad"),

        # Centros Comerciales
        (-17.7854, -63.1822, "Ventura Mall"),
        (-17.7750, -63.1900, "Las Brisas Mall"),
    ]

    print("\nVerificando ubicación de puntos de interés:\n")

    resultados = {}

    for lat, lon, nombre in puntos_interes:
        district = DistrictService.get_district_by_point(db, lat, lon)

        if district:
            distrito_nombre = district.district_name
            if distrito_nombre not in resultados:
                resultados[distrito_nombre] = []
            resultados[distrito_nombre].append(nombre)

            print(f"✓ {nombre}")
            print(f"  Coordenadas: ({lat}, {lon})")
            print(f"  Distrito: {distrito_nombre} ({district.area_km2} km²)")
        else:
            if "Fuera de distritos" not in resultados:
                resultados["Fuera de distritos"] = []
            resultados["Fuera de distritos"].append(nombre)

            print(f"✗ {nombre}")
            print(f"  Coordenadas: ({lat}, {lon})")
            print(f"  Distrito: Fuera de los límites")
        print()

    # Resumen por distrito
    print("="*70)
    print("RESUMEN POR DISTRITO")
    print("="*70)

    for distrito in sorted(resultados.keys()):
        lugares = resultados[distrito]
        print(f"\n{distrito} ({len(lugares)} lugares):")
        for lugar in lugares:
            print(f"  • {lugar}")

    db.close()

    print("\n✓ Test completado\n")


if __name__ == "__main__":
    test_puntos_interes()

