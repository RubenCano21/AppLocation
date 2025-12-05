import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.province_service import ProvinceService
from app.database.postgres_db import SessionLocal

db = SessionLocal()
provinces = ProvinceService.get_all_provinces(db)
print(f'\nTotal: {len(provinces)} provincias\n')

for p in provinces:
    print(f'{p.id}. {p.province_name} - {p.area_km2} km²')

province = ProvinceService.get_province_by_point(db, -17.7833, -63.1821)
print(f'\nPlaza 24 de Septiembre está en: {province.province_name if province else "No encontrado"}')

db.close()
print('\n✓ Servicio funcionando correctamente')

