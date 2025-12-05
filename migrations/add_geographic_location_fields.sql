-- Agregar columnas de distrito y provincia a la tabla locations

ALTER TABLE locations ADD COLUMN IF NOT EXISTS district_id INTEGER;
ALTER TABLE locations ADD COLUMN IF NOT EXISTS district_name VARCHAR(100);
ALTER TABLE locations ADD COLUMN IF NOT EXISTS province_id INTEGER;
ALTER TABLE locations ADD COLUMN IF NOT EXISTS province_name VARCHAR(200);

-- Crear foreign keys
ALTER TABLE locations
ADD CONSTRAINT fk_location_district
FOREIGN KEY (district_id) REFERENCES districts(id) ON DELETE SET NULL;

ALTER TABLE locations
ADD CONSTRAINT fk_location_province
FOREIGN KEY (province_id) REFERENCES provinces(id) ON DELETE SET NULL;

-- Crear índices
CREATE INDEX IF NOT EXISTS idx_district_name ON locations(district_name);
CREATE INDEX IF NOT EXISTS idx_province_name ON locations(province_name);

-- Mensaje de confirmación
SELECT 'Columnas agregadas exitosamente' as mensaje;

