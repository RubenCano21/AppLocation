-- Migración: Agregar campos descentralizados del timestamp
-- Descripción: Separa el timestamp en date, hour_value, time_period para análisis dimensional
-- Fecha: 2025-12-03

-- Agregar columnas a la tabla locations
ALTER TABLE locations ADD COLUMN IF NOT EXISTS date TIMESTAMP;
ALTER TABLE locations ADD COLUMN IF NOT EXISTS hour_value INTEGER;
ALTER TABLE locations ADD COLUMN IF NOT EXISTS time_period VARCHAR(20);
ALTER TABLE locations ADD COLUMN IF NOT EXISTS time_period_code VARCHAR(10);
ALTER TABLE locations ADD COLUMN IF NOT EXISTS time_id INTEGER;

-- Crear índices para mejorar el rendimiento de consultas
CREATE INDEX IF NOT EXISTS idx_locations_date ON locations(date);
CREATE INDEX IF NOT EXISTS idx_locations_time_id ON locations(time_id);
CREATE INDEX IF NOT EXISTS idx_locations_time_period ON locations(time_period);

-- Agregar comentarios descriptivos
COMMENT ON COLUMN locations.date IS 'Fecha sin hora extraída del timestamp';
COMMENT ON COLUMN locations.hour_value IS 'Hora del día (0-23) extraída del timestamp';
COMMENT ON COLUMN locations.time_period IS 'Periodo del día: MAÑANA, TARDE, NOCHE';
COMMENT ON COLUMN locations.time_period_code IS 'Código del periodo: MOR, AFT, NIG';
COMMENT ON COLUMN locations.time_id IS 'Foreign key a dim_time.id (igual a hour_value)';

-- Si ya tienes datos existentes, puedes poblarlos con:
-- UPDATE locations
-- SET
--     date = DATE(timestamp),
--     hour_value = EXTRACT(HOUR FROM timestamp),
--     time_period = CASE
--         WHEN EXTRACT(HOUR FROM timestamp) >= 6 AND EXTRACT(HOUR FROM timestamp) < 12 THEN 'MAÑANA'
--         WHEN EXTRACT(HOUR FROM timestamp) >= 12 AND EXTRACT(HOUR FROM timestamp) < 19 THEN 'TARDE'
--         ELSE 'NOCHE'
--     END,
--     time_period_code = CASE
--         WHEN EXTRACT(HOUR FROM timestamp) >= 6 AND EXTRACT(HOUR FROM timestamp) < 12 THEN 'MOR'
--         WHEN EXTRACT(HOUR FROM timestamp) >= 12 AND EXTRACT(HOUR FROM timestamp) < 19 THEN 'AFT'
--         ELSE 'NIG'
--     END,
--     time_id = EXTRACT(HOUR FROM timestamp)
-- WHERE date IS NULL;

