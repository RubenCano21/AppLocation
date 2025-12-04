-- Agregar columna speed_range a la tabla locations
-- Fecha: 2025-12-04

ALTER TABLE locations
ADD COLUMN IF NOT EXISTS speed_range VARCHAR(30);

-- Agregar índice para optimizar consultas por rango de velocidad
CREATE INDEX IF NOT EXISTS idx_speed_range ON locations(speed_range);

-- Comentario de la columna
COMMENT ON COLUMN locations.speed_range IS 'Clasificación de velocidad: DETENIDO, CAMINANDO, BICICLETA, VEHÍCULO LENTO, VEHÍCULO MODERADO, VEHÍCULO RÁPIDO';

