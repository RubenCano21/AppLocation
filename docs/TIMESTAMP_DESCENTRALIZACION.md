# Descentralización del Timestamp - Documentación

## 📋 Resumen

Se implementó la descentralización del campo `timestamp` para separar la fecha y la hora en campos independientes, permitiendo análisis dimensional del tiempo.

## 🎯 Objetivo

Transformar los datos de ubicación de Supabase (producción) sin modificar la BD de origen, extrayendo y clasificando información temporal para facilitar análisis por periodos del día.

## 🔄 Flujo ETL

### 1. **Extracción** (Supabase - Producción)
- Se conecta via API REST a Supabase
- Extrae datos de la tabla de ubicaciones
- **NO modifica** nada en producción

### 2. **Transformación** (PySpark)
El campo `timestamp` se descentraliza en:

| Campo Nuevo | Tipo | Descripción | Ejemplo |
|------------|------|-------------|---------|
| `date` | Date | Fecha sin hora | `2025-12-03` |
| `hour_value` | Integer | Hora del día (0-23) | `14` |
| `time_period` | String | Periodo del día | `TARDE` |
| `time_period_code` | String | Código corto del periodo | `AFT` |
| `time_id` | Integer | FK a dim_time | `14` |

#### Clasificación de Periodos

```python
MAÑANA (MOR):  06:00 - 11:59
TARDE  (AFT):  12:00 - 18:59
NOCHE  (NIG):  19:00 - 05:59
```

### 3. **Carga** (PostgreSQL - Destino)
- Los datos transformados se cargan en PostgreSQL
- La tabla `locations` incluye todos los campos nuevos
- Se crean índices para optimizar consultas por tiempo

## 📁 Archivos Modificados

### 1. `app/spark/transformations.py`
```python
# Nuevas transformaciones agregadas:
- Conversión de timestamp a formato datetime
- Extracción de fecha (date)
- Extracción de hora (hour_value)
- Clasificación en periodos (time_period)
- Generación de códigos (time_period_code)
- Creación de time_id para FK
```

### 2. `app/models/db_models.py`
```python
# Campos agregados a la clase Location:
date = Column(DateTime, index=True)
hour_value = Column(Integer)
time_period = Column(String(20))
time_period_code = Column(String(10))
time_id = Column(Integer, index=True)
```

### 3. `app/services/etl_service.py`
```python
# Actualizado el select para incluir nuevos campos:
.select(
    ...,
    "date", "hour_value", "time_period", 
    "time_period_code", "time_id",
    ...
)
```

## 🗃️ Esquema de Base de Datos

### Tabla: dim_time (Dimensión)
```sql
CREATE TABLE dim_time (
    id INTEGER PRIMARY KEY,        -- 0-23
    hour INTEGER NOT NULL,
    period VARCHAR(20),            -- MAÑANA, TARDE, NOCHE
    period_code VARCHAR(10)        -- MOR, AFT, NIG
);
```

### Tabla: locations (Hechos)
```sql
ALTER TABLE locations ADD COLUMN date TIMESTAMP;
ALTER TABLE locations ADD COLUMN hour_value INTEGER;
ALTER TABLE locations ADD COLUMN time_period VARCHAR(20);
ALTER TABLE locations ADD COLUMN time_period_code VARCHAR(10);
ALTER TABLE locations ADD COLUMN time_id INTEGER;

CREATE INDEX idx_locations_date ON locations(date);
CREATE INDEX idx_locations_time_id ON locations(time_id);
```

## 🚀 Uso

### Ejecutar el ETL Completo
```python
from app.services.etl_service import ETLService

etl = ETLService()
result = await etl.run_full_etl()
```

### Poblar Dimensión de Tiempo
```python
from app.services.dimension_service import DimensionService
from app.database.postgres_db import get_db

db = next(get_db())
DimensionService.populate_dim_time(db)
```

### Ejecutar Migración SQL
```bash
psql -h localhost -U postgres -d location_db -f migrations/add_timestamp_fields.sql
```

## 🧪 Pruebas

### Ejecutar Test de Transformación
```bash
python tests/test_timestamp_transformation.py
```

Este test valida:
- ✅ Extracción correcta de fecha
- ✅ Extracción correcta de hora (0-23)
- ✅ Clasificación correcta en MAÑANA/TARDE/NOCHE
- ✅ Generación correcta de códigos MOR/AFT/NIG
- ✅ Creación correcta de time_id

## 📊 Consultas de Ejemplo

### Análisis por Periodo del Día
```sql
SELECT 
    time_period,
    COUNT(*) as total_locations,
    AVG(battery) as avg_battery,
    AVG(signal) as avg_signal
FROM locations
GROUP BY time_period
ORDER BY time_period;
```

### Análisis por Hora del Día
```sql
SELECT 
    hour_value,
    time_period,
    COUNT(*) as registros,
    COUNT(DISTINCT device_id) as dispositivos_unicos
FROM locations
GROUP BY hour_value, time_period
ORDER BY hour_value;
```

### Join con Dimensión de Tiempo
```sql
SELECT 
    l.date,
    t.hour,
    t.period,
    COUNT(*) as registros
FROM locations l
JOIN dim_time t ON l.time_id = t.id
GROUP BY l.date, t.hour, t.period
ORDER BY l.date, t.hour;
```

## 🔍 Verificación

### Datos Originales en Supabase
```
timestamp: "2025-12-03 14:30:00"
```

### Datos Transformados en PostgreSQL
```
timestamp: "2025-12-03 14:30:00"
date: "2025-12-03"
hour_value: 14
time_period: "TARDE"
time_period_code: "AFT"
time_id: 14
```

## ⚠️ Notas Importantes

1. **No Modificación de Producción**: La BD de Supabase NO se modifica
2. **Datos Históricos**: Si ya tienes datos en PostgreSQL, usa el UPDATE en `migrations/add_timestamp_fields.sql`
3. **Timezone**: Los timestamps se procesan en UTC por defecto
4. **Índices**: Se crean automáticamente para optimizar consultas

## 📈 Beneficios

✅ **Análisis temporal más eficiente**: Consultas por periodo del día  
✅ **Modelo dimensional**: Integración con dim_time  
✅ **Performance**: Índices optimizados para queries temporales  
✅ **Flexibilidad**: Mantiene timestamp original + campos descentralizados  
✅ **Sin impacto en producción**: Transformación solo en destino  

## 🔗 Próximos Pasos

1. Ejecutar migración SQL en PostgreSQL destino
2. Poblar dim_time con `DimensionService.populate_dim_time()`
3. Ejecutar ETL para cargar datos con nuevos campos
4. Validar con consultas de ejemplo
5. Crear dashboards por periodo del día

---
**Fecha**: 2025-12-03  
**Versión**: 1.0  
**Autor**: Sistema ETL AppLocation

