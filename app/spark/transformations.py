# app/spark/transformations.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, concat, avg, count,
    round as spark_round, floor, countDistinct,
    hour, to_date, udf
)
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
import logging

logger = logging.getLogger(__name__)


def get_districts_and_provinces_data(spark, postgres_url, postgres_user, postgres_password):
    """
    Carga datos de distritos y provincias desde PostgreSQL para hacer join

    Returns:
        tuple: (districts_df, provinces_df)
    """
    try:
        # Cargar distritos con sus geometrías como WKT
        districts_df = spark.read \
            .format("jdbc") \
            .option("url", postgres_url) \
            .option("dbtable", "(SELECT id, district_number, district_name, ST_AsText(geometry) as geom_wkt FROM districts) as districts_data") \
            .option("user", postgres_user) \
            .option("password", postgres_password) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        # Cargar provincias
        provinces_df = spark.read \
            .format("jdbc") \
            .option("url", postgres_url) \
            .option("dbtable", "(SELECT id, province_name, ST_AsText(geometry) as geom_wkt FROM provinces) as provinces_data") \
            .option("user", postgres_user) \
            .option("password", postgres_password) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        logger.info(f"✓ Loaded {districts_df.count()} districts and {provinces_df.count()} provinces for spatial join")

        return districts_df, provinces_df

    except Exception as e:
        logger.error(f"Error loading geographic data: {e}")
        return None, None


def assign_district_udf(districts_broadcast):
    """
    UDF para asignar distrito basado en coordenadas
    Usa búsqueda simple por distancia aproximada
    """
    def find_district(lat, lon):
        if lat is None or lon is None:
            return None, None

        districts = districts_broadcast.value
        # Buscar distrito más cercano (aproximación simple)
        # En producción, se debería usar ST_Contains en PostgreSQL
        min_dist = float('inf')
        closest_district = None

        for district in districts:
            # Simplificación: usar distancia euclidiana
            # En realidad necesitaríamos parsear la geometría WKT
            # Por ahora retornamos None para que se haga en PostgreSQL
            pass

        return None, None

    return udf(find_district, StructType([
        StructField("district_id", IntegerType(), True),
        StructField("district_name", StringType(), True)
    ]))


def normalize_operator_udf():
    """UDF para normalizar nombres de operadores"""
    def normalize(operator):
        if operator is None or operator == '':
            return 'SIN SEÑAL'

        operator = operator.strip()
        operator_upper = operator.upper()

        # Unknown, Sin señal -> SIN SEÑAL
        if operator_upper in ['UNKNOWN', 'SIN SEÑAL', 'SIN SEAL', 'N/A', 'NA']:
            return 'SIN SEÑAL'

        # Normalizar ENTEL y sus variantes
        if any(x in operator_upper for x in ['ENTEL', 'BOMOV', '+18VACUNATE', 'LADISTANCIANOSCUIDA', 'MOVIL GSM', 'T-MOBILE']):
            return 'ENTEL'

        # TIGO
        if 'TIGO' in operator_upper:
            return 'TIGO'

        # VIVA
        if 'VIVA' in operator_upper:
            return 'VIVA'

        # Sin señal (variantes)
        if 'SIN' in operator_upper or 'SEAL' in operator_upper:
            return 'SIN SEÑAL'

        # Si no coincide con ninguno, devolver en mayúsculas
        return operator_upper

    return udf(normalize, StringType())


def normalize_network_type_udf():
    """UDF para normalizar tipos de red"""
    def normalize(network_type):
        if network_type is None or network_type == '':
            return 'SIN DATOS'

        network = network_type.strip()
        network_upper = network.upper()

        # WiFi
        if 'WIFI' in network_upper or 'WI-FI' in network_upper:
            return 'WiFi'

        # 5G
        if '5G' in network_upper:
            return '5G'

        # 4G/LTE/Mobile -> 4G
        if '4G' in network_upper or 'LTE' in network_upper or ('MOBILE' in network_upper and len(network_upper) < 15):
            return '4G'

        # 3G
        if '3G' in network_upper or 'HSDPA' in network_upper or 'HSPA' in network_upper or 'UMTS' in network_upper or 'WCDMA' in network_upper:
            return '3G'

        # 2G
        if '2G' in network_upper or 'EDGE' in network_upper or 'GPRS' in network_upper or 'GSM' in network_upper:
            return '2G'

        # Si no coincide con ninguno, devolver SIN DATOS
        return 'SIN DATOS'

    return udf(normalize, StringType())


def classify_speed_udf():
    """UDF para clasificar velocidad en rangos"""
    def classify(speed):
        if speed is None:
            return 'DETENIDO'

        try:
            speed_val = float(speed)
            # Convertir de m/s a km/h para clasificar
            speed_kmh = speed_val * 3.6

            if speed_kmh == 0:
                return 'DETENIDO'
            elif speed_kmh <= 5:  # 0.1 - 5 km/h
                return 'CAMINANDO'
            elif speed_kmh <= 10:  # 5 - 10 km/h
                return 'CORRIENDO'
            elif speed_kmh <= 60:  # 10 - 60 km/h
                return 'TRANSPORTE PÚBLICO'
            else:  # > 60 km/h
                return 'VEHÍCULO'
        except (ValueError, TypeError):
            return 'DETENIDO'

    return udf(classify, StringType())


def transform_locations(df: DataFrame) -> DataFrame:
    """
    Transforma datos de Supabase de forma optimizada
    - Mantiene latitud/longitud originales
    - Clasifica hora en período del día (MAÑANA/TARDE/NOCHE)
    - Clasifica altitud en rangos (BAJA/MEDIA/ALTA)
    - Clasifica batería en niveles (CRITICO/BAJO/MEDIO/ALTO)
    """
    logger.info("Starting optimized data transformation...")

    # 1. Filtrar coordenadas inválidas
    df = df.filter(
        (col("latitude").isNotNull()) &
        (col("longitude").isNotNull()) &
        (col("latitude").between(-90, 90)) &
        (col("longitude").between(-180, 180))
    )

    initial_count = df.count()
    logger.info(f"Valid coordinates: {initial_count:,}")

    # 2. Clasificar PERÍODO DEL DÍA (solo este campo, sin hour separado)
    df = df.withColumn(
        "period",
        when((hour(col("timestamp")) >= 6) & (hour(col("timestamp")) < 12), "MAÑANA")
        .when((hour(col("timestamp")) >= 12) & (hour(col("timestamp")) < 19), "TARDE")
        .otherwise("NOCHE")
    )

    # 3. Clasificar ALTITUD (solo este campo, sin altitude original duplicado)
    df = df.withColumn(
        "altitude_range",
        when(col("altitude").isNull(), None)
        .when(col("altitude") <= 400, "BAJA")
        .when(col("altitude") <= 500, "MEDIA")
        .otherwise("ALTA")
    )

    # 4. Clasificar BATERÍA (solo este campo)
    df = df.withColumn(
        "battery_level",
        when(col("battery").isNull(), None)
        .when(col("battery") < 25, "CRITICO")
        .when(col("battery") < 50, "BAJO")
        .when(col("battery") < 75, "MEDIO")
        .otherwise("ALTO")
    )

    # 5. Normalizar tipo de red (usando when/otherwise en lugar de UDF)
    df = df.withColumn(
        "network_type",
        when(col("network_type").isNull(), "SIN DATOS")
        .when(col("network_type").rlike("(?i)wifi|wi-fi"), "WiFi")
        .when(col("network_type").rlike("(?i)5G"), "5G")
        .when(col("network_type").rlike("(?i)4G|LTE|mobile"), "4G")
        .when(col("network_type").rlike("(?i)3G|HSDPA|HSPA|UMTS|WCDMA"), "3G")
        .when(col("network_type").rlike("(?i)2G|EDGE|GPRS|GSM"), "2G")
        .otherwise("SIN DATOS")
    )

    # Crear network_generation basado en network_type normalizado
    df = df.withColumn("network_generation", col("network_type"))

    # 6. Clasificar calidad de señal (solo este campo)
    df = df.withColumn(
        "signal_quality",
        when(col("signal").isNull(), None)
        .when(col("signal") >= -60, "EXCELENTE")
        .when(col("signal") >= -70, "BUENA")
        .when(col("signal") >= -80, "REGULAR")
        .otherwise("POBRE")
    )

    # 7. Crear geometría WKT para PostGIS
    df = df.withColumn(
        "location_geom",
        concat(
            lit("SRID=4326;POINT("),
            col("longitude"),
            lit(" "),
            col("latitude"),
            lit(")")
        )
    )

    # 8. Extraer solo la fecha (sin hora)
    df = df.withColumn("date", to_date(col("timestamp")))

    # 9. Manejar valores nulos en campos numéricos
    df = df.withColumn("altitude", when(col("altitude").isNull(), 0.0).otherwise(col("altitude")))
    df = df.withColumn("speed", when(col("speed").isNull(), 0.0).otherwise(col("speed")))
    df = df.withColumn("battery", when(col("battery").isNull(), 0.0).otherwise(col("battery")))
    df = df.withColumn("signal", when(col("signal").isNull(), 0.0).otherwise(col("signal")))

    # 10. Normalizar y limpiar operador (usando when/otherwise en lugar de UDF)
    df = df.withColumn(
        "sim_operator",
        when(col("sim_operator").isNull(), "SIN SEÑAL")
        .when(col("sim_operator") == "", "SIN SEÑAL")
        .when(col("sim_operator").rlike("(?i)unknown|sin señ|sin seal|n/a"), "SIN SEÑAL")
        .when(col("sim_operator").rlike("(?i)entel|bomov|18vacunate|distancia|movil gsm|t-mobile"), "ENTEL")
        .when(col("sim_operator").rlike("(?i)tigo"), "TIGO")
        .when(col("sim_operator").rlike("(?i)viva"), "VIVA")
        .otherwise(col("sim_operator"))
    )

    # 11. Clasificar velocidad en rangos (usando when/otherwise en lugar de UDF)
    # Convertir speed de m/s a km/h y clasificar
    df = df.withColumn(
        "speed_range",
        when((col("speed").isNull()) | (col("speed") == 0), "DETENIDO")
        .when(col("speed") * 3.6 <= 5, "CAMINANDO")
        .when(col("speed") * 3.6 <= 10, "CORRIENDO")
        .when(col("speed") * 3.6 <= 60, "TRANSPORTE PÚBLICO")
        .otherwise("VEHÍCULO")
    )

    # 12. Crear grilla para análisis espacial (para heatmap)
    df = df.withColumn("lat_grid", floor(col("latitude") / 0.01) * 0.01)
    df = df.withColumn("lon_grid", floor(col("longitude") / 0.01) * 0.01)

    # 13. Agregar columnas de ubicación geográfica (inicialmente NULL)
    # Estas se llenarán después en PostgreSQL con consultas espaciales
    df = df.withColumn("district_id", lit(None).cast(IntegerType()))
    df = df.withColumn("district_name", lit(None).cast(StringType()))
    df = df.withColumn("province_id", lit(None).cast(IntegerType()))
    df = df.withColumn("province_name", lit(None).cast(StringType()))

    final_count = df.count()
    logger.info(f"✓ Transformation completed")
    logger.info(f"  Input records: {initial_count:,}")
    logger.info(f"  Output records: {final_count:,}")
    logger.info(f"  Filtered out: {initial_count - final_count:,}")

    return df


def aggregate_by_grid(df: DataFrame, grid_size: float = 0.01) -> DataFrame:
    """
    Agrega datos por celdas de grilla para heatmap
    grid_size: 0.01 grados ≈ 1km
    """
    logger.info(f"Creating grid aggregation (grid_size={grid_size})...")

    # Crear grilla
    df_grid = df.withColumn("lat_grid", floor(col("latitude") / grid_size) * grid_size)
    df_grid = df_grid.withColumn("lon_grid", floor(col("longitude") / grid_size) * grid_size)

    # Agregar por celda
    aggregated = df_grid.groupBy("lat_grid", "lon_grid").agg(
        count("*").alias("point_count"),
        countDistinct("device_id").alias("unique_devices"),
        avg("battery").alias("avg_battery"),
        avg("signal").alias("avg_signal"),
        avg("altitude").alias("avg_altitude"),
        avg("speed").alias("avg_speed")
    )

    # Redondear valores
    aggregated = aggregated.withColumn("avg_battery", spark_round(col("avg_battery"), 2))
    aggregated = aggregated.withColumn("avg_signal", spark_round(col("avg_signal"), 2))
    aggregated = aggregated.withColumn("avg_altitude", spark_round(col("avg_altitude"), 2))
    aggregated = aggregated.withColumn("avg_speed", spark_round(col("avg_speed"), 2))

    # Crear polígono WKT de la celda
    aggregated = aggregated.withColumn(
        "cell_geom",
        concat(
            lit("SRID=4326;POLYGON(("),
            col("lon_grid"), lit(" "), col("lat_grid"), lit(","),
            col("lon_grid") + grid_size, lit(" "), col("lat_grid"), lit(","),
            col("lon_grid") + grid_size, lit(" "), col("lat_grid") + grid_size, lit(","),
            col("lon_grid"), lit(" "), col("lat_grid") + grid_size, lit(","),
            col("lon_grid"), lit(" "), col("lat_grid"),
            lit("))")
        )
    )

    aggregated = aggregated.withColumn("grid_size", lit(grid_size))

    grid_count = aggregated.count()
    logger.info(f"✓ Grid created with {grid_count:,} cells")

    return aggregated


def get_statistics(df: DataFrame) -> dict:
    """
    Calcula estadísticas generales del dataset
    """
    from pyspark.sql.functions import min as spark_min, max as spark_max

    # Estadísticas generales
    stats = df.agg(
        count("*").alias("total"),
        countDistinct("device_id").alias("devices"),
        avg("battery").alias("avg_battery"),
        avg("signal").alias("avg_signal"),
        avg("speed").alias("avg_speed"),
        avg("altitude").alias("avg_altitude"),
        spark_min("timestamp").alias("min_date"),
        spark_max("timestamp").alias("max_date")
    ).collect()[0]

    # Distribución por período del día
    period_dist = df.groupBy("period") \
        .count() \
        .orderBy(
        when(col("period") == "MAÑANA", 1)
        .when(col("period") == "TARDE", 2)
        .otherwise(3)
    ) \
        .collect()

    # Distribución por rango de altitud
    altitude_dist = df.groupBy("altitude_range") \
        .count() \
        .orderBy(
        when(col("altitude_range") == "BAJA", 1)
        .when(col("altitude_range") == "MEDIA", 2)
        .otherwise(3)
    ) \
        .collect()

    # Distribución por nivel de batería
    battery_dist = df.groupBy("battery_level") \
        .count() \
        .orderBy(
        when(col("battery_level") == "CRITICO", 1)
        .when(col("battery_level") == "BAJO", 2)
        .when(col("battery_level") == "MEDIO", 3)
        .otherwise(4)
    ) \
        .collect()

    # Distribución por generación de red
    network_dist = df.groupBy("network_generation") \
        .count() \
        .orderBy(col("count").desc()) \
        .collect()

    # Distribución por operador
    operator_dist = df.groupBy("sim_operator") \
        .count() \
        .orderBy(col("count").desc()) \
        .collect()

    return {
        "total_points": stats["total"],
        "unique_devices": stats["devices"],
        "avg_battery": round(stats["avg_battery"], 2) if stats["avg_battery"] else 0,
        "avg_signal": round(stats["avg_signal"], 2) if stats["avg_signal"] else 0,
        "avg_speed": round(stats["avg_speed"], 2) if stats["avg_speed"] else 0,
        "avg_altitude": round(stats["avg_altitude"], 2) if stats["avg_altitude"] else 0,
        "date_range": {
            "start": str(stats["min_date"]) if stats["min_date"] else None,
            "end": str(stats["max_date"]) if stats["max_date"] else None
        },
        "period_distribution": [
            {"period": row["period"], "count": row["count"]}
            for row in period_dist
        ],
        "altitude_distribution": [
            {"range": row["altitude_range"], "count": row["count"]}
            for row in altitude_dist if row["altitude_range"]
        ],
        "battery_distribution": [
            {"level": row["battery_level"], "count": row["count"]}
            for row in battery_dist if row["battery_level"]
        ],
        "network_distribution": [
            {"generation": row["network_generation"], "count": row["count"]}
            for row in network_dist
        ],
        "operator_distribution": [
            {"operator": row["sim_operator"], "count": row["count"]}
            for row in operator_dist
        ]
    }