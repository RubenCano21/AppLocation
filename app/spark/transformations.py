# app/spark/transformations.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, concat, avg, count,
    round as spark_round, floor, countDistinct
)
import logging

logger = logging.getLogger(__name__)


def transform_locations(df: DataFrame) -> DataFrame:
    """
    Aplica todas las transformaciones necesarias a los datos de ubicación
    Campos de entrada: id, device_name, device_id, latitude, longitude,
                       altitude, speed, battery, signal, sim_operator,
                       network_type, timestamp
    """
    logger.info("Starting data transformation...")

    # 1. Filtrar coordenadas inválidas
    df = df.filter(
        (col("latitude").isNotNull()) &
        (col("longitude").isNotNull()) &
        (col("latitude").between(-90, 90)) &
        (col("longitude").between(-180, 180))
    )

    # 2. Normalizar tipo de red
    df = df.withColumn(
        "network_type_normalized",
        when(col("network_type").isNull(), "unknown")
        .when(col("network_type").rlike("(?i)5G"), "5G")
        .when(col("network_type").rlike("(?i)4G|LTE"), "4G")
        .when(col("network_type").rlike("(?i)3G|HSDPA|HSPA|UMTS"), "3G")
        .when(col("network_type").rlike("(?i)2G|EDGE|GPRS"), "2G")
        .otherwise("unknown")
    )

    # 3. Clasificar estado de batería
    df = df.withColumn(
        "battery_status",
        when(col("battery").isNull(), "unknown")
        .when(col("battery") >= 75, "high")
        .when(col("battery") >= 50, "medium")
        .when(col("battery") >= 25, "low")
        .otherwise("critical")
    )

    # 4. Clasificar calidad de señal (asumiendo rango típico -120 a -40 dBm)
    # Si tu campo signal tiene otro rango, ajusta estos valores
    df = df.withColumn(
        "signal_quality",
        when(col("signal").isNull(), "unknown")
        .when(col("signal") >= -60, "excellent")
        .when(col("signal") >= -70, "good")
        .when(col("signal") >= -80, "fair")
        .otherwise("poor")
    )

    # 5. Crear geometría WKT para PostGIS (POINT)
    df = df.withColumn(
        "location",
        concat(
            lit("SRID=4326;POINT("),
            col("longitude"),
            lit(" "),
            col("latitude"),
            lit(")")
        )
    )

    # 6. Redondear coordenadas para mejor precisión (6 decimales ~ 10cm)
    df = df.withColumn("latitude", spark_round(col("latitude"), 6))
    df = df.withColumn("longitude", spark_round(col("longitude"), 6))

    # 7. Manejar valores nulos
    df = df.withColumn(
        "altitude",
        when(col("altitude").isNull(), 0.0).otherwise(col("altitude"))
    )
    df = df.withColumn(
        "speed",
        when(col("speed").isNull(), 0.0).otherwise(col("speed"))
    )

    # 8. Agregar columnas de grilla para análisis espacial (0.01 grados ~ 1km)
    df = df.withColumn("lat_grid", floor(col("latitude") / 0.01) * 0.01)
    df = df.withColumn("lon_grid", floor(col("longitude") / 0.01) * 0.01)

    # 9. Limpiar nombres de operadores (trimming y normalización)
    df = df.withColumn(
        "sim_operator",
        when(col("sim_operator").isNull(), "unknown")
        .otherwise(col("sim_operator"))
    )

    record_count = df.count()
    logger.info(f"Transformation completed. Records: {record_count}")

    return df


def aggregate_by_grid(df: DataFrame, grid_size: float = 0.01) -> DataFrame:
    """
    Agrega datos por celdas de grilla (heatmap)
    grid_size: tamaño de celda en grados (~1km = 0.01)
    """
    logger.info(f"Aggregating by grid with size {grid_size}...")

    # Crear grilla
    df_grid = df.withColumn(
        "lat_grid",
        floor(col("latitude") / grid_size) * grid_size
    )
    df_grid = df_grid.withColumn(
        "lon_grid",
        floor(col("longitude") / grid_size) * grid_size
    )

    # Agregar por celda
    aggregated = df_grid.groupBy("lat_grid", "lon_grid").agg(
        count("*").alias("point_count"),
        countDistinct("device_id").alias("unique_devices"),
        avg("battery").alias("avg_battery"),
        avg("signal").alias("avg_signal"),
        avg("altitude").alias("avg_altitude"),
        avg("speed").alias("avg_speed")
    )

    # Crear polígono de la celda en formato WKT
    aggregated = aggregated.withColumn(
        "cell_polygon",
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

    # Agregar grid_size como columna
    aggregated = aggregated.withColumn("grid_size", lit(grid_size))

    # Redondear valores
    aggregated = aggregated.withColumn("avg_battery", spark_round(col("avg_battery"), 2))
    aggregated = aggregated.withColumn("avg_signal", spark_round(col("avg_signal"), 2))
    aggregated = aggregated.withColumn("avg_altitude", spark_round(col("avg_altitude"), 2))
    aggregated = aggregated.withColumn("avg_speed", spark_round(col("avg_speed"), 2))

    grid_count = aggregated.count()
    logger.info(f"Grid aggregation completed. Grid cells: {grid_count}")

    return aggregated


def aggregate_by_device(df: DataFrame) -> DataFrame:
    """
    Agrega estadísticas por dispositivo
    """
    logger.info("Aggregating statistics by device...")

    from pyspark.sql.functions import min as spark_min, max as spark_max

    device_stats = df.groupBy("device_id", "device_name").agg(
        count("*").alias("total_records"),
        spark_min("timestamp").alias("first_seen"),
        spark_max("timestamp").alias("last_seen"),
        avg("battery").alias("avg_battery"),
        avg("signal").alias("avg_signal"),
        avg("speed").alias("avg_speed"),
        avg("latitude").alias("most_common_lat"),
        avg("longitude").alias("most_common_lon")
    )

    # Redondear valores
    device_stats = device_stats.withColumn("avg_battery", spark_round(col("avg_battery"), 2))
    device_stats = device_stats.withColumn("avg_signal", spark_round(col("avg_signal"), 2))
    device_stats = device_stats.withColumn("avg_speed", spark_round(col("avg_speed"), 2))
    device_stats = device_stats.withColumn("most_common_lat", spark_round(col("most_common_lat"), 6))
    device_stats = device_stats.withColumn("most_common_lon", spark_round(col("most_common_lon"), 6))

    return device_stats


def filter_by_area(df: DataFrame, min_lat: float, max_lat: float,
                   min_lon: float, max_lon: float) -> DataFrame:
    """
    Filtra puntos dentro de un área rectangular
    """
    return df.filter(
        (col("latitude").between(min_lat, max_lat)) &
        (col("longitude").between(min_lon, max_lon))
    )


def filter_by_network(df: DataFrame, network_type: str) -> DataFrame:
    """
    Filtra por tipo de red normalizado
    """
    return df.filter(col("network_type_normalized") == network_type)


def filter_by_operator(df: DataFrame, operator: str) -> DataFrame:
    """
    Filtra por operador
    """
    return df.filter(col("sim_operator") == operator)


def calculate_statistics(df: DataFrame) -> dict:
    """
    Calcula estadísticas generales del dataset
    """
    from pyspark.sql.functions import min as spark_min, max as spark_max

    stats = df.agg(
        count("*").alias("total_count"),
        countDistinct("device_id").alias("unique_devices"),
        avg("battery").alias("avg_battery"),
        avg("signal").alias("avg_signal"),
        avg("speed").alias("avg_speed"),
        spark_min("timestamp").alias("min_date"),
        spark_max("timestamp").alias("max_date")
    ).collect()[0]

    # Distribución de tipos de red
    network_dist = df.groupBy("network_type_normalized") \
        .count() \
        .orderBy(col("count").desc()) \
        .collect()

    # Distribución de operadores
    operator_dist = df.groupBy("sim_operator") \
        .count() \
        .orderBy(col("count").desc()) \
        .collect()

    return {
        "total_points": stats["total_count"],
        "unique_devices": stats["unique_devices"],
        "avg_battery": round(stats["avg_battery"], 2) if stats["avg_battery"] else 0,
        "avg_signal": round(stats["avg_signal"], 2) if stats["avg_signal"] else 0,
        "avg_speed": round(stats["avg_speed"], 2) if stats["avg_speed"] else 0,
        "date_range": {
            "start": str(stats["min_date"]) if stats["min_date"] else None,
            "end": str(stats["max_date"]) if stats["max_date"] else None
        },
        "network_distribution": [
            {"network": row["network_type_normalized"], "count": row["count"]}
            for row in network_dist
        ],
        "operator_distribution": [
            {"operator": row["sim_operator"], "count": row["count"]}
            for row in operator_dist
        ]
    }