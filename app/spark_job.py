# spark_job.py
import json
import os
import sys
from app.db_utils import SPARK_APP_NAME, SPARK_MASTER, DEST_PG_HOST, DEST_PG_PORT, DEST_PG_DB, DEST_PG_USER, DEST_PG_PASSWORD
from app.database.supabase_utils import fetch_all_supabase
from pyspark.sql import SparkSession, functions as F, types as T
import psycopg2
from psycopg2.extras import execute_values

def create_spark():
    # Configurar el ejecutable de Python para PySpark
    # Usar sys.executable para apuntar al Python del virtualenv actual
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable    # Configurar el ejecutable de Python para PySpark`n    # Usar sys.executable para apuntar al Python del virtualenv actual`n    os.environ['PYSPARK_PYTHON'] = sys.executable`n    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable`n
    spark = (SparkSession.builder
             .appName(SPARK_APP_NAME)
             .master(SPARK_MASTER)
             .getOrCreate())
    return spark

def fetch_data():
    print("Fetching data from Supabase REST API...")
    data = fetch_all_supabase()
    print(f"Fetched {len(data)} rows.")
    return data

def json_to_spark_df(spark, json_list):
    """
    Convierte lista de JSON (dicts) a DataFrame Spark.
    """
    if not json_list:
        # schema vacío
        return spark.createDataFrame([], schema=[])
    # crear RDD a partir de los JSON strings para inferir schema correctamente
    rdd = spark.sparkContext.parallelize([json.dumps(r) for r in json_list])
    df = spark.read.json(rdd)
    return df

def transform(df):
    """
    Transformaciones:
      - casteo de tipos
      - filtro de coordenadas válidas
      - normalización de timestamp
      - ejemplo de resumen por device por día
    """
    if df.rdd.isEmpty():
        return df, df

    df2 = df.withColumn("latitude", df["latitude"].cast(T.DoubleType())) \
            .withColumn("longitude", df["longitude"].cast(T.DoubleType())) \
            .withColumn("speed", df["speed"].cast(T.DoubleType())) \
            .withColumn("altitude", df["altitude"].cast(T.DoubleType())) \
            .withColumn("battery", df["battery"].cast(T.DoubleType())) \
            .withColumn("timestamp", F.to_timestamp("timestamp"))

    df_clean = df2.filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())

    summary = df_clean.withColumn("date", F.to_date("timestamp")) \
        .groupBy("device_id", "device_name", "date") \
        .agg(
            F.count("*").alias("points_count"),
            F.avg("speed").alias("avg_speed"),
            F.max("speed").alias("max_speed"),
            F.avg("battery").alias("avg_battery")
        ).orderBy(F.col("date").desc())

    return df_clean, summary

def ensure_tables_created():
    """
    Crea las tablas destino si no existen (simple SQL).
    """
    conn = psycopg2.connect(host=DEST_PG_HOST,
                            port=DEST_PG_PORT,
                            dbname=DEST_PG_DB,
                            user=DEST_PG_USER,
                            password=DEST_PG_PASSWORD)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS public.devices_summary_by_device_day (
      device_id TEXT,
      device_name TEXT,
      date DATE,
      points_count BIGINT,
      avg_speed DOUBLE PRECISION,
      max_speed DOUBLE PRECISION,
      avg_battery DOUBLE PRECISION
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS public.devices_positions_clean (
      id TEXT PRIMARY KEY,
      device_name TEXT,
      device_id TEXT,
      latitude DOUBLE PRECISION,
      longitude DOUBLE PRECISION,
      altitude DOUBLE PRECISION,
      speed DOUBLE PRECISION,
      battery DOUBLE PRECISION,
      signal TEXT,
      sim_operator TEXT,
      network_type TEXT,
      timestamp TIMESTAMP
    );
    """)
    conn.commit()
    cur.close()
    conn.close()

def write_summary_to_postgres(df_summary):
    """
    Convierte summary a pandas y hace batch insert/replace en Postgres.
    Para datasets grandes: implementa append/incr/upsert según necesidad.
    """
    if df_summary.rdd.isEmpty():
        print("No summary rows to write.")
        return

    pdf = df_summary.toPandas()

    # Conexión
    conn = psycopg2.connect(host=DEST_PG_HOST,
                            port=DEST_PG_PORT,
                            dbname=DEST_PG_DB,
                            user=DEST_PG_USER,
                            password=DEST_PG_PASSWORD)
    cur = conn.cursor()

    # Si quieres truncar la tabla antes:
    # cur.execute("TRUNCATE TABLE public.devices_summary_by_device_day;")
    # conn.commit()

    # Usaremos INSERT ... ON CONFLICT (si tienes PK), pero aquí no definimos PK.
    # Para simplicidad haremos DELETE de las filas de las fechas presentes y luego INSERT.
    # Obtener fechas únicas
    dates = pdf['date'].dropna().astype(str).unique().tolist()
    if dates:
        # delete las filas de esas fechas para evitar duplicados (ajusta según tu política)
        cur.execute("DELETE FROM public.devices_summary_by_device_day WHERE date = ANY(%s);", (dates,))

    # Preparar tuplas
    tuples = list(pdf[['device_id','device_name','date','points_count','avg_speed','max_speed','avg_battery']].itertuples(index=False, name=None))

    sql = """
    INSERT INTO public.devices_summary_by_device_day
    (device_id, device_name, date, points_count, avg_speed, max_speed, avg_battery)
    VALUES (%s)
    """
    if tuples:
        execute_values(cur, sql, tuples)
        conn.commit()
        print(f"Wrote {len(tuples)} summary rows to Postgres.")
    else:
        print("No tuples to insert.")

    cur.close()
    conn.close()

def write_clean_to_postgres(df_clean):
    """
    Escribe la tabla limpia en devices_positions_clean reemplazando por completo.
    Para grandes volúmenes, usa append o particiones.
    """
    if df_clean.rdd.isEmpty():
        print("No clean rows to write.")
        return

    pdf = df_clean.select("id","device_name","device_id","latitude","longitude","altitude","speed","battery","signal","sim_operator","network_type","timestamp").toPandas()

    conn = psycopg2.connect(host=DEST_PG_HOST, port=DEST_PG_PORT, dbname=DEST_PG_DB, user=DEST_PG_USER, password=DEST_PG_PASSWORD)
    cur = conn.cursor()

    # Borrar y reinsertar (alternativa: truncate + copy_from for performance)
    cur.execute("TRUNCATE TABLE public.devices_positions_clean;")
    conn.commit()

    tuples = list(pdf.itertuples(index=False, name=None))
    sql = """
    INSERT INTO public.devices_positions_clean
    (id, device_name, device_id, latitude, longitude, altitude, speed, battery, signal, sim_operator, network_type, timestamp)
    VALUES (%s)
    """
    if tuples:
        execute_values(cur, sql, tuples)
        conn.commit()
        print(f"Wrote {len(tuples)} clean rows to Postgres.")
    else:
        print("No tuples to insert.")

    cur.close()
    conn.close()

def main():
    # crear spark
    spark = create_spark()

    # fetch data
    data = fetch_data()

    if not data:
        print("No data fetched; exiting.")
        spark.stop()
        return

    # crear df spark
    df = json_to_spark_df(spark, data)
    print("Input schema:")
    df.printSchema()

    df_clean, df_summary = transform(df)

    # crear tablas destino si no existen
    ensure_tables_created()

    # escribir resultados
    write_clean_to_postgres(df_clean)
    write_summary_to_postgres(df_summary)

    spark.stop()
    print("ETL terminado.")

if __name__ == "__main__":
    main()

