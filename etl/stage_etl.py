"""
PySpark-джоба: PostgreSQL + MinIO + Kafka → Stage (Iceberg)
Запуск ВНУТРИ spark-master:
  spark-submit --master spark://spark-master:7077 \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,\
org.postgresql:postgresql:42.7.2,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
software.amazon.awssdk:bundle:2.17.119 \
    etl/stage_etl.py
"""

import os, json
from pyspark.sql import SparkSession, functions as F, types as T

# ----------- переменные окружения -----------------
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB   = os.getenv("POSTGRES_DB", "purchases")
POSTGRES_USER = os.getenv("POSTGRES_USER", "app")
POSTGRES_PWD  = os.getenv("POSTGRES_PASSWORD", "app_pwd")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_KEY      = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET   = os.getenv("MINIO_ROOT_PASSWORD", "minio123")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
PURCHASES_TOPIC = os.getenv("PURCHASES_TOPIC", "purchases")

WAREHOUSE = "s3a://stage/warehouse"     # куда кладём Iceberg-таблицы

# ------------- Spark session ----------------------
spark = (
    SparkSession.builder.appName("Stage_ETL")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hadoop")
    .config("spark.sql.catalog.spark_catalog.warehouse", WAREHOUSE)
    # S3A (MinIO)
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ---------- PostgreSQL → Iceberg (clients, sellers) ---------------
jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
props = {"user": POSTGRES_USER, "password": POSTGRES_PWD, "driver": "org.postgresql.Driver"}

for name in ("clients", "sellers"):
    (
        spark.read.jdbc(
            url=jdbc_url,
            table=f"public.{name}",
            properties=props 
        )
        .withColumn("load_date", F.current_date())
        .write.format("iceberg").mode("overwrite")
        .saveAsTable(f"spark_catalog.default.stage_{name}")
    )
    print(f"✓ stage_{name}")


# ---------- MinIO (products Parquet / Iceberg) --------------------
try:
    df_prod = spark.read.format("iceberg").load("spark_catalog.default.products")
except Exception:
    df_prod = spark.read.parquet("s3a://products/*.parquet")

(df_prod.withColumn("load_date", F.current_date())
       .write.format("iceberg").mode("overwrite")
       .saveAsTable("spark_catalog.default.stage_products"))
print("✓ stage_products")

# ---------- Kafka purchases → Iceberg -----------------------------
schema = T.StructType([
    T.StructField("purchase_id", T.StringType()),
    T.StructField("client_id", T.StringType()),
    T.StructField("product_id", T.StringType()),
    T.StructField("quantity", T.IntegerType()),
    T.StructField("price", T.DoubleType()),
    T.StructField("timestamp", T.StringType()),
])

raw = (spark.read.format("kafka")
               .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
               .option("subscribe", PURCHASES_TOPIC)
               .option("startingOffsets", "earliest")
               .load())

purchases = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
                 .select("d.*")
                 .withColumn("timestamp", F.to_timestamp("timestamp"))
                 .withColumn("load_date", F.current_date()))

(purchases.write.format("iceberg").mode("overwrite")
         .saveAsTable("spark_catalog.default.stage_purchases"))
print("✓ stage_purchases")

print("Stage ETL done")
