# etl/count_stage.py
from pyspark.sql import SparkSession
import os

# 1) Создаём SparkSession
spark = (
    SparkSession.builder
    .appName("CountStage")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hadoop")
    .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://stage/warehouse")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

# 2) Отключаем подробные логи
spark.sparkContext.setLogLevel("ERROR")

# 3) Считаем строки
tables = ["stage_clients", "stage_sellers", "stage_products", "stage_purchases"]
for t in tables:
    df = spark.table(f"spark_catalog.default.{t}")
    print(f"{t}: {df.count()} rows")

# 4) Закрываем сессию
spark.stop()
