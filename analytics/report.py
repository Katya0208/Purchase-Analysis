import os
from pyspark.sql import SparkSession, functions as F, Window
import requests

# ------------- Переменные окружения ----------------------
WAREHOUSE = "s3a://stage/warehouse"     # Iceberg warehouse
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET = os.getenv("MINIO_ROOT_PASSWORD", "minio123")

# ------------- Spark Session -----------------------------
spark = (
    SparkSession.builder.appName("Analytics_Job")
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
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ------------- Чтение таблиц из Iceberg ------------------
# ------------- Чтение таблиц из Iceberg ------------------
clients = spark.read.format("iceberg").load("spark_catalog.default.stage_clients")
products = spark.read.format("iceberg").load("spark_catalog.default.stage_products")
purchases = spark.read.format("iceberg").load("spark_catalog.default.stage_purchases")
sellers = spark.read.format("iceberg").load("spark_catalog.default.stage_sellers")

# ------------- Проверка данных ---------------------------
print("=== stage_clients ===")
clients.printSchema()
print(f"Количество записей: {clients.count()}")
clients.show(truncate=False)

print("=== stage_products ===")
products.printSchema()
print(f"Количество записей: {products.count()}")
products.show(truncate=False)

print("=== stage_purchases ===")
purchases.printSchema()
print(f"Количество записей: {purchases.count()}")
purchases.show(truncate=False)

print("=== stage_sellers ===")
sellers.printSchema()
print(f"Количество записей: {sellers.count()}")
sellers.show(truncate=False)

# ------------- Аналитика: Примеры ------------------------

# 1. Общая выручка по категориям товаров
revenue_by_category = (
    purchases.alias("p")  # Алиас для покупок
    .join(products.alias("pr"), "product_id")  # Алиас для продуктов
    .groupBy("category")
    .agg(
        F.sum(F.col("quantity") * F.col("pr.price")).alias("total_revenue"),  # ← явно указываем pr.price
        F.countDistinct("purchase_id").alias("total_purchases")
    )
    .orderBy(F.desc("total_revenue"))
)

# 2. Топ 10 клиентов по количеству покупок
top_clients = (
    purchases.groupBy("client_id")
    .agg(F.count("*").alias("purchase_count"))
    .orderBy(F.desc("purchase_count"))
    .limit(10)
)

# 3. Средний чек по дням
avg_check_by_day = (
    purchases.withColumn("date", F.to_date("timestamp"))
    .groupBy("date")
    .agg(
        F.avg(F.col("quantity") * F.col("price")).alias("avg_check"),
        F.countDistinct("purchase_id").alias("purchases_per_day")
    )
    .orderBy("date")
)

# 4. Продавцы с наибольшим количеством продаж
windowSpec = Window.orderBy(F.desc("total_sales"))

top_products = (
    purchases.groupBy("product_id")
    .agg(F.sum("quantity").alias("total_sales"))
    .join(products, "product_id")  # чтобы получить name
    .select("product_id", "name", "total_sales")
    .withColumn("rank", F.rank().over(windowSpec))
    .filter(F.col("rank") == 1)
)

# ------------- Вывод результатов в консоль ---------------
print("=== Общая выручка по категориям ===")
revenue_by_category.show(truncate=False)

print("=== Топ 10 клиентов ===")
top_clients.show(truncate=False)

print("=== Средний чек по дням ===")
avg_check_by_day.show(truncate=False)

print("=== Продукты с наибольшим количеством продаж ===")
top_products.show(truncate=False)

# ------------- Завершение -------------------------------
spark.stop()

# docker compose exec spark-master spark-submit \
#   --master spark://spark-master:7077 \
#   --conf spark.driver.host=spark-master \
#   --conf spark.executor.host=spark-worker \
#   --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,software.amazon.awssdk:bundle:2.17.119 \
#   /workspace/analytics/report.py