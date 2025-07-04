import os
from pyspark.sql import SparkSession, functions as F, Window
from datetime import datetime

WAREHOUSE = "s3a://stage/warehouse"     # Iceberg warehouse
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
REPORTS_BUCKET = "s3a://stage/reports"  # Bucket for reports

spark = (
    SparkSession.builder.appName("Analytics_Job")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hadoop")
    .config("spark.sql.catalog.spark_catalog.warehouse", WAREHOUSE)
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

clients = spark.read.format("iceberg").load("spark_catalog.default.stage_clients")
products = spark.read.format("iceberg").load("spark_catalog.default.stage_products")
purchases = spark.read.format("iceberg").load("spark_catalog.default.stage_purchases")
sellers = spark.read.format("iceberg").load("spark_catalog.default.stage_sellers")

#Проверка
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

def save_report_to_s3(df, report_name):
    """Сохраняет отчет в формате CSV в S3"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"{REPORTS_BUCKET}/{report_name}_{timestamp}.csv"
    
    df.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(output_path)
    
    print(f"Отчет сохранен в: {output_path}")

def calculate_revenue_by_category(purchases_df, products_df) -> list:
    """Рассчитывает общую выручку и количество покупок по категориям товаров"""
    df = (
        purchases_df.alias("p")
        .join(products_df.alias("pr"), "product_id", "left")
        .groupBy(F.coalesce("category", F.lit("N/A")).alias("category"))
        .agg(
            F.sum(F.col("quantity") * F.col("p.price")).alias("total_revenue"),
            F.countDistinct("purchase_id").alias("total_purchases")
        )
        .orderBy(F.desc("total_revenue"))
    )
    
    # Сохраняем отчет в S3
    save_report_to_s3(df, "revenue_by_category")
    
    return [row.asDict() for row in df.collect()]

def get_top_clients(purchases_df, limit=10) -> list:
    """Возвращает топ N клиентов по количеству покупок"""
    df = (
        purchases_df.groupBy("client_id")
        .agg(F.count("*").alias("purchase_count"))
        .orderBy(F.desc("purchase_count"))
        .limit(limit)
    )
    
    # Сохраняем отчет в S3
    save_report_to_s3(df, "top_clients")
    
    return [row.asDict() for row in df.collect()]

def calculate_avg_check_by_day(purchases_df) -> list:
    """Рассчитывает средний чек и количество покупок по дням"""
    purchase_totals = purchases_df.groupBy("purchase_id", "timestamp").agg(
        F.sum(F.col("quantity") * F.col("price")).alias("purchase_total")
    )
    
    df = (
        purchase_totals.withColumn("date", F.to_date("timestamp"))
        .groupBy("date")
        .agg(
            F.avg("purchase_total").alias("avg_check"),
            F.countDistinct("purchase_id").alias("purchases_per_day")
        )
        .orderBy("date")
    )
    
    # Сохраняем отчет в S3
    save_report_to_s3(df, "avg_check_by_day")
    
    return [row.asDict() for row in df.collect()]

def get_top_selling_products(purchases_df, products_df, limit=10) -> list:
    """Возвращает продукты с наибольшим количеством продаж"""
    windowSpec = Window.partitionBy(F.lit(1)).orderBy(F.desc("total_sales"))
    
    df = (
        purchases_df.groupBy("product_id")
        .agg(F.sum("quantity").alias("total_sales"))
        .join(products_df, "product_id", "left")  # left join для сохранения всех продуктов
        .select(
            "product_id",
            F.coalesce("name", F.lit("Unknown")).alias("name"),
            "total_sales"
        )
        .withColumn("rank", F.dense_rank().over(windowSpec))
        .filter(F.col("rank") <= limit)  # Топ-N вместо только первого места
        .orderBy("rank")
    )
    
    # Сохраняем отчет в S3
    save_report_to_s3(df, "top_selling_products")
    
    return [row.asDict() for row in df.collect()]

#Вывод
print("=== Общая выручка по категориям ===")
revenue_data = calculate_revenue_by_category(purchases, products)
for item in revenue_data:
    print(f"{item['category']}: {item['total_revenue']} руб. ({item['total_purchases']} покупок)")
print("\n\n")

print("=== Топ 10 клиентов ===")
top_clients_data = get_top_clients(purchases, 5)
for idx, client in enumerate(top_clients_data, 1):
    print(f"{idx}. Клиент {client['client_id']}: {client['purchase_count']} покупок")
print("\n\n")

print("=== Средний чек по дням ===")
avg_check_data = calculate_avg_check_by_day(purchases)
for day in avg_check_data:
    print(f"{day['date']}: {day['avg_check']:.2f} руб. ({day['purchases_per_day']} покупок)")
print("\n\n")

print("=== Продукты с наибольшим количеством продаж ===")
top_products_data = get_top_selling_products(purchases, products)
for product in top_products_data:
    print(f"{product['name']}: {product['total_sales']} шт.")

spark.stop()