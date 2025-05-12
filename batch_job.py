from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import logging
import argparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main(current_date):
    spark = SparkSession.builder \
        .appName("BatchProcessing") \
        .config("spark.driver.extraJavaOptions", "--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.security=ALL-UNNAMED") \
        .config("spark.driver.extraClassPath", "/Users/lowban/Desktop/MAI/IT-projects/Purchase-Analysis/postgresql-42.6.0.jar") \
        .getOrCreate()

    stage_path = f"/Users/lowban/Desktop/MAI/IT-projects/Purchase-Analysis/stage/data/date={current_date}"

    schema = StructType([
        StructField("client", StructType([
            StructField("client_id", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("address", StringType(), True),
            StructField("registration_date", StringType(), True)
        ]), True),
        StructField("product", StructType([
            StructField("product_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("available_stock", IntegerType(), True),
            StructField("updated_at", StringType(), True)
        ]), True),
        StructField("purchase", StructType([
            StructField("purchase_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", DoubleType(), True),
            StructField("timestamp", StringType(), True)
        ]), True)
    ])

    try:
        df = spark.read \
            .schema(schema) \
            .parquet(stage_path)
        logger.info("Данные успешно прочитаны из Parquet")
    except Exception as e:
        logger.error(f"Ошибка чтения Parquet: {str(e)}")
        raise

    clients_df = df.select(
        col("client.client_id").alias("client_id"),
        col("client.first_name").alias("first_name"),
        col("client.last_name").alias("last_name"),
        col("client.email").alias("email"),
        col("client.address").alias("address"),
        to_timestamp(col("client.registration_date"), "yyyy-MM-dd HH:mm:ss").alias("registration_date")
    ).distinct()

    try:
        clients_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/postgres") \
            .option("dbtable", "clients") \
            .option("user", "lowban") \
            .option("password", "ваш_пароль") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        logger.info("Данные успешно записаны в PostgreSQL")
    except Exception as e:
        logger.error(f"Ошибка записи в PostgreSQL: {str(e)}")
        raise

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Date partition in YYYY-MM-DD format")
    args = parser.parse_args()
    main(args.date)


# CREATE TABLE product(
# )
# 
# 
# 