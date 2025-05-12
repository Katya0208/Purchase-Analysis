from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from datetime import datetime
import uuid
import logging
import argparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main(current_date):
    spark = SparkSession.builder \
        .appName("GenerateTestData") \
        .config("spark.driver.extraJavaOptions", "--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.security=ALL-UNNAMED") \
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

    test_data = [
        {
            "client": {
                "client_id": str(uuid.uuid4()),
                "first_name": "Alice",
                "last_name": "Johnson",
                "email": "alice@example.com",
                "address": "123 Main St",
                "registration_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            },
            "product": {
                "product_id": str(uuid.uuid4()),
                "name": "Laptop",
                "category": "Electronics",
                "price": 999.99,
                "available_stock": 10,
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            },
            "purchase": {
                "purchase_id": str(uuid.uuid4()),
                "product_id": str(uuid.uuid4()),
                "quantity": 2,
                "price": 99.99,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        }
    ]

    df = spark.createDataFrame(test_data, schema=schema)
    
    try:
        df.write \
            .mode("overwrite") \
            .parquet(stage_path)
        logger.info(f"Данные успешно записаны в {stage_path}")
    except Exception as e:
        logger.error(f"Ошибка записи в Parquet: {str(e)}")
        raise

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Date partition in YYYY-MM-DD format")
    args = parser.parse_args()
    main(args.date)