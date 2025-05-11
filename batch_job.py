from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
import argparse

def main(current_date):
    jdbc_driver_path = "postgresql-42.6.0.jar"

    spark = SparkSession.builder \
        .appName("BatchProcessing") \
        .config("spark.driver.extraJavaOptions", "--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.security=ALL-UNNAMED") \
        .config("spark.driver.extraClassPath", jdbc_driver_path) \
        .getOrCreate()

    # Путь к Stage слою
    stage_path = f"stage/data/date={current_date}"
    
    # Чтение данных
    df = spark.read.parquet(stage_path)

    # Извлечение клиентов
    clients_df = df.select(
        col("client.client_id").alias("client_id"),
        col("client.first_name").alias("first_name"),
        col("client.last_name").alias("last_name"),
        col("client.email").alias("email"),
        col("client.address").alias("address"),
        to_timestamp(col("client.registration_date"), "yyyy-MM-dd HH:mm:ss").alias("registration_date")
    ).distinct()

    # Запись новых клиентов
    clients_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", "clients") \
        .option("user", "lowban") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()


    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Date partition in YYYY-MM-DD format")
    args = parser.parse_args()
    
    main(args.date)