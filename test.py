# from pyspark.sql import SparkSession

# spark = SparkSession.builder \
#     .appName("Test") \
#     .config("spark.driver.extraJavaOptions", "--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.security=ALL-UNNAMED") \
#     .getOrCreate()

# df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
# df.show()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("JDBCTest") \
    .config("spark.driver.extraClassPath", "postgresql-42.6.0.jar") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "clients") \
    .option("user", "lowban") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.show()