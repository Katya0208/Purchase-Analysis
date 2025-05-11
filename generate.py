from pyspark.sql import SparkSession
from datetime import datetime
import uuid

# Инициализируйте Spark
spark = SparkSession.builder \
    .appName("GenerateTestData") \
    .getOrCreate()

# Укажите путь для сохранения тестовых данных
stage_path = "stage/data/date=2025-05-11"

# Сгенерируйте тестовые данные
test_data = [
    {
        # Структура должна соответствовать Stage слою (раздел 6 документации)
        "client": {
            "client_id": str(uuid.uuid4()),  # Генерация случайного UUID
            "first_name": "Alice",
            "last_name": "Johnson",
            "email": "alice@example.com",
            "address": "123 Main St",
            "registration_date": datetime(2024, 1, 1)
        },
        "purchase": {
            "purchase_id": str(uuid.uuid4()),
            "product_id": str(uuid.uuid4()),
            "quantity": 2,
            "price": 99.99,
            "timestamp": "2025-05-11T12:00:00Z"  # ISO 8601
        },
        "product": {
            "product_id": str(uuid.uuid4()),
            "name": "Laptop",
            "category": "Electronics",
            "price": 999.99,
            "available_stock": 10,
            "updated_at": int(datetime(2025, 4, 6).timestamp() * 1000)  # Timestamp в миллисекундах
        }
    }
]

# Создайте DataFrame и сохраните в Parquet
df = spark.createDataFrame(test_data)
df.write.parquet(stage_path)

print(f"Тестовые данные сохранены в {stage_path}")