# streaming/consumer.py
"""
Консьюмер Kafka → ClickHouse с использованием confluent-kafka.
"""

import json
import os
import datetime
from confluent_kafka import Consumer, KafkaException
from clickhouse_driver import Client

# Настройка ClickHouse
client = Client(
    host=os.getenv("CLICKHOUSE_HOST", "localhost"),
    user=os.getenv("CLICKHOUSE_USER", "default"),
    password=os.getenv("CLICKHOUSE_PASSWORD", "")
)

client.execute("""
CREATE TABLE IF NOT EXISTS purchases_rt (
  purchase_id String,
  client_id   String,
  product_id  String,
  quantity    UInt32,
  price       Float64,
  ts          DateTime
) ENGINE = MergeTree ORDER BY ts
""")

# Настройка Kafka Consumer
conf = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
    'group.id': 'purchase-consumers',
    'auto.offset.reset': 'earliest',
}
consumer = Consumer(conf)
consumer.subscribe([os.getenv("PURCHASES_TOPIC", "purchases")])

print("✅ Consumer started, waiting for messages…")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())

        # Десериализуем
        r = json.loads(msg.value().decode('utf-8'))

        # Приводим price к float
        price = float(r["price"])

        # Парсим timestamp в datetime
        # Пример r["timestamp"] = "2025-05-18T12:00:00Z"
        ts_str = r["timestamp"]
        # Заменяем 'Z' на '+00:00' для fromisoformat
        if ts_str.endswith("Z"):
            ts_str = ts_str[:-1] + "+00:00"
        ts = datetime.datetime.fromisoformat(ts_str)

        # Записываем в ClickHouse
        client.execute(
            "INSERT INTO purchases_rt VALUES",
            [(
                r["purchase_id"],
                r["client_id"],
                r["product_id"],
                r["quantity"],
                price,
                ts
            )]
        )
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    print("Consumer stopped")
