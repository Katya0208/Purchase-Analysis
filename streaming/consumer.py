# streaming/consumer.py
"""
Консьюмер Kafka → ClickHouse с использованием confluent-kafka.
"""

import json
import os
import datetime
from confluent_kafka import Consumer, KafkaException
from clickhouse_driver import Client
import sys

# Настройка ClickHouse
client = Client(
    host=os.getenv("CLICKHOUSE_HOST", "localhost"),
    user=os.getenv("CLICKHOUSE_USER", "default"),
    password=os.getenv("CLICKHOUSE_PASSWORD", "")
)

# HEALTHCHECK: если запущен с флагом --check-health, просто проверим ClickHouse
if "--check-health" in sys.argv:
    try:
        client.execute("SELECT 1")
        sys.exit(0)
    except Exception as e:
        sys.exit(1)

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
    'group.id': f'purchase-consumer-{int(datetime.datetime.now().timestamp())}',  # Уникальный ID группы
    'auto.offset.reset': 'latest',  # Читаем только новые сообщения
    'enable.auto.commit': False  # Отключаем авто-коммит
}
consumer = Consumer(conf)
consumer.subscribe([os.getenv("PURCHASES_TOPIC", "purchases")])

print("✅ Consumer started, waiting for messages…")
print(f"Using consumer group: {conf['group.id']}")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        # Десериализуем
        try:
            r = json.loads(msg.value().decode('utf-8'))
            print(f"Received message: {r}")

            # Приводим price к float
            price = float(r["price"])

            # Парсим timestamp в datetime
            ts_str = r["timestamp"]
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
            # Ручной коммит после успешной обработки
            consumer.commit(msg)
        except Exception as e:
            print(f"Error processing message: {e}")
            continue
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    print("Consumer stopped")
