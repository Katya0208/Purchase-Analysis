# streaming/consumer.py
"""
Консьюмер Kafka → ClickHouse (потоковые покупки).
Запускать на хосте, где установлен Python 3.10+.
"""

import json, os
from kafka import KafkaConsumer
from clickhouse_driver import Client

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC     = os.getenv("PURCHASES_TOPIC", "purchases")
client = Client(host="localhost")     # порт 9000 проброшен

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

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    value_deserializer=lambda m: json.loads(m.decode()),
    auto_offset_reset="earliest",
)

print("✅ Consumer started, waiting for messages…")
for msg in consumer:
    r = msg.value
    client.execute(
        "INSERT INTO purchases_rt VALUES",
        [(r["purchase_id"], r["client_id"], r["product_id"],
          r["quantity"], r["price"], r["timestamp"])]
    )
