import os
from clickhouse_driver import Client

client = Client(
    host=os.getenv("CLICKHOUSE_HOST", "localhost"),
    user=os.getenv("CLICKHOUSE_USER", "default"),
    password=os.getenv("CLICKHOUSE_PASSWORD", "")
)

try:
    result = client.execute("SELECT count() FROM purchases_rt")
    count = result[0][0]
    print(f"✅ В таблице purchases_rt {count} строк(и)")
except Exception as e:
    print(f"❌ Ошибка при проверке ClickHouse: {e}")
