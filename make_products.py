import pandas as pd, uuid, random, datetime as dt, numpy as np

df = pd.DataFrame({
    # превращаем UUID в строку сразу
    "product_id": [str(uuid.uuid4()) for _ in range(5)],
    "name": [f"Product {i}" for i in range(5)],
    "category": random.choices(["Electronics", "Books", "Clothing"], k=5),
    "price": np.round(np.random.uniform(5, 500, 5), 2),
    "available_stock": np.random.randint(10, 100, 5),
    # корректное ISO-время с таймзоной UTC
    "updated_at": [dt.datetime.now(dt.timezone.utc).isoformat()] * 5
})

df.to_parquet("sample_products.parquet", index=False)
print("Parquet-файл sample_products.parquet создан")
