# api/main.py
from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel, condecimal
from uuid import UUID, uuid4
from kafka import KafkaProducer
import uuid, datetime, os, json
from typing import Union, Optional, List, Dict
from confluent_kafka import Producer
from clickhouse_driver import Client

app = FastAPI()

# Настройка ClickHouse
clickhouse_client = Client(
    host=os.getenv("CLICKHOUSE_HOST", "localhost"),
    user=os.getenv("CLICKHOUSE_USER", "default"),
    password=os.getenv("CLICKHOUSE_PASSWORD", "")
)

producer = Producer({
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
})

class Purchase(BaseModel):
    purchase_id: Optional[UUID] = None
    client_id: str
    product_id: str
    quantity: int
    price: float
    timestamp: str

# -------------------- Аналитические эндпоинты --------------------------------------

@app.get("/api/analytics/top_products")
async def get_top_products(limit: int = 10):
    """
    Получить топ-N товаров по количеству продаж
    """
    query = f"""
    SELECT 
        product_id,
        sum(quantity) as total_quantity,
        sum(quantity * price) as total_revenue
    FROM purchases_rt
    GROUP BY product_id
    ORDER BY total_quantity DESC
    LIMIT {limit}
    """
    try:
        result = clickhouse_client.execute(query)
        return [
            {
                "product_id": row[0],
                "total_quantity": row[1],
                "total_revenue": row[2]
            }
            for row in result
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics/sales_by_month")
async def get_sales_by_month():
    """
    Получить статистику продаж по месяцам
    """
    query = """
    SELECT 
        toStartOfMonth(ts) as month,
        count() as total_purchases,
        sum(quantity) as total_quantity,
        sum(quantity * price) as total_revenue
    FROM purchases_rt
    GROUP BY month
    ORDER BY month DESC
    """
    try:
        result = clickhouse_client.execute(query)
        return [
            {
                "month": row[0].strftime("%Y-%m"),
                "total_purchases": row[1],
                "total_quantity": row[2],
                "total_revenue": row[3]
            }
            for row in result
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics/client_statistics")
async def get_client_statistics(client_id: str):
    """
    Получить статистику по конкретному клиенту
    """
    query = f"""
    SELECT 
        client_id,
        count() as total_purchases,
        sum(quantity) as total_quantity,
        sum(quantity * price) as total_spent,
        avg(price) as avg_price
    FROM purchases_rt
    WHERE client_id = '{client_id}'
    GROUP BY client_id
    """
    try:
        result = clickhouse_client.execute(query)
        if not result:
            raise HTTPException(status_code=404, detail="Client not found")
        
        row = result[0]
        return {
            "client_id": row[0],
            "total_purchases": row[1],
            "total_quantity": row[2],
            "total_spent": row[3],
            "avg_price": row[4]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics/revenue_by_hour")
async def get_revenue_by_hour():
    """
    Получить распределение выручки по часам
    """
    query = """
    SELECT 
        toHour(ts) as hour,
        sum(quantity * price) as revenue
    FROM purchases_rt
    GROUP BY hour
    ORDER BY hour
    """
    try:
        result = clickhouse_client.execute(query)
        return [
            {
                "hour": row[0],
                "revenue": row[1]
            }
            for row in result
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------- Эндпоинт: raw_purchase --------------------------------------
@app.post("/api/raw_purchase", response_model=dict, openapi_extra={
    "requestBody": {
        "content": {
            "application/json": {
                "example": {
                    "purchase_id": "p-001",
                    "client_id": "c-123",
                    "product_id": "prod-567",
                    "quantity": 3,
                    "price": 99.99,
                    "timestamp": "2025-05-18T12:00:00Z"
                }
            }
        }
    }
})
async def raw_purchase(request: Request):
    """
    Сырая ручка для добавления записи о покупке в Kafka-топик 'purchases'.
    Просто берёт JSON из тела и шлёт в Kafka, без валидации.
    """
    data = await request.json()
    try:
        producer.produce(
            topic=os.getenv("PURCHASES_TOPIC", "purchases"),
            value=json.dumps(data).encode('utf-8')
        )
        producer.flush()
        return {"status": "ok", "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Kafka produce error: {str(e)}")


# -------------------- Эндпоинт: валидированный purchase --------------------------
@app.post("/api/purchase", response_model=Purchase)
async def create_purchase(p: Purchase):
    """
    Валидированная ручка. Если purchase_id не указан, генерирует UUID. 
    Отправляет сообщение в Kafka, возвращает Pydantic-модель.
    """
    rec = p.dict()
    # Если нет purchase_id, генерируем новый
    if rec["purchase_id"] is None:
        rec["purchase_id"] = uuid4()
    # Убедимся, что timestamp — datetime (Pydantic гарантирует)
    try:
        producer.produce(
            topic=os.getenv("PURCHASES_TOPIC", "purchases"),
            value=json.dumps(rec).encode('utf-8')
        )
        producer.flush()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Kafka produce error: {str(e)}")
    return rec