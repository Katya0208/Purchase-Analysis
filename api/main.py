# api/main.py
from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel, condecimal
from uuid import UUID, uuid4
from kafka import KafkaProducer
import uuid, datetime, os, json

app = FastAPI()

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
    value_serializer=lambda v: json.dumps(v, default=str).encode(),
)

class Purchase(BaseModel):
    purchase_id: UUID | None = None
    client_id:   UUID
    product_id:  UUID
    quantity:    int
    price:       condecimal(gt=0)
    timestamp:   datetime.datetime

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
        producer.send("purchases", data)
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
        producer.send("purchases", rec)
        producer.flush()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Kafka produce error: {str(e)}")
    return rec