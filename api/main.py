# api/main.py
from fastapi import FastAPI
from pydantic import BaseModel, condecimal
from uuid import UUID
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

@app.post("/api/purchases")
def add_purchase(p: Purchase):
    rec = p.dict()
    rec["purchase_id"] = rec["purchase_id"] or uuid.uuid4()
    producer.send("purchases", rec)
    return {"status": "ok", "purchase_id": rec["purchase_id"]}
