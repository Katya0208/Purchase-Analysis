from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel, condecimal
from uuid import UUID, uuid4
from kafka import KafkaProducer
import uuid, datetime, os, json
from typing import Union, Optional, List, Dict
from confluent_kafka import Producer
from clickhouse_driver import Client
import boto3
from botocore.client import Config
import pandas as pd
from io import StringIO
from pathlib import Path

app = FastAPI(
    title="Purchase Analysis API",
    description="API для анализа покупок в магазине",
    version="1.0.0",
    openapi_tags=[
        {
            "name": "data_management",
            "description": "Эндпоинты для управления данными (товары, продавцы, покупки)"
        },
        {
            "name": "clickhouse_analytics",
            "description": "Аналитика в реальном времени из ClickHouse"
        },
        {
            "name": "s3_analytics",
            "description": "Аналитика из S3 (исторические отчеты)"
        }
    ]
)

# Настройка ClickHouse
clickhouse_client = Client(
    host=os.getenv("CLICKHOUSE_HOST", "localhost"),
    user=os.getenv("CLICKHOUSE_USER", "default"),
    password=os.getenv("CLICKHOUSE_PASSWORD", "")
)

producer = Producer({
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
})

# Настройка S3 клиента
s3_client = boto3.client(
    's3',
    endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
    aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minio"),
    aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minio123"),
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

BUCKET_NAME = "stage"
REPORTS_PREFIX = "reports/"

class Purchase(BaseModel):
    purchase_id: Optional[UUID] = None
    client_id: str
    product_id: str
    quantity: int
    price: float
    timestamp: str

class Product(BaseModel):
    product_id: str
    name: str
    category: str
    price: float
    description: Optional[str] = None
    seller_id: str

class Seller(BaseModel):
    seller_id: str
    name: str
    email: str
    phone: Optional[str] = None
    address: Optional[str] = None

# -------------------- Эндпоинты для управления данными --------------------------

@app.post("/api/products", response_model=Product, tags=["data_management"])
async def create_product(product: Product):
    """
    Добавление нового товара в систему.
    Отправляет данные в Kafka для последующей обработки.
    """
    try:
        # Отправляем данные в Kafka
        producer.produce(
            topic=os.getenv("PRODUCTS_TOPIC", "products"),
            value=json.dumps(product.dict()).encode('utf-8')
        )
        producer.flush()
        return product
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Kafka produce error: {str(e)}")

@app.get("/api/products", response_model=List[Product], tags=["data_management"])
async def get_products(category: Optional[str] = None, seller_id: Optional[str] = None):
    """
    Получение списка товаров с возможностью фильтрации по категории и продавцу
    """
    try:
        query = """
        SELECT 
            product_id,
            name,
            category,
            price,
            description,
            seller_id
        FROM products_rt
        WHERE 1=1
        """
        
        params = []
        if category:
            query += " AND category = %s"
            params.append(category)
        if seller_id:
            query += " AND seller_id = %s"
            params.append(seller_id)
            
        result = clickhouse_client.execute(query, params)
        
        return [
            {
                "product_id": row[0],
                "name": row[1],
                "category": row[2],
                "price": row[3],
                "description": row[4],
                "seller_id": row[5]
            }
            for row in result
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/sellers", response_model=Seller, tags=["data_management"])
async def create_seller(seller: Seller):
    """
    Добавление нового продавца в систему.
    Отправляет данные в Kafka для последующей обработки.
    """
    try:
        # Отправляем данные в Kafka
        producer.produce(
            topic=os.getenv("SELLERS_TOPIC", "sellers"),
            value=json.dumps(seller.dict()).encode('utf-8')
        )
        producer.flush()
        return seller
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Kafka produce error: {str(e)}")

@app.get("/api/sellers", response_model=List[Seller], tags=["data_management"])
async def get_sellers():
    """
    Получение списка всех продавцов
    """
    try:
        query = """
        SELECT 
            seller_id,
            name,
            email,
            phone,
            address
        FROM sellers_rt
        """
        
        result = clickhouse_client.execute(query)
        
        return [
            {
                "seller_id": row[0],
                "name": row[1],
                "email": row[2],
                "phone": row[3],
                "address": row[4]
            }
            for row in result
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/sellers/{seller_id}", response_model=Seller, tags=["data_management"])
async def get_seller(seller_id: str):
    """
    Получение информации о конкретном продавце
    """
    try:
        query = """
        SELECT 
            seller_id,
            name,
            email,
            phone,
            address
        FROM sellers_rt
        WHERE seller_id = %s
        """
        
        result = clickhouse_client.execute(query, [seller_id])
        
        if not result:
            raise HTTPException(status_code=404, detail="Seller not found")
            
        row = result[0]
        return {
            "seller_id": row[0],
            "name": row[1],
            "email": row[2],
            "phone": row[3],
            "address": row[4]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/purchase", response_model=List[Purchase], tags=["data_management"])
async def get_purchases(
    client_id: Optional[str] = None,
    product_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """
    Получение списка покупок с возможностью фильтрации по:
    - client_id: ID клиента
    - product_id: ID товара
    - start_date: начальная дата (включительно)
    - end_date: конечная дата (включительно)
    """
    try:
        query = """
        SELECT 
            purchase_id,
            client_id,
            product_id,
            quantity,
            price,
            timestamp
        FROM purchases_rt
        WHERE 1=1
        """
        
        params = []
        if client_id:
            query += " AND client_id = %s"
            params.append(client_id)
        if product_id:
            query += " AND product_id = %s"
            params.append(product_id)
        if start_date:
            query += " AND timestamp >= %s"
            params.append(start_date)
        if end_date:
            query += " AND timestamp <= %s"
            params.append(end_date)
            
        query += " ORDER BY timestamp DESC"
            
        result = clickhouse_client.execute(query, params)
        
        return [
            {
                "purchase_id": row[0],
                "client_id": row[1],
                "product_id": row[2],
                "quantity": row[3],
                "price": row[4],
                "timestamp": row[5].strftime("%Y-%m-%dT%H:%M:%SZ")
            }
            for row in result
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------- Аналитика из ClickHouse --------------------------

@app.get("/api/analytics/top_products", tags=["clickhouse_analytics"])
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

@app.get("/api/analytics/sales_by_month", tags=["clickhouse_analytics"])
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

@app.get("/api/analytics/client_statistics", tags=["clickhouse_analytics"])
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

@app.get("/api/analytics/revenue_by_hour", tags=["clickhouse_analytics"])
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

# -------------------- Аналитика из S3 --------------------------

@app.get("/api/analytics/s3/revenue_by_category", tags=["s3_analytics"])
async def get_s3_revenue_by_category():
    """
    Получить отчет о выручке по категориям из S3
    """
    try:
        # Получаем список файлов в директории reports
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=f"{REPORTS_PREFIX}revenue_by_category_"
        )
        
        if 'Contents' not in response:
            raise HTTPException(status_code=404, detail="No reports found")
        
        # Берем самый последний отчет
        latest_report = max(response['Contents'], key=lambda x: x['LastModified'])
        
        # Читаем содержимое файла
        obj = s3_client.get_object(
            Bucket=BUCKET_NAME,
            Key=latest_report['Key']
        )
        
        # Читаем CSV файл
        df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        return df.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics/s3/top_clients", tags=["s3_analytics"])
async def get_s3_top_clients():
    """
    Получить отчет о топ клиентах из S3
    """
    try:
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=f"{REPORTS_PREFIX}top_clients_"
        )
        
        if 'Contents' not in response:
            raise HTTPException(status_code=404, detail="No reports found")
        
        latest_report = max(response['Contents'], key=lambda x: x['LastModified'])
        
        obj = s3_client.get_object(
            Bucket=BUCKET_NAME,
            Key=latest_report['Key']
        )
        
        df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        return df.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics/s3/avg_check_by_day", tags=["s3_analytics"])
async def get_s3_avg_check_by_day():
    """
    Получить отчет о среднем чеке по дням из S3
    """
    try:
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=f"{REPORTS_PREFIX}avg_check_by_day_"
        )
        
        if 'Contents' not in response:
            raise HTTPException(status_code=404, detail="No reports found")
        
        latest_report = max(response['Contents'], key=lambda x: x['LastModified'])
        
        obj = s3_client.get_object(
            Bucket=BUCKET_NAME,
            Key=latest_report['Key']
        )
        
        df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        return df.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics/s3/top_selling_products", tags=["s3_analytics"])
async def get_s3_top_selling_products():
    """
    Получить отчет о топ продаваемых продуктах из S3
    """
    try:
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=f"{REPORTS_PREFIX}top_selling_products_"
        )
        
        if 'Contents' not in response:
            raise HTTPException(status_code=404, detail="No reports found")
        
        latest_report = max(response['Contents'], key=lambda x: x['LastModified'])
        
        obj = s3_client.get_object(
            Bucket=BUCKET_NAME,
            Key=latest_report['Key']
        )
        
        df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        return df.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/api/purchase", response_model=dict, tags=["data_management"], openapi_extra={
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
async def purchase(request: Request):
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
