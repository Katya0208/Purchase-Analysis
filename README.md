# Проект "Анализ покупок в магазине"

## Оглавление

1. [Описание архитектуры](#описание-архитектуры)  
2. [Требования](#требования)  
3. [Запуск инфраструктуры](#запуск-инфраструктуры)  
4. [Инициализация данных](#инициализация-данных)  
5. [Добавление тестовых данных](#добавление-тестовых-данных)  
6. [Запуск streaming-консьюмера](#запуск-streaming-консьюмера)  
7. [Проверка ClickHouse](#проверка-clickhouse)  
8. [Запуск batch ETL (Stage)](#запуск-batch-etl-stage)  
9. [Проверка результатов Stage ETL](#проверка-результатов-stage-etl)  
10. [Полезные команды](#полезные-команды)  

---

## Описание архитектуры

- **PostgreSQL** — хранилище master-данных: клиенты (clients), продавцы (sellers).  
- **MinIO** (S3 API) — файловое хранилище parquet-файлов для товаров (products).  
- **Kafka** — поток покупок (topic `purchases`).  
- **ClickHouse** — оперативное (real-time) хранилище для отчетов (`purchases_rt`).  
- **Spark** — batch-слой (Stage ETL) читает из PostgreSQL, MinIO, Kafka и пишет Iceberg-таблицы в MinIO.  
- **FastAPI** — API для отправки покупок в Kafka.  
- **Streaming consumer** — читает из Kafka и пишет в ClickHouse.

---

## Требования

- Docker и Docker Compose  
- Python 3.10+ (для локального запуска consumer и make_products)  
- mc (MinIO Client) для работы с MinIO  

---

## Запуск инфраструктуры

```bash
docker compose up -d
```

Поднимет контейнеры:
- postgres (порт 5433)  
- zookeeper  
- kafka (порт 9092 внутри сети, 29092 на localhost)  
- clickhouse (HTTP 8123, TCP 9000)  
- minio (консоль 9001, API 9002)  
- spark-master (7077/8080) и spark-worker  
- kafka-init и minio-init для авто-настройки  

---

## Инициализация данных 

### !При поднятии контейнеров автоматически (тут описание, ничего не надо делать)!

1. **PostgreSQL**  
   SQL-скрипты из `./initdb/` автоматически выполняются при старте контейнера.

2. **MinIO**  
   Скрипт `minio-init` создаст бакеты:
   - `products`
   - `stage`

3. **Kafka**  
   Скрипт `kafka-init` создаст топик `purchases`.

---

## Добавление тестовых данных

### Клиенты и продавцы

Добавьте вручную (через pgAdmin или psql) пару записей в:
- `public.clients`
- `public.sellers`

Пример через psql:

```bash
docker compose exec postgres bash
psql -U $POSTGRES_USER -d $POSTGRES_DB
```

```sql
-- Добавляем клиентов
INSERT INTO clients (client_id, first_name, last_name, email, phone, address)
VALUES
  (gen_random_uuid(), 'Анна', 'Смирнова', 'anna@example.com', '+37120000001', 'Рига, ул. Ленина, 5'),
  (gen_random_uuid(), 'Олег', 'Кузнецов', 'oleg@example.com', '+37120000002', 'Даугавпилс, ул. Свободы, 10');

-- Добавляем продавцов
INSERT INTO sellers (seller_id, name, contact_email, contact_phone, rating)
VALUES
  (gen_random_uuid(), 'BestShop', 'contact@bestshop.lv', '+37130000001', 4.8),
  (gen_random_uuid(), 'SuperStore', 'info@superstore.lv', '+37130000002', 4.6);

```

```sql
SELECT COUNT(*) FROM clients;
SELECT COUNT(*) FROM sellers;
```

```bash
q
\q
exit
```
## Minio

Заходим, логинимся с данными из .env http://127.0.0.1:9001. При создании контейнера должно создаться 2 backetа: products и stage.

### Товары

1. Сгенерировать тестовый parquet:
   ```bash
   python make_products.py
   ```
2. Загрузить в MinIO:
   ```bash
   mc alias set myminio http://localhost:9002 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD
   mc cp sample_products.parquet myminio/products/
   ```
   Можно через сайт, http://localhost:9001, открывает backet products и добавляем файл .parquet через Upload

---

## API

```bash
uvicorn main:app --reload --port 8000 &
```

## Запуск streaming-консьюмера

```bash
python streaming/consumer.py
```

- Автоматически создаст таблицу `purchases_rt` в ClickHouse, если ещё не создана.  
- Будет слушать топик `purchases` и записывать каждое сообщение.

---

## Kafka + API

Заходим на http://127.0.0.1:8000/docs, добавляем покупки через Try it out -> Execute.



## Проверка ClickHouse

Проверить число записей в real-time таблице:
```bash
curl 'http://localhost:8123/?query=SELECT+count()+FROM+purchases_rt'
```

Выводит количество покупок.

---

## Запуск batch ETL (Stage)

Перед запуском убедитесь, что в Kafka есть покупки (см. следующий раздел).

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages \
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,\
org.postgresql:postgresql:42.7.2,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
software.amazon.awssdk:bundle:2.17.119 \
  /workspace/etl/stage_etl.py
```

Создаст Iceberg-таблицы:
- `spark_catalog.default.stage_clients`
- `spark_catalog.default.stage_sellers`
- `spark_catalog.default.stage_products`
- `spark_catalog.default.stage_purchases`

Можно проверить, что в minio в backet stage создалась папка warehouse, в который будут лежать все данные stage слоя.

---

## Проверка результатов Stage ETL

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4,software.amazon.awssdk:bundle:2.17.119 \
  /workspace/etl/count_stage.py
```

Ожидаемый вывод:
```
stage_clients: 2 rows
stage_sellers: 2 rows
stage_products: 5 rows
stage_purchases: X rows
```

---  

## Вывод анализа данных со Stage
```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=spark-master \
  --conf spark.executor.host=spark-worker \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,software.amazon.awssdk:bundle:2.17.119 \
  /workspace/analytics/report.py
```

Ожидаемый вывод таблиц:
- `Таблица stage_clients`
- `Таблица stage_sellers`
- `Таблица stage_products`
- `Таблица stage_purchases`  

Ожидаемый вывод аналитики:ы
- `Общая выручка по категориям`
- `Топ 10 клиентов`
- `Средний чек по дням`
- `Продукты с наибольшим количеством продаж`

---  

## Полезные команды

- Остановка всех: `docker compose down`  
- Просмотр логов Kafka: `docker compose logs -f kafka`  
- Просмотр топиков:  
  ```bash
  docker compose exec kafka     kafka-topics --bootstrap-server kafka:9092 --list
  ```
