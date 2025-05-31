FROM python:3.10-slim

WORKDIR /app

# Копируем зависимые файлы
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем сам скрипт
COPY consumer.py .

# По умолчанию запускаем консьюмера
CMD ["python", "consumer.py"]
