#!/usr/bin/env bash
set -e

NETWORK="docker_default"      # если другое имя — поменяйте

# ждём, пока MinIO начнёт отвечать
echo "⏳  Жду старта MinIO..."
until docker run --rm --network $NETWORK curl -s http://minio:9000/minio/health/ready >/dev/null; do
  sleep 1
done
echo "✅  MinIO готов!"

# одна команда в одном контейнере  → alias сохраняется на время выполнения
docker run --rm --network $NETWORK \
  -e MINIO_ROOT_USER=$MINIO_ROOT_USER \
  -e MINIO_ROOT_PASSWORD=$MINIO_ROOT_PASSWORD \
  minio/mc sh -c "
    mc alias set myminio http://minio:9000 \$MINIO_ROOT_USER \$MINIO_ROOT_PASSWORD && \
    echo '→ Cоздаю bucket products' && mc mb --ignore-existing myminio/products && \
    echo '→ Cоздаю bucket stage'    && mc mb --ignore-existing myminio/stage
  "
