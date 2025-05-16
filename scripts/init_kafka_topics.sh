#!/usr/bin/env bash
docker exec docker-kafka-1 kafka-topics \
  --bootstrap-server localhost:29092 \
  --create --topic purchases --partitions 3 --replication-factor 1

