#!/bin/bash
cd ../docker


docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.postgresql:postgresql:42.7.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,software.amazon.awssdk:bundle:2.17.119 \
  /workspace/etl/stage_etl.py


docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4,software.amazon.awssdk:bundle:2.17.119 \
  /workspace/etl/count_stage.py