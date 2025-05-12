#!/bin/bash

PROJECT_DIR="/Users/lowban/Desktop/MAI/IT-projects/Purchase-Analysis"
DATE=$(date +\%Y-\%m-\%d)

source "$PROJECT_DIR/.venv/bin/activate"

mkdir -p "$PROJECT_DIR/stage/data/date=$DATE"

spark-submit \
  --driver-class-path "$PROJECT_DIR/postgresql-42.6.0.jar" \
  "$PROJECT_DIR/generate.py" --date "$DATE"

spark-submit \
  --driver-class-path "$PROJECT_DIR/postgresql-42.6.0.jar" \
  --jars "$PROJECT_DIR/postgresql-42.6.0.jar" \
  "$PROJECT_DIR/batch_job.py" --date "$DATE"