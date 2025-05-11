from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['your_email@example.com']
}

dag = DAG(
    'batch_processing',
    default_args=default_args,
    description='Daily batch processing of client and product data',
    schedule='0 0 * * *',
    start_date=datetime(2025, 5, 11),
    catchup=False
)

spark_job = SparkSubmitOperator(
    task_id='spark_batch_job',
    application='/Users/lowban/Desktop/MAI/IT-projects/Purchase-Analysis/batch_job.py',
    application_args=["--date", "{{ ds }}"],
    conf={
        "spark.driver.extraClassPath": "/Users/lowban/Desktop/MAI/IT-projects/Purchase-Analysis/postgresql-42.6.0.jar",
        "spark.master": "local[*]"
    },
    dag=dag
)