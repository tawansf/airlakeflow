from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

from my_kafka.bronze import bronze_ingestion_data_my_kafka
from my_kafka.silver import silver_transformation_data_my_kafka

load_dotenv()

SODA_PATH = os.getenv("SODA_PATH", "/opt/airflow/soda")


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "owner": "airflow",
}

with DAG(
    dag_id="my_kafka_pipeline",
    start_date=datetime(2026, 2, 10),
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["my_kafka", "pipeline", "bronze", "silver", "quality", "gold"],
) as dag:
    ingestion = PythonOperator(
        task_id="bronze_ingestion_data_my_kafka",
        python_callable=bronze_ingestion_data_my_kafka,
    )
    transformation = PythonOperator(
        task_id="silver_transformation_data_my_kafka",
        python_callable=silver_transformation_data_my_kafka,
    )

    ingestion >> transformation
