from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

from csgo.bronze import bronze_ingestion_data_csgo
from csgo.silver import silver_transformation_data_csgo
from csgo.gold import gold_aggregate_csgo_daily

load_dotenv()

SODA_PATH = os.getenv("SODA_PATH", "/opt/airflow/soda")


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "owner": "airflow",
}

with DAG(
    dag_id="csgo_pipeline",
    start_date=datetime(2026, 2, 10),
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["csgo", "pipeline", "bronze", "silver", "quality", "gold"],
) as dag:
    ingestion = PythonOperator(
        task_id="bronze_ingestion_data_csgo",
        python_callable=bronze_ingestion_data_csgo,
    )
    transformation = PythonOperator(
        task_id="silver_transformation_data_csgo",
        python_callable=silver_transformation_data_csgo,
    )
    gold_aggregate = PythonOperator(
        task_id="gold_aggregate_csgo_daily",
        python_callable=gold_aggregate_csgo_daily,
    )

    ingestion >> transformation >> gold_aggregate
