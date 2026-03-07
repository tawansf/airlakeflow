"""
Pipeline demo: Bronze -> Silver -> Gold with MOCK data.
Run this DAG after alf run to see the full pipeline without an external API.
Then replicate the logic in your own pipelines.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from crypto.bronze import bronze_ingestion_data_bitcoin
from crypto.silver import silver_transformation_data_bitcoin
from crypto.gold import gold_aggregate_bitcoin_daily

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "owner": "airflow",
}

with DAG(
    dag_id="crypto_pipeline",
    start_date=datetime(2026, 2, 10),
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["crypto", "pipeline", "bronze", "silver", "gold", "demo"],
) as dag:
    ingestion = PythonOperator(
        task_id="bronze_ingestion_data_bitcoin",
        python_callable=bronze_ingestion_data_bitcoin,
    )
    transformation = PythonOperator(
        task_id="silver_transformation_data_bitcoin",
        python_callable=silver_transformation_data_bitcoin,
    )
    gold_aggregate = PythonOperator(
        task_id="gold_aggregate_bitcoin_daily",
        python_callable=gold_aggregate_bitcoin_daily,
    )
    ingestion >> transformation >> gold_aggregate
