"""
DAG unified crypto pipeline: Bronze → Silver → Quality → Gold.
Scheduled to run hourly; tasks with retry in case of transient failure.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

from crypto.bronze import bronze_ingestion_data_bitcoin
from crypto.silver import silver_transformation_data_bitcoin
from crypto.gold import gold_aggregate_bitcoin_daily

load_dotenv()

SODA_PATH = os.getenv("SODA_PATH", "/opt/airflow/soda")

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
    tags=["crypto", "pipeline", "bronze", "silver", "quality", "gold"],
) as dag:
    ingestion = PythonOperator(
        task_id="bronze_ingestion_data_bitcoin",
        python_callable=bronze_ingestion_data_bitcoin,
    )

    transformation = PythonOperator(
        task_id="silver_transformation_data_bitcoin",
        python_callable=silver_transformation_data_bitcoin,
    )

    quality_bronze = BashOperator(
        task_id="soda_scan_bronze_bitcoin",
        bash_command=f"""
        soda scan -d postgres_datawarehouse \
                  -c {SODA_PATH}/configuration.yaml \
                  {SODA_PATH}/checks/bitcoin_bronze.yaml
        """,
    )

    quality_silver = BashOperator(
        task_id="soda_scan_silver_bitcoin",
        bash_command=f"""
        soda scan -d postgres_datawarehouse \
                  -c {SODA_PATH}/configuration.yaml \
                  {SODA_PATH}/checks/bitcoin_silver.yaml
        """,
    )

    gold_aggregate = PythonOperator(
        task_id="gold_aggregate_bitcoin_daily",
        python_callable=gold_aggregate_bitcoin_daily,
    )

    ingestion >> quality_bronze >> transformation >> quality_silver >> gold_aggregate
