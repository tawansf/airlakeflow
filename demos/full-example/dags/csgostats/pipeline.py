from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

from csgostats.bronze import bronze_ingestion_data_csgostats
from csgostats.silver import silver_transformation_data_csgostats
from csgostats.gold import gold_aggregate_csgostats_daily
from monitoring.soda_persistence import run_soda_scan_and_persist

load_dotenv()

SODA_PATH = os.getenv("SODA_PATH", "/opt/airflow/soda")




def _soda_scan_bronze(**context):
    run_soda_scan_and_persist(
        data_source="postgres_datawarehouse",
        config_path=f"{SODA_PATH}/configuration.yaml",
        contract_path=f"{SODA_PATH}/contracts/csgostats_bronze.yaml",
        dag_id=context["dag"].dag_id,
        task_id=context["task"].task_id,
    )


def _soda_scan_silver(**context):
    run_soda_scan_and_persist(
        data_source="postgres_datawarehouse",
        config_path=f"{SODA_PATH}/configuration.yaml",
        contract_path=f"{SODA_PATH}/contracts/csgostats_silver.yaml",
        dag_id=context["dag"].dag_id,
        task_id=context["task"].task_id,
    )

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "owner": "airflow",
}

with DAG(
    dag_id="csgostats_pipeline",
    start_date=datetime(2026, 2, 10),
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["csgostats", "pipeline", "bronze", "silver", "quality", "gold"],
) as dag:
    ingestion = PythonOperator(
        task_id="bronze_ingestion_data_csgostats",
        python_callable=bronze_ingestion_data_csgostats,
    )
    transformation = PythonOperator(
        task_id="silver_transformation_data_csgostats",
        python_callable=silver_transformation_data_csgostats,
    )
    
    quality_bronze = PythonOperator(
        task_id="soda_scan_bronze_csgostats",
        python_callable=_soda_scan_bronze,
    )

    quality_silver = PythonOperator(
        task_id="soda_scan_silver_csgostats",
        python_callable=_soda_scan_silver,
    )

    gold_aggregate = PythonOperator(
        task_id="gold_aggregate_csgostats_daily",
        python_callable=gold_aggregate_csgostats_daily,
    )

    ingestion >> quality_bronze >> transformation >> quality_silver >> gold_aggregate