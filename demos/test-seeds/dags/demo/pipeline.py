"""
Demo pipeline: User + Task (Bronze -> Silver -> Gold).
Run 00_setup_database_migrations and 00_seeds first, then this DAG.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from demo.bronze import bronze_ingestion_demo
from demo.silver import silver_transformation_demo
from demo.gold import gold_aggregate_demo

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "owner": "airflow",
}

with DAG(
    dag_id="demo_pipeline",
    start_date=datetime(2026, 2, 10),
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["demo", "pipeline", "bronze", "silver", "gold", "user", "task"],
) as dag:
    ingestion = PythonOperator(
        task_id="bronze_ingestion_demo",
        python_callable=bronze_ingestion_demo,
    )
    transformation = PythonOperator(
        task_id="silver_transformation_demo",
        python_callable=silver_transformation_demo,
    )
    gold_aggregate = PythonOperator(
        task_id="gold_aggregate_demo",
        python_callable=gold_aggregate_demo,
    )
    ingestion >> transformation >> gold_aggregate
