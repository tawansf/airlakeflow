from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from crypto.transformations.bitcoin import (
    get_jdbc_url_and_properties,
    run_silver_bitcoin_transformation,
)


def silver_transformation_data_bitcoin():
    jdbc_url, db_properties = get_jdbc_url_and_properties()
    run_silver_bitcoin_transformation(jdbc_url, db_properties)


default_args_silver = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="02_silver_transformation_data_bitcoin",
    start_date=datetime(2026, 2, 10),
    schedule_interval=None,
    catchup=False,
    default_args=default_args_silver,
    tags=["transformation", "spark", "silver", "crypto"],
) as dag:
    transformation_data_bitcoin = PythonOperator(
        task_id="silver_transformation_data_bitcoin",
        python_callable=silver_transformation_data_bitcoin,
    )
    transformation_data_bitcoin
