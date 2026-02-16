from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

SODA_PATH = os.getenv("SODA_PATH")

with DAG(
    dag_id="03_silver_quality_bitcoin",
    start_date=datetime(2026, 2, 10),
    schedule_interval=None,
    catchup=False,
    tags=["quality", "soda", "silver"]
) as dag:
    verify_silver = BashOperator(
        task_id="soda_scan_silver_bitcoin",
        bash_command=f"""
        soda scan -d postgres_datawarehouse \
                  -c {SODA_PATH}/configuration.yaml \
                  {SODA_PATH}/checks/bitcoin_silver.yaml
        """
    )

    verify_silver