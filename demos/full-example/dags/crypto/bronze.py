from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging
import time
import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

GEEKO_URL_API = os.getenv("GEEKO_URL_API")
API_TIMEOUT_SECONDS = int(os.getenv("API_TIMEOUT_SECONDS", "30"))
API_MAX_RETRIES = int(os.getenv("API_MAX_RETRIES", "3"))
API_BACKOFF_BASE_SECONDS = float(os.getenv("API_BACKOFF_BASE_SECONDS", "1.0"))


def _fetch_bitcoin_data(url: str, timeout: int, max_retries: int, backoff_base: float) -> dict:
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, timeout=timeout)
            if response.status_code != 200:
            raise RuntimeError(
                f"API returned status {response.status_code} (expected 200). URL: {url}"
            )
            return response.json()
        except requests.exceptions.Timeout as e:
            last_error = RuntimeError(f"Timeout calling API after {timeout}s: {e}")
            logger.warning("Attempt %s/%s: timeout.", attempt, max_retries)
        except requests.exceptions.RequestException as e:
            last_error = RuntimeError(f"Network error calling API: {e}")
            logger.warning("Attempt %s/%s: network error.", attempt, max_retries)
        except json.JSONDecodeError as e:
            last_error = RuntimeError(f"API response is not valid JSON: {e}")
            logger.warning("Attempt %s/%s: invalid JSON.", attempt, max_retries)
        if attempt < max_retries:
            delay = backoff_base ** attempt
            time.sleep(delay)
    raise last_error


def bronze_ingestion_data_bitcoin():
    if not GEEKO_URL_API:
        raise ValueError("GEEKO_URL_API not configured (missing environment variable).")

    data = _fetch_bitcoin_data(
        GEEKO_URL_API,
        timeout=API_TIMEOUT_SECONDS,
        max_retries=API_MAX_RETRIES,
        backoff_base=API_BACKOFF_BASE_SECONDS,
    )
    data["processed_at"] = datetime.now().isoformat()
    payload_str = json.dumps(data)

    pg_hook = PostgresHook(postgres_conn_id="postgres_datawarehouse")
    insert_sql = """INSERT INTO bronze.bitcoin_raw (payload) VALUES (%s);"""
    pg_hook.run(insert_sql, parameters=[payload_str])

    logger.info("Bronze ingestion for Bitcoin completed: 1 record inserted into bronze.bitcoin_raw.")

default_args_bronze = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="01_bronze_ingestion_data_bitcoin",
    start_date=datetime(2026, 2, 10),
    schedule_interval=None,
    catchup=False,
    default_args=default_args_bronze,
    tags=['bronze', 'crypto', 'ingestion']
) as dag:
    ingestion_data_bitcoin = PythonOperator(
        task_id='bronze_ingestion_data_bitcoin',
        python_callable=bronze_ingestion_data_bitcoin
    )

    ingestion_data_bitcoin