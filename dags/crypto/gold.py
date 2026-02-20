"""
Silver to Gold aggregation: bitcoin_daily.
Populates gold.bitcoin_daily from silver.bitcoin (aggregation by day).
"""
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

UPSERT_GOLD_BITCOIN_DAILY = """
INSERT INTO gold.bitcoin_daily (date, crypto_id, avg_price, min_price, max_price, records_count)
SELECT
    DATE(updated_at) AS date,
    crypto_id,
    AVG(price)::NUMERIC(18, 8) AS avg_price,
    MIN(price)::NUMERIC(18, 8) AS min_price,
    MAX(price)::NUMERIC(18, 8) AS max_price,
    COUNT(*)::INTEGER AS records_count
FROM silver.bitcoin
GROUP BY DATE(updated_at), crypto_id
ON CONFLICT (date, crypto_id) DO UPDATE SET
    avg_price = EXCLUDED.avg_price,
    min_price = EXCLUDED.min_price,
    max_price = EXCLUDED.max_price,
    records_count = EXCLUDED.records_count;
"""


def gold_aggregate_bitcoin_daily():
    """Updates the gold.bitcoin_daily table from silver.bitcoin."""
    pg_hook = PostgresHook(postgres_conn_id="postgres_datawarehouse")
    pg_hook.run(UPSERT_GOLD_BITCOIN_DAILY)
    logger.info("Gold bitcoin_daily aggregation completed.")
    return None


default_args_gold = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="04_gold_aggregate_bitcoin_daily",
    start_date=datetime(2026, 2, 10),
    schedule_interval=None,
    catchup=False,
    default_args=default_args_gold,
    tags=["gold", "crypto", "aggregation"],
) as dag:
    aggregate = PythonOperator(
        task_id="gold_aggregate_bitcoin_daily",
        python_callable=gold_aggregate_bitcoin_daily,
    )
    aggregate
