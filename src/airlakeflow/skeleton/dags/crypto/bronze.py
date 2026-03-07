"""
Bronze: data ingestion. This example uses MOCK data (no external API)
so you can run the full pipeline right after alf init.
"""
import json
import logging
from datetime import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def _mock_bitcoin_payload() -> dict:
    """Generate a mock payload (as if from CoinGecko API)."""
    now = int(datetime.now().timestamp())
    return {
        "bitcoin": {"usd": 43250.50, "last_updated_at": now},
        "processed_at": datetime.now().isoformat(),
    }


def bronze_ingestion_data_bitcoin():
    """Insert a mock record into bronze.bitcoin_raw. Runs without network."""
    payload = _mock_bitcoin_payload()
    payload_str = json.dumps(payload)
    hook = PostgresHook(postgres_conn_id="postgres_datawarehouse")
    hook.run("INSERT INTO bronze.bitcoin_raw (payload) VALUES (%s);", parameters=[payload_str])
    logger.info("Bronze ingestion done: 1 mock record in bronze.bitcoin_raw.")
