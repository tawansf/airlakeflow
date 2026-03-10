import logging
import os
from datetime import datetime
from dotenv import load_dotenv

from airflow.providers.postgres.hooks.postgres import PostgresHook

load_dotenv()

logger = logging.getLogger(__name__)


def bronze_ingestion_data_my_snapshot():
    """Bronze ingestion for my_snapshot. Source: api."""
    pg_hook = PostgresHook(postgres_conn_id="postgres_datawarehouse")
    # Example: API. Adjust the URL and logic.
    import json
    import requests
    url = os.getenv("MY_SNAPSHOT_API_URL", "https://api.example.com/data")
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()
    data["processed_at"] = datetime.now().isoformat()
    payload_str = json.dumps(data)
    insert_sql = """INSERT INTO bronze.my_snapshot_raw (payload) VALUES (%s);"""
    pg_hook.run(insert_sql, parameters=[payload_str])
    logger.info("Bronze ingestion my_snapshot completed.")