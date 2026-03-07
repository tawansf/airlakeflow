"""
Bronze -> Silver transformation with pandas (no Spark).
Use this module as a reference for your other pipelines.
"""
import json
import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def run_silver_bitcoin_transformation() -> None:
    """Read bronze.bitcoin_raw, transform and write to silver.bitcoin."""
    hook = PostgresHook(postgres_conn_id="postgres_datawarehouse")
    # Pandas read_sql/to_sql with DBAPI2 connection uses SQLite queries; use SQLAlchemy engine for PostgreSQL.
    uri = hook.get_uri()
    if uri.startswith("postgresql://"):
        uri = "postgresql+psycopg2://" + uri[len("postgresql://") :]
    engine = create_engine(uri)
    try:
        df = pd.read_sql(
            "SELECT id, data_ingestao, payload FROM bronze.bitcoin_raw ORDER BY id",
            engine,
        )

        if df.empty:
            logger.warning("Bronze empty; nothing to transform.")
            return

        rows = []
        for _, row in df.iterrows():
            try:
                payload = row["payload"] if isinstance(row["payload"], dict) else json.loads(row["payload"])
                btc = payload.get("bitcoin", {})
                price = float(btc.get("usd", 0))
                ts = btc.get("last_updated_at")
                updated_at = datetime.fromtimestamp(ts) if ts else (row["data_ingestao"] or datetime.now())
                rows.append({
                    "crypto_id": "bitcoin",
                    "currency": "usd",
                    "price": price,
                    "metadata": json.dumps(payload) if isinstance(payload, dict) else payload,
                    "updated_at": updated_at,
                })
            except (TypeError, KeyError, json.JSONDecodeError) as e:
                logger.warning("Row skipped (invalid payload): %s", e)

        if not rows:
            logger.warning("No valid records for Silver.")
            return

        silver_df = pd.DataFrame(rows)
        silver_df["created_at"] = datetime.now()
        silver_df = silver_df.drop_duplicates(subset=["crypto_id", "updated_at"])

        silver_df.to_sql(
            "bitcoin",
            engine,
            schema="silver",
            if_exists="append",
            index=False,
            method="multi",
        )
        logger.info("Silver Bitcoin: %s record(s) written.", len(silver_df))
    finally:
        engine.dispose()
