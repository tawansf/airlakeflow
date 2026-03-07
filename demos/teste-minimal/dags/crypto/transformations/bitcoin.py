"""
Transformação Bronze -> Silver com pandas (sem Spark).
Use este módulo como referência para suas outras pipelines.
"""
import json
import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def run_silver_bitcoin_transformation() -> None:
    """Lê bronze.bitcoin_raw, transforma e grava em silver.bitcoin."""
    hook = PostgresHook(postgres_conn_id="postgres_datawarehouse")
    # Pandas read_sql/to_sql com conexão DBAPI2 usa queries SQLite (ex.: sqlite_master);
    # usar engine SQLAlchemy para o dialect PostgreSQL.
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
            logger.warning("Bronze vazio; nada a transformar.")
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
                logger.warning("Linha ignorada (payload inválido): %s", e)

        if not rows:
            logger.warning("Nenhum registro válido para Silver.")
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
        logger.info("Silver Bitcoin: %s registro(s) escritos.", len(silver_df))
    finally:
        engine.dispose()
