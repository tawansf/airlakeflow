"""Silver SCD2/snapshot for my_snapshot. Tracks history with valid_from, valid_to, is_current."""
import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

# Business key for SCD2 (change to your unique column(s))
SCD2_KEY = "id"


def run_silver_my_snapshot_transformation() -> None:
    """Read bronze.my_snapshot_raw, add SCD2 columns, merge into silver.my_snapshot."""
    hook = PostgresHook(postgres_conn_id="postgres_datawarehouse")
    uri = hook.get_uri()
    if uri.startswith("postgresql://"):
        uri = "postgresql+psycopg2://" + uri[len("postgresql://"):]
    engine = create_engine(uri)
    try:
        # TODO: adjust bronze query to your schema
        df = pd.read_sql(
            f'SELECT * FROM bronze."my_snapshot_raw"',
            engine,
        )
        if df.empty:
            logger.warning("Bronze empty; nothing to snapshot.")
            return

        now = datetime.now()
        df["valid_from"] = now
        df["valid_to"] = pd.NaT
        df["is_current"] = True

        if SCD2_KEY in df.columns:
            keys_to_close = df[SCD2_KEY].drop_duplicates().tolist()
            if keys_to_close:
                from sqlalchemy import text
                with engine.connect() as conn:
                    conn.execute(
                        text(
                            f'UPDATE silver."my_snapshot" SET valid_to = :now, is_current = false '
                            f'WHERE is_current = true AND "{SCD2_KEY}" = ANY(:keys)'
                        ),
                        {"now": now, "keys": keys_to_close},
                    )
                    conn.commit()
        # Insert new snapshot rows
        df.to_sql(
            "my_snapshot",
            engine,
            schema="silver",
            if_exists="append",
            index=False,
            method="multi",
        )
        logger.info("Silver my_snapshot SCD2: %s row(s) appended.", len(df))
    finally:
        engine.dispose()