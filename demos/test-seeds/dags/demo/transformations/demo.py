"""Bronze -> Silver transformation for User and Task (demo)."""
import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def run_silver_demo_transformation() -> None:
    """Read bronze.user and bronze.task, write to silver.user and silver.task."""
    hook = PostgresHook(postgres_conn_id="postgres_datawarehouse")
    uri = hook.get_uri()
    if uri.startswith("postgres://"):
        uri = "postgresql://" + uri[len("postgres://") :]
    engine = create_engine(uri)
    try:
        for table in ("user", "task"):
            df = pd.read_sql(f'SELECT * FROM bronze.{table}', engine)
            if df.empty:
                logger.warning("Bronze.%s empty.", table)
                continue
            if "created_at" not in df.columns:
                df["created_at"] = datetime.now()
            if "updated_at" not in df.columns:
                df["updated_at"] = datetime.now()
            df.to_sql(
                table,
                engine,
                schema="silver",
                if_exists="replace",
                index=False,
                method="multi",
            )
            logger.info("Silver %s: %s row(s).", table, len(df))
    finally:
        engine.dispose()
