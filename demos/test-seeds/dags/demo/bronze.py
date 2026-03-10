"""
Bronze: seed data or mock ingestion for User and Task.
Run 00_seeds first to load data/seeds/bronze/*.csv, or this inserts one mock row each.
"""
import logging
from datetime import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def bronze_ingestion_demo():
    """Ensure bronze.user and bronze.task have data (mock if empty). Run 00_seeds first for full data."""
    hook = PostgresHook(postgres_conn_id="postgres_datawarehouse")
    # Insert one mock row per table if empty (id is SERIAL)
    hook.run(
        """
        INSERT INTO bronze.user (name, created_at, updated_at)
        SELECT 'Demo User', NOW(), NOW()
        WHERE NOT EXISTS (SELECT 1 FROM bronze.user LIMIT 1);
        """
    )
    hook.run(
        """
        INSERT INTO bronze.task (user_id, title, created_at, updated_at)
        SELECT 1, 'Demo Task', NOW(), NOW()
        WHERE NOT EXISTS (SELECT 1 FROM bronze.task LIMIT 1);
        """
    )
    logger.info("Bronze demo: user and task ready.")
