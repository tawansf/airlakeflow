"""Gold: aggregate task count per user (demo)."""
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

GOLD_SQL = """
CREATE TABLE IF NOT EXISTS gold.user_task_summary (
    user_id INTEGER PRIMARY KEY,
    task_count INTEGER NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
INSERT INTO gold.user_task_summary (user_id, task_count, updated_at)
SELECT
    u.id AS user_id,
    COUNT(t.id)::INTEGER AS task_count,
    NOW() AS updated_at
FROM silver.user u
LEFT JOIN silver.task t ON t.user_id = u.id
GROUP BY u.id
ON CONFLICT (user_id) DO UPDATE SET
    task_count = EXCLUDED.task_count,
    updated_at = EXCLUDED.updated_at;
"""


def gold_aggregate_demo():
    pg_hook = PostgresHook(postgres_conn_id="postgres_datawarehouse")
    pg_hook.run(GOLD_SQL)
    logger.info("Gold user_task_summary updated.")
