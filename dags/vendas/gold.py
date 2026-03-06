import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

UPSERT_GOLD_VENDAS_DAILY = """
INSERT INTO gold.vendas_daily (date, entity_id, avg_value, min_value, max_value, records_count)
SELECT
    DATE(updated_at) AS date,
    entity_id,
    AVG(value)::NUMERIC(18, 8) AS avg_value,
    MIN(value)::NUMERIC(18, 8) AS min_value,
    MAX(value)::NUMERIC(18, 8) AS max_value,
    COUNT(*)::INTEGER AS records_count
FROM silver.vendas
GROUP BY DATE(updated_at), entity_id
ON CONFLICT (date, entity_id) DO UPDATE SET
    avg_value = EXCLUDED.avg_value,
    min_value = EXCLUDED.min_value,
    max_value = EXCLUDED.max_value,
    records_count = EXCLUDED.records_count;
"""


def gold_aggregate_vendas_daily():
    pg_hook = PostgresHook(postgres_conn_id="postgres_datawarehouse")
    pg_hook.run(UPSERT_GOLD_VENDAS_DAILY)
    logger.info("Gold vendas_daily aggregation completed.")