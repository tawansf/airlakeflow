"""
Bronze: ingestão de dados. Este exemplo usa dados MOCK (sem API externa)
para você rodar a pipeline completa logo após alf init.
"""
import json
import logging
from datetime import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def _mock_bitcoin_payload() -> dict:
    """Gera um payload simulado (como se viesse da API CoinGecko)."""
    now = int(datetime.now().timestamp())
    return {
        "bitcoin": {"usd": 43250.50, "last_updated_at": now},
        "processed_at": datetime.now().isoformat(),
    }


def bronze_ingestion_data_bitcoin():
    """Insere um registro mock em bronze.bitcoin_raw. Roda sem rede."""
    payload = _mock_bitcoin_payload()
    payload_str = json.dumps(payload)
    hook = PostgresHook(postgres_conn_id="postgres_datawarehouse")
    hook.run("INSERT INTO bronze.bitcoin_raw (payload) VALUES (%s);", parameters=[payload_str])
    logger.info("Bronze ingestão concluída: 1 registro mock em bronze.bitcoin_raw.")
