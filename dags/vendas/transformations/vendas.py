"""Transformação Silver para vendas. Ajuste a lógica conforme o schema bronze/silver."""
import logging
import os

logger = logging.getLogger(__name__)

from airflow.hooks.base import BaseHook


def get_jdbc_url_and_properties(conn_id: str = "postgres_datawarehouse"):
    conn = BaseHook.get_connection(conn_id)
    database = os.getenv("DATAWAREHOUSE_DB", "datawarehouse")
    jdbc_url = f"jdbc:postgresql://{conn.host}:{conn.port}/{database}"
    return jdbc_url, {
        "user": conn.login,
        "password": conn.password,
        "driver": "org.postgresql.Driver",
        "stringtype": "unspecified",
    }


def run_silver_vendas_transformation(jdbc_url: str, db_properties: dict) -> None:
    from pyspark.sql import SparkSession
    # TODO: ler bronze.vendas_raw, transformar, escrever em silver.vendas
    logger.info("Silver transformation vendas (Spark) - implementar leitura/escrita.")
