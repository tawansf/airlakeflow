"""Silver transformation for my_kafka. Adjust the logic according to the bronze/silver schema."""
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


def run_silver_my_kafka_transformation(jdbc_url: str, db_properties: dict) -> None:
    from pyspark.sql import SparkSession
    # TODO: read bronze.my_kafka_raw, transform, write to silver.my_kafka
    logger.info("Silver transformation my_kafka (Spark) - implement reading/writing.")
