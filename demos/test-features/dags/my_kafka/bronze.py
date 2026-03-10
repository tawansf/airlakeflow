import logging
import os
from datetime import datetime
from dotenv import load_dotenv

from airflow.providers.postgres.hooks.postgres import PostgresHook

load_dotenv()

logger = logging.getLogger(__name__)


def bronze_ingestion_data_my_kafka():
    """Bronze ingestion for my_kafka. Source: kafka."""
    pg_hook = PostgresHook(postgres_conn_id="postgres_datawarehouse")
    # Kafka stub: consume from topic, write to bronze. Configure KAFKA_BOOTSTRAP_SERVERS and topic.
    # from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook
    insert_sql = """INSERT INTO bronze.my_kafka_raw (payload) VALUES ('{"_source": "kafka", "placeholder": true}');"""
    pg_hook.run(insert_sql)
    logger.info("Bronze ingestion my_kafka completed.")