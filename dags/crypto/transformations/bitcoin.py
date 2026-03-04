import logging
import os
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)


def get_jdbc_url_and_properties(conn_id: str = "postgres_datawarehouse"):
    conn = BaseHook.get_connection(conn_id)
    database = os.getenv("DATAWAREHOUSE_DB", "datawarehouse")
    jdbc_url = f"jdbc:postgresql://{conn.host}:{conn.port}/{database}"
    driver = os.getenv("DRIVER_CLASS", "org.postgresql.Driver")
    return jdbc_url, {
        "user": conn.login,
        "password": conn.password,
        "driver": driver,
        "stringtype": "unspecified",
    }


def run_silver_bitcoin_transformation(
    jdbc_url: str,
    db_properties: dict,
) -> None:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col,
        current_timestamp,
        from_unixtime,
        get_json_object,
        lit,
        to_timestamp,
    )
    from pyspark.sql.types import DecimalType

    jar_path = os.getenv("JAR_PATH", "/opt/airflow/plugins/postgresql-42.2.18.jar")
    logger.info("Starting SparkSession for Silver Bitcoin...")

    spark = (
        SparkSession.builder.appName("Silver_Transformation_Bitcoin")
        .config("spark.jars", jar_path)
        .config("spark.driver.extraClassPath", jar_path)
        .config("spark.executor.extraClassPath", jar_path)
        .getOrCreate()
    )

    try:
        logger.info("Reading bronze.bitcoin_raw...")
        df_bronze = spark.read.jdbc(
            url=jdbc_url,
            table="bronze.bitcoin_raw",
            properties=db_properties,
        )

        df_silver = df_bronze.select(
            lit("usd").alias("currency"),
            lit("bitcoin").alias("crypto_id"),
            get_json_object(col("payload"), "$.bitcoin.usd")
            .cast(DecimalType(18, 8))
            .alias("price"),
            to_timestamp(
                from_unixtime(
                    get_json_object(col("payload"), "$.bitcoin.last_updated_at")
                )
            ).alias("updated_at"),
            current_timestamp().alias("created_at"),
            col("payload").alias("metadata"),
        )

        df_silver = df_silver.dropDuplicates(["crypto_id", "updated_at"])
        df_silver.show(5, truncate=False)

        logger.info("Writing to silver.bitcoin...")
        df_silver.write.jdbc(
            url=jdbc_url,
            table="silver.bitcoin",
            mode="append",
            properties=db_properties,
        )
        logger.info("Silver Bitcoin transformation completed.")
    finally:
        spark.stop()
