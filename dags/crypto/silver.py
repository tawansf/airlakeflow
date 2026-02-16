from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

JAR_PATH = os.getenv("JAR_PATH")
DRIVER_CLASS = os.getenv("DRIVER_CLASS")

def silver_transformation_data_bitcoin():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, from_unixtime, to_timestamp, get_json_object, lit, current_timestamp
    from pyspark.sql.types import DecimalType

    conn = BaseHook.get_connection('postgres_datawarehouse')
    jdbc_url = f"jdbc:postgresql://{conn.host}:{conn.port}/{conn.schema}"

    db_properties = {
        "user": conn.login,
        "password": conn.password,
        "driver": DRIVER_CLASS,
        "stringtype": "unspecified"
    }

    print("Init SparkSession...")

    spark = SparkSession.builder \
        .appName("Silver_Transformation_Bitcoin") \
        .config("spark.jars", JAR_PATH) \
        .config("spark.driver.extraClassPath", JAR_PATH) \
        .config("spark.executor.extraClassPath", JAR_PATH) \
        .getOrCreate()
    
    print("Read data from bronze.bitcoin_raw...")
    
    df_bronze = spark.read.jdbc(
            url=jdbc_url,
            table="bronze.bitcoin_raw",
            properties=db_properties,
    )
        
    df_silver = df_bronze.select(
        lit("usd").alias("currency"),
        lit("bitcoin").alias("crypto_id"),
        
        get_json_object(col("payload"), "$.bitcoin.usd").cast(DecimalType(18, 2)).alias("price"),

        to_timestamp(from_unixtime(
            get_json_object(col("payload"), "$.bitcoin.last_updated_at")
        )).alias("updated_at"),
        
        current_timestamp().alias("created_at"),
        
        col("payload").alias("metadata")
    )

    df_silver = df_silver.dropDuplicates(["crypto_id", "updated_at"])

    df_silver.show(5, truncate=False)

    print("Store data into silver.bitcoin...")

    df_silver.write.jdbc(
        url=jdbc_url,
        table="silver.bitcoin",
        mode="append",
        properties=db_properties
    )

    print("Process completed successfully.")

    spark.stop()


with DAG(
    dag_id="02_silver_transformation_data_bitcoin",
    start_date=datetime(2026, 2, 10),
    schedule_interval=None,
    catchup=False,
    tags=["transformation", "spark", "silver", "crypto"]
) as dag:

    transformation_data_bitcoin = PythonOperator(
        task_id="silver_transformation_data_bitcoin",
        python_callable=silver_transformation_data_bitcoin
    )

    transformation_data_bitcoin