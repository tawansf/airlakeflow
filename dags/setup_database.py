from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="00_setup_database_migrations",
    start_date=datetime(2026, 2, 10),
    schedule_interval=None,
    catchup=False,
    tags=["setup", "migration"]
) as dag:

    migration_001 = PostgresOperator(
        task_id="migration_001_bronze",
        postgres_conn_id="postgres_datawarehouse",
        sql="sql/migrations/V001__setup_bronze.sql"
    )

    migration_002 = PostgresOperator(
        task_id="migration_002_silver",
        postgres_conn_id="postgres_datawarehouse",
        sql="sql/migrations/V002__setup_silver.sql"
    )

    migration_001 >> migration_002