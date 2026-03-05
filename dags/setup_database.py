from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args_setup = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="00_setup_database_migrations",
    start_date=datetime(2026, 2, 10),
    schedule_interval=None,
    catchup=False,
    default_args=default_args_setup,
    tags=["setup", "migration"]
) as dag:

    migration_001 = PostgresOperator(
        task_id="migration_001_setup_schema",
        postgres_conn_id="postgres_datawarehouse",
        sql="sql/migrations/V001__setup_schema.sql"
    )

    migration_002 = PostgresOperator(
        task_id="migration_002_setup_bronze",
        postgres_conn_id="postgres_datawarehouse",
        sql="sql/migrations/V002__setup_bronze.sql"
    )

    migration_003 = PostgresOperator(
        task_id="migration_003_setup_silver",
        postgres_conn_id="postgres_datawarehouse",
        sql="sql/migrations/V003__setup_silver.sql"
    )

    migration_004 = PostgresOperator(
        task_id="migration_004_setup_gold",
        postgres_conn_id="postgres_datawarehouse",
        sql="sql/migrations/V004__setup_gold.sql"
    )

    migration_005 = PostgresOperator(
        task_id="migration_005_setup_soda",
        postgres_conn_id="postgres_datawarehouse",
        sql="sql/migrations/V005__monitoring_soda_metricas_agrupado.sql"
    )

    migration_006 = PostgresOperator(
        task_id="migration_006_soda_metricas_soda4_enrich",
        postgres_conn_id="postgres_datawarehouse",
        sql="sql/migrations/V006__soda_metricas_soda4_enrich.sql"
    )

    migration_001 >> migration_002 >> migration_003 >> migration_004 >> migration_005 >> migration_006