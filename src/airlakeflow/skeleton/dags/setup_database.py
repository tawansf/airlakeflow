from pathlib import Path

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args_setup = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

_DAGS_DIR = Path(__file__).resolve().parent
_MIGRATIONS_DIR = _DAGS_DIR / "sql" / "migrations"


def _discover_migrations():
    if not _MIGRATIONS_DIR.exists():
        return []
    files = sorted(_MIGRATIONS_DIR.glob("V*.sql"))
    return [f.name for f in files]


def _migration_task_id(filename: str) -> str:
    base = filename.replace(".sql", "")
    parts = base.split("__", 1)
    num_str = parts[0].replace("V", "").lstrip("0") or "0"
    num = int(num_str)
    desc = parts[1].replace("-", "_") if len(parts) > 1 else "migration"
    return f"migration_{num:03d}_{desc}"


migration_files = _discover_migrations()

# Create bronze/silver/gold schemas first (in case scripts/ did not run, e.g. local or reused volume).
INIT_SCHEMAS_SQL = "../scripts/002_create_schemas.sql"

with DAG(
    dag_id="00_setup_database_migrations",
    start_date=datetime(2026, 2, 10),
    schedule_interval=None,
    catchup=False,
    default_args=default_args_setup,
    tags=["setup", "migration"],
) as dag:
    init_schemas = PostgresOperator(
        task_id="init_schemas",
        postgres_conn_id="postgres_datawarehouse",
        sql=INIT_SCHEMAS_SQL,
    )
    tasks = []
    for i, filename in enumerate(migration_files):
        task_id = _migration_task_id(filename)
        sql_path = f"sql/migrations/{filename}"
        op = PostgresOperator(
            task_id=task_id,
            postgres_conn_id="postgres_datawarehouse",
            sql=sql_path,
        )
        tasks.append(op)
    for i in range(1, len(tasks)):
        tasks[i - 1] >> tasks[i]
    if tasks:
        init_schemas >> tasks[0]
