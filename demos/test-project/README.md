# AirLakeFlow project

Medallion-style data pipeline (Bronze / Silver / Gold) with Airflow and Soda.

## Structure

- `dags/` – Airflow DAGs and `setup_database.py` (runs SQL migrations from `dags/sql/migrations/`)
- `soda/` – Soda config and data quality contracts
- `scripts/` – Postgres init (e.g. create `datawarehouse` database)
- `config/`, `plugins/`, `data/` – Airflow config, plugins, and data dirs

## Quick start

1. Copy `.env.example` to `.env` and set `AIRFLOW_UID` if needed.
2. Run `docker compose up -d`.
3. Add pipelines with `alf new etl <name>` and migrations with `alf new migration <name>`.
