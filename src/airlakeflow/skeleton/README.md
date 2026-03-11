# AirLakeFlow project

Medallion-style data pipeline (Bronze / Silver / Gold) with Airflow and Soda.

**This demo runs without any manual adjustment:** after `alf init` and `alf run`, run the DAGs in order (setup → seeds → demo_pipeline). All features (models, migrations, seeds, pipeline, Soda or ALF-Checks) are included.

## Structure

- `dags/` – Airflow DAGs and `setup_database.py` (runs SQL migrations from `dags/sql/migrations/`)
- `dags/demo/` – **Pipeline demo** (User + Task, Bronze → Silver → Gold) — run it after setup and seeds
- `soda/` – Soda config and data quality contracts
- `scripts/` – Postgres init (e.g. create `datawarehouse` database)
- `config/`, `plugins/`, `data/` – Airflow config, plugins, and data dirs

## Quick start

1. Copy `.env.example` to `.env` and set `AIRFLOW_UID` if needed (or run `alf run` — it creates `.env` for you).
2. Start the stack: `alf run` (or `docker compose up -d`).
3. Open Airflow UI at http://localhost:8080 (user/password: airflow).
4. Run DAGs **in this order** (once each, then demo on schedule or manually):
   - **00_setup_database_migrations** — creates schemas (bronze, silver, gold) and tables from migrations.
   - **00_seeds** — loads `data/seeds/bronze/*.csv` into bronze (included at init when demo is enabled).
   - **demo_pipeline** — User + Task pipeline (Bronze → Silver → Gold).
5. Add your own pipelines with `alf new etl <name>` and migrations with `alf new migration <name>`.

## Models and migrations (optional)

You can define tables as **models** in `config/models/` and generate migrations from them:

- `alf new model <name> --layer silver` — creates a model class in `config/models/`.
- `alf migrations generate` — generates `dags/sql/migrations/*.sql` from models (uses `migration_driver` from `.airlakeflow.yaml`, default: postgres).

This keeps schema as the single source of truth; new SQL dialects (Oracle, SQL Server, etc.) can be added later.
