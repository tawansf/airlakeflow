# AirLakeFlow

Framework and CLI to build and run data pipelines using the **Medallion** pattern (Bronze → Silver → Gold) with **Apache Airflow**.

**Other languages:** [Português (pt-BR)](docs/translations/README.pt-BR.md)

---

## What the framework does

- **Initializes projects** with a ready-made structure (DAGs, Soda, Docker Compose, migrations).
- **Generates ETL pipelines** by domain (bronze, silver, gold, optionally Soda contracts).
- **Creates versioned SQL migrations** per layer (bronze/silver/gold).
- **Validates** project structure and environment (Docker, required files).
- **Runs the application** (up, stop, restart, logs) via Docker Compose.

All through the `**alf**` command (alias: `airlakeflow`).

---

## Installation

```bash
pip install -e .
```

Requirements: **Python 3.10+**. To run generated projects: **Docker** and **Docker Compose**.

### Framework development (repo root)

To work on AirLakeFlow code at the repository root, create a venv and install the package in editable mode:

```bash
python3 -m venv .venv
source .venv/bin/activate   # Linux/macOS
# or:  .venv\Scripts\activate   # Windows
pip install -e .
alf --version
```

The `.venv` lives at the repo root; demos in `demos/` use their own environments (e.g. `demos/full-example/venv`). For development: `pip install -e ".[dev]"` (adds pytest, ruff, black). Run tests: `pytest tests/`. Lint: `ruff check src tests`. Format: `black src tests`.

---

## Quick start

```bash
# 1. Create a new project
alf init my-project
cd my-project

# 2. Create an ETL pipeline
alf new etl sales
alf new migration setup_bronze_sales --dag sales --layer bronze
# (edit dags/sql/migrations/ and logic in dags/sales/)

# 3. Optional: add quality with Soda
alf add soda --etl sales

# 4. Validate and run
alf validate
alf run
```

---

## Commands

### Project

| Command                               | Description                                                                                                                       |
| ------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `alf init [name]`                     | Create a new project (folder with dags/, soda/, docker-compose, etc.). Omit name to use the current directory.                   |
| `alf validate [--project-root PATH]`  | Check structure (dags/, soda/, docker-compose) and Docker (daemon, stack). Use `--no-docker` or `--no-stack` to narrow the check.  |


### ETL and migrations

| Command                   | Description                                                                                                            |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `alf new etl NAME`        | Generate an ETL pipeline (bronze, silver, gold, pipeline.py). Options: `--contracts`, `--no-gold`, `--source api|file|jdbc`. |
| `alf new migration NAME`  | Create a SQL migration (V0XX__name.sql). Choose DAG and layer (bronze/silver/gold) or use `--dag` and `--layer`.       |


### Quality

| Command                              | Description                                                                                                                          |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------ |
| `alf add soda [--etl NAME \| --all]` | Integrate Soda: config, contracts, and scan tasks in pipelines. With no option: interactive mode (lists ETLs + “Full project”).     |


### Docker (application)

| Command                    | Description                                                                                                                                 |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `alf run`                  | Start the stack in the background (`docker compose up -d`). Creates `.env` and `logs/` if missing; sets AIRFLOW_UID and Postgres port when possible. |
| `alf stop`                 | Stop containers.                                                                                                                           |
| `alf restart`              | Stop and start again.                                                                                                                      |
| `alf down [--volumes]`     | Tear down the stack (optionally remove volumes).                                                                                           |
| `alf logs [-f] [SERVICE]`  | Show service logs.                                                                                                                         |
| `alf ps`                   | List running containers.                                                                                                                   |


For any command that operates on a project you can pass `**--project-root PATH**` (default: current directory).

---

## Generated project structure

After `alf init name` or when using an example in `demos/`:

```
name/
  dags/              # Airflow DAGs (one subdir per domain)
    setup_database.py
    sql/migrations/  # V001__*.sql, V002__*.sql, ...
  soda/              # Soda config and contracts
    configuration.yaml
    contracts/
  scripts/           # Infra scripts (e.g. create DB)
  config/, plugins/, data/, logs/
  docker-compose.yaml
  Dockerfile
  .env.example, .env, requirements.txt
```

---

## Repository structure (framework)

| Folder               | Contents                                                                                                                                                          |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **src/airlakeflow/** | Framework code: CLI, templates, skeleton used by `alf init` ([src layout](https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/)).       |
| **demos/**           | Example projects: full-example (complete), test-project.                                                                                                            |
| **docs/**            | Reference documentation (see [docs/README.md](docs/README.md)). Translations: [docs/translations/](docs/translations/).                                            |
| **planning/**        | Planning and design docs (not end-user documentation).                                                                                                            |
| **tests/**           | Framework tests (pytest).                                                                                                                                         |


---

## License

[LICENSE](LICENSE).
