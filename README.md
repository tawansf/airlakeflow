# AirLakeFlow

[![CI](https://github.com/tawansf/airlakeflow/actions/workflows/ci.yml/badge.svg)](https://github.com/tawansf/airlakeflow/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Framework and CLI to build and run data pipelines using the **Medallion** pattern (Bronze → Silver → Gold) with **Apache Airflow**.

**Other languages:** [Portuguese (pt-BR)](docs/translations/README.pt-BR.md)

---

## What the framework does

- **Initializes projects** with a ready-made structure (DAGs, Soda, Docker Compose, migrations).
- **Generates ETL pipelines** by domain (bronze, silver, gold, optionally Soda contracts).
- **Creates versioned SQL migrations** per layer (bronze/silver/gold).
- **Validates** project structure and environment (Docker, required files).
- **Runs the application** (up, stop, restart, logs) via Docker Compose.
- **Quality:** Soda contracts or native ALF-Checks (config/checks/ + DAG).
- **Migrations:** SQL from Python models (config/models/) with layer/partition support.

All through the **`alf`** command (alias: `airlakeflow`).

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
alf version
```

The `.venv` lives at the repo root; demos in `demos/` use their own environments (e.g. `demos/full-example/venv`). For development: `pip install -e ".[dev]"` (adds pytest, ruff, black, bandit, pip-audit). Then:

```bash
pytest tests/ -q          # run tests
ruff check src tests      # lint
black src tests           # format
pip-audit                 # check dependencies for vulnerabilities (skips editable pkg)
bandit -r src -q -l       # security lint (fail on High only)
```

(If `pytest` is not in your PATH, use `python3 -m pytest tests/ -q`.)

---

## Quick start

```bash
# 1. Create a new project (demo User+Task included by default; use -D for minimal)
alf init my-project
cd my-project

# 2. Start the stack and run DAGs in order: 00_setup_database_migrations → 00_seeds → demo_pipeline
alf run
# Airflow UI: http://localhost:8080 — run setup, then seeds, then demo_pipeline

# 3. Or create your own ETL and models
alf new etl sales
alf new model product --layer silver
alf new contract silver product   # interactive: choose Soda or ALF-Checks
alf new migration setup_bronze_sales -d sales -l bronze

# 4. Quality: Soda or native ALF-Checks
alf add soda --etl sales
# or: alf add alf-checks   # config/checks/ + DAG 01_alf_checks

# 5. Validate and run
alf validate
alf run
```

---

## Commands

Commands are grouped in the CLI as **Project**, **Resources**, **Quality**, and **Docker (stack)**. Use `alf help` or `alf --help` for usage. For any command that operates on a project, pass **`-r PATH`** for project root (default: current directory).

### Project

| Command | Description |
| ------- | ----------- |
| `alf init [name]` | Create a new project. Demo (User+Task pipeline) included by default; use `-D` for minimal. Options: `-m` Soda, `-b` backend (pandas/pyspark), `-w`/`-W` Docker/local. |
| `alf upgrade [-n] [-B]` | Update project files from the framework skeleton (optional backup in `.airlakeflow_backup/`). |
| `alf validate [-r] [-N] [-S] [-q]` | Check structure (dags/, soda/, docker-compose) and Docker (daemon, stack). `-N` skip Docker, `-S` skip stack. |
| `alf doctor [-r] [-q]` | Extended validation: structure, Docker, Python, permissions; suggests fixes. |
| `alf help` | Show main help. |
| `alf version` | Show version. |

### Resources

| Command | Description |
| ------- | ----------- |
| `alf new etl [NAME]` | Create an ETL pipeline (bronze, silver, gold, pipeline.py). NAME optional in interactive mode. Options: `-t` table, `-c` Soda contracts, `-g`/`-G` gold, `-s` source (api/file/jdbc/...), `--pattern` default/snapshot, `--partition-by`, `--incremental-by`. |
| `alf new migration NAME` | Create a SQL migration (V0XX__name.sql). `-d` DAG, `-l` layer (bronze/silver/gold); prompted if omitted. |
| `alf new contract [SCHEMA TABLE]` | Create a contract. Interactive: choose **Soda** (soda/contracts/) or **ALF-Checks** (config/checks/{schema}/{table}.yaml). Schema/table prompted if omitted. |
| `alf new model NAME` | Create a model in config/models/ and generate its migration. `-l` layer, `--partition-by` column. |
| `alf list etls` | List ETL pipelines (dags/ folders with pipeline.py). |
| `alf migrations generate` | Generate migration SQL from config/models/. `-D` driver (postgres, etc.). |
| `alf migrations up` | Apply pending migrations. `-u` connection URI. |
| `alf migrations down` | Rollback last migration. `-n` dry run, `-F` force. |
| `alf migrations doctor` | Compare models with migrations; report drift. |
| `alf migrations align` | Align migrations to models (model is reference). `-F` skip confirm. |
| `alf seed` | Ensure data/seeds/ exists and generate DAG 00_seeds (loads data/seeds/bronze/*.csv and silver/*.csv). |
| `alf docs [-o DIR] [--format html\|json]` | Generate static catalog from models and migrations (docs/catalog.html or .json). |

### Quality

| Command | Description |
| ------- | ----------- |
| `alf add soda [-e ETL \| -a]` | Integrate Soda: config, contracts, scan tasks in ETLs. `-e` one ETL, `-a` all. |
| `alf add greatxp` | Great Expectations (in development). |
| `alf add alf-checks` | Add ALF-Checks (native): config/checks/, DAG 01_alf_checks. Alternative to Soda. |


### Docker (stack)

| Command | Description |
| ------- | ----------- |
| `alf run [-b] [-f]` | Start the application (Docker: compose up; local: airflow standalone). `-b` build images, `-f` foreground. |
| `alf stop` | Stop containers. |
| `alf restart` | Stop then start. |
| `alf down [-v]` | Tear down the stack. `-v` remove volumes. |
| `alf status` | Show stack status (how many services running). |
| `alf exec SERVICE COMMAND...` | Run a command inside a service container (e.g. `alf exec airflow-scheduler airflow dags list`). |
| `alf logs [-f] [SERVICE]` | Show container logs. |
| `alf ps` | List running services. |

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
  config/            # config/models/ (Python models), config/checks/ (ALF-Checks, after alf add alf-checks)
  scripts/, plugins/, data/, logs/
  docker-compose.yaml
  Dockerfile
  .env.example, .env, requirements.txt
```

---

## Repository structure (framework)

| Folder               | Contents                                                                                                                                                          |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **src/airlakeflow/** | Framework code: CLI, templates, skeleton used by `alf init` ([src layout](https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/)).       |
| **demos/**           | Example project: full-example (complete pipeline, Soda, monitoring).                                                                                                |
| **docs/**            | Reference documentation (see [docs/README.md](docs/README.md)). Translations: [docs/translations/](docs/translations/).                                            |
| **planning/**        | Planning and design docs (not end-user documentation).                                                                                                            |
| **tests/**           | Framework tests (pytest).                                                                                                                                         |


---

## Lint and format (Ruff + Black)

The project uses **Ruff** for linting and **Black** for code style. After `pip install -e ".[dev]"`:

```bash
ruff check src tests      # lint (import order, unused imports, etc.); use --fix to auto-fix
black src tests           # format code
```

Run both before committing so CI stays green. Ruff fixes many issues automatically (e.g. `ruff check src tests --fix`); Black has no fix mode, it just rewrites the files.

---

## License

[LICENSE](LICENSE).
