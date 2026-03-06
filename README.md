# End-to-End Data Engineering Pipeline Framework

This repository serves as a robust, agnostic template for building scalable data pipelines. It implements a modern data stack architecture using containerization, distributed processing, and automated data quality checks, designed to be easily adaptable to various business domains.

## Architecture Overview

The pipeline follows the Medallion Architecture pattern (Bronze, Silver, Gold layers) and is fully containerized. The infrastructure is orchestrated to handle the complete data lifecycle:

1.  **Orchestration:** Apache Airflow manages the workflow dependencies and scheduling.
2.  **Ingestion (Bronze Layer):** Flexible extraction mechanisms to load raw data (JSON/CSV/API responses) into a persistent storage layer.
3.  **Distributed Processing (Silver Layer):** Apache Spark (PySpark) is utilized for high-performance data transformation, schema enforcement, and cleaning.
4.  **Data Quality Guardrails:** Soda Core is integrated into the pipeline to enforce schema validation and business rules before data is made available for consumption.

## Tech Stack

* **Orchestration:** Apache Airflow
* **Processing Engine:** Apache Spark (PySpark)
* **Storage / Data Warehouse:** PostgreSQL (adaptable to other SQL dialects)
* **Data Quality:** Soda Core (SodaCL)
* **Infrastructure:** Docker & Docker Compose
* **Language:** Python 3.12

## Repository Structure

* `dags/`: Airflow DAG definitions. One **domain** (e.g. crypto, vendas) = one subfolder with `bronze.py`, `silver.py`, `gold.py`, optional `transformations/`, and a `pipeline.py` for the full flow.
* `dags/sql/migrations/`: **Schema migrations** (versioned). Create tables/schemas for Bronze, Silver, and Gold. Run by the DAG `00_setup_database_migrations`. Naming: `V001__description.sql`, `V002__…`, etc.
* `scripts/`: **Infra scripts only** (e.g. Postgres init). Not schema migrations. Example: `001_init_datawarehouse.sql` runs once when the Postgres container starts and creates the `datawarehouse` database.
* `soda/`: Soda Core config and **data quality checks** (YAML). One file per table or layer (e.g. `bitcoin_bronze.yaml`, `bitcoin_silver.yaml`).
* `plugins/`: JDBC drivers (e.g. PostgreSQL) and custom Airflow plugins.
* `docker-compose.yaml`, `Dockerfile`: Multi-container environment (Airflow, Postgres, Redis, etc.).

## Pipeline Workflow

### 1. Ingestion & Raw Storage (Bronze)
The architecture supports raw data ingestion from agnostic sources (Rest APIs, S3, FTP). Data is stored in its native format (e.g., JSONB) to ensure full data lineage and allow for future re-processing without improved logic.

### 2. Transformation & Refinement (Silver)
PySpark jobs are responsible for:
* Reading raw data from the Bronze layer.
* Flattening complex structures.
* Applying type casting and schema enforcement.
* Handling deduplication strategies (e.g., SCD Type 1 or 2).
* Writing optimized data to the Silver layer.

### 3. Automated Quality Gates
Before the pipeline completes, Soda Core scans the transformed data against defined contracts:
* **Schema Validation:** Ensures columns and data types match expectations.
* **Freshness Checks:** Verifies that data is up-to-date within the expected SLA.
* **Business Rules:** Custom SQL-based checks to validate domain-specific logic (e.g., non-negative values, valid categorical options).

## Getting Started

### Prerequisites
* Docker Engine
* Docker Compose

### Installation & Execution

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/tawansf/data-engineering.git
    or 
    git clone git@github.com:tawansf/data-engineering.git
    ```

2.  **Environment Configuration:**
    Copy `.env.example` to `.env` and fill in the values. The file manages API URLs, paths, and (optionally) credentials for the pipeline and Soda.

3.  **Build the Infrastructure:**
    Since this project uses a custom Docker image to support Java/Spark within Airflow, a build step is required:
    ```bash
    docker compose build
    ```

4.  **Initialize Services:**
    Start the orchestration and database containers:
    ```bash
    docker compose up -d
    ```

5.  **Access the Orchestrator:**
    Navigate to `http://localhost:8080` to access the Airflow UI and trigger the DAGs. The **crypto_pipeline** DAG runs the full flow (Bronze → Quality Bronze → Silver → Quality Silver → Gold) on an hourly schedule. The DAGs `01_bronze_*`, `02_silver_*`, `03_silver_quality_*`, and `04_gold_*` can be run individually for ad-hoc use. Run **00_setup_database_migrations** once (or after adding new migrations) to create schemas and tables, including `gold.bitcoin_daily`.

6.  **Configure the Data Warehouse connection (required for pipeline DAGs):**
    The database `datawarehouse` is created automatically on the first `docker compose up` (via `scripts/001_init_datawarehouse.sql`). If the Postgres volume already existed before adding this script, create the database manually: `docker compose exec postgres psql -U airflow -d airflow -c "CREATE DATABASE datawarehouse;"`.

    In the Airflow UI, add the connection used by the Bronze/Silver pipeline:
    * Go to **Admin** → **Connections** → **+** (Add a new record).
    * Set **Connection Id:** `postgres_datawarehouse`
    * **Connection Type:** Postgres
    * **Host:** `postgres`
    * **Schema:** `datawarehouse` (this is the database name for the connection)
    * **Login:** `airflow`
    * **Password:** `airflow`
    * **Port:** `5432`
    * Save.

## Runbook (operação)

* **Ingestão Bronze falha (API ou timeout):** Verifique logs da task `bronze_ingestion_data_bitcoin`. Confirme `GEEKO_URL_API` no `.env` e conectividade. O DAG faz 2 retries com 1 minuto de intervalo; se persistir, revise rate limit da API ou aumente `API_TIMEOUT_SECONDS` / `API_MAX_RETRIES`.
* **Soda falha (quality):** Abra o log da task `soda_scan_silver_bitcoin` ou `soda_scan_bronze_bitcoin` e veja qual check falhou (Soda imprime o nome). Ajuste os arquivos em `soda/checks/` ou os dados na tabela. Para credenciais do Soda, edite `soda/configuration.yaml` ou use variáveis de ambiente em produção.
* **Re-rodar apenas Silver (ou Gold):** No Airflow, use o DAG `02_silver_transformation_data_bitcoin` ou `04_gold_aggregate_bitcoin_daily` e dispare uma run manual. Para re-processar apenas uma data, seria necessário um parâmetro ou DAG com conf (não implementado por padrão).
* **Onde ver logs:** Airflow UI → DAG run → task → “Log”. Logs do scheduler/worker também em `logs/` no projeto.

## CLI (AirLakeFlow / alf)

The project includes a CLI **AirLakeFlow** with alias **`alf`**. Commands use spaces (no hyphens): `alf new etl`, `alf new contract`, `alf init`, etc.

### Install (editable, from repo root)

```bash
pip install -e .
# or run without installing: PYTHONPATH=. python3 -m airlakeflow.cli ...
```

### Commands

* **`alf new etl NAME [options]`** — Generate a new ETL pipeline in the current project (no migrations; create them with `alf new migration`).
  * `NAME`: domain name (e.g. `vendas`, `weather`). Creates `dags/<name>/`, pipeline, bronze/silver/gold, transformations, and optionally Soda contracts.
  * `--contracts` / `--no-contracts`: generate Soda 4 contracts (bronze + silver). Default: no.
  * `--gold` / `--no-gold`: include gold layer. Default: yes.
  * `--source api|file|jdbc`: bronze ingestion type. Default: `api`.
  * `--no-spark`: silver without Spark (Python-only placeholder).
  * `--table-name NAME`: table/base name (default: same as NAME).
  * `--project-root PATH`: project root (default: current directory).

  Example:
  ```bash
  alf new etl vendas --contracts --project-root .
  ```

* **`alf new migration NAME [options]`** — Create a migration for an existing DAG. You choose the DAG (or are prompted) and the layer (bronze, silver, gold). Creates a new `V0XX__<name>.sql` with a placeholder table (default columns); edit the file to match your schema.
  * `NAME`: short description for the migration (e.g. `setup_bronze_csgostats`).
  * `--dag NAME`: DAG folder name (e.g. `csgostats`, `vendas`). If omitted, the CLI lists DAGs to choose from.
  * `--layer bronze|silver|gold`: layer. If omitted, you are prompted.
  * `--project-root PATH`: project root (default: current directory).

  Example:
  ```bash
  alf new migration setup_bronze_csgostats --dag csgostats --layer bronze
  alf new migration minha_migration   # prompts for DAG and layer
  ```

* **`alf new contract SCHEMA TABLE [options]`** — Generate a Soda contract for an existing table (in development). For now, use `alf new etl NAME --contracts` to generate contracts with the ETL.

* **`alf new layer NAME [options]`** — Generate a new layer or resource (in development).

* **`alf init [DEST]`** — Create a new project by copying the framework structure into `DEST` (default: current dir).
  * `--demo` / `--no-demo`: include crypto demo DAG. Default: yes.
  * `--with-monitoring` / `--no-monitoring`: include monitoring schema and Soda report. Default: yes.

### Migrations (auto-discovery)

The DAG `00_setup_database_migrations` **discovers all** `V*.sql` files in `dags/sql/migrations/` and runs them in order. You do **not** need to edit `setup_database.py` when adding new migrations; just add a new file (e.g. `V007__setup_bronze_vendas.sql`) and run the setup DAG.

---

## Adding a new pipeline (new domain)

You can reuse this structure for any new data source (APIs, files, streams). The fastest way is to use the CLI (see above). Alternatively, follow the same pattern manually: **one domain = one folder under `dags/`**, with Bronze → Silver → Gold and quality checks.

### Step-by-step (manual)

1. **Create the domain folder**  
   Example: `dags/vendas/` (or use `alf new etl vendas`).

2. **Bronze (ingestion)**  
   Add a DAG file (e.g. `bronze.py`) that reads from your source (API, file, etc.) and writes **raw** data to Postgres (e.g. `bronze.vendas_raw` with a `payload` JSONB column). Reuse the same connection `postgres_datawarehouse`. Optionally add retries and timeout like in `crypto/bronze.py`.

3. **Migrations**  
   In `dags/sql/migrations/`, add a new version (e.g. `V007__setup_bronze_vendas.sql`). The DAG `00_setup_database_migrations` picks up all `V*.sql` files automatically; no need to edit `setup_database.py`. Run the setup DAG once after adding the file.

4. **Silver (transformation)**  
   Add a transformation that reads from the new Bronze table, cleans/normalizes, and writes to a Silver table. You can put the Spark (or SQL) logic in `dags/<domain>/transformations/` and call it from a `silver.py` DAG, following the `crypto` example.

5. **Gold (aggregation)**  
   If needed, add a Gold table and a task that aggregates from Silver (e.g. daily rollups). See `crypto/gold.py` and the gold migration files.

6. **Quality (Soda)**  
   In `soda/contracts/` (Soda 4), add YAML contracts for the new tables (e.g. `vendas_bronze.yaml`, `vendas_silver.yaml`). Add PythonOperator tasks in your pipeline that call `run_soda_scan_and_persist` with the contract paths.

7. **Unified pipeline DAG**  
   Create a `pipeline.py` that chains: ingestion → quality_bronze → transformation → quality_silver → gold_aggregate. Set `schedule_interval` and `default_args` as in `crypto/pipeline.py`.

### Conventions

* **Migrations:** Always in `dags/sql/migrations/`, named `V00X__short_description.sql`. Run via `00_setup_database_migrations`.
* **Scripts vs migrations:** Use `scripts/` only for infra (e.g. creating a database at container init). Use `sql/migrations/` for all schema changes (tables, indexes).
* **Config:** Prefer environment variables (e.g. in `.env`) for URLs and paths. For many domains, use prefixed vars (e.g. `CRYPTO_API_URL`, `VENDAS_API_URL`) to keep config clear.
* **Other sources:** The same Medallion flow applies to CSV, Parquet, streams, etc.: ingest raw into Bronze, transform into Silver, aggregate into Gold, and add Soda checks per layer.

## Customization

To adapt this framework to your specific use case:
1.  Point the extraction logic in your domain folder to your data source (API, file, stream).
2.  Adjust the Spark (or SQL) transformation to match your target schema.
3.  Define quality rules in `soda/checks/` for each new table or layer.

## Maintainer

**Tawan Silva** *Senior Software Developer & Data Engineering Enthusiast*

This project is maintained by Tawan Silva as part of a continuous study on high-performance data architectures.

* [LinkedIn](https://www.linkedin.com/in/tawansf/)
* [GitHub](https://github.com/tawansf)