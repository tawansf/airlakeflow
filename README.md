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

* `dags/`: Contains the Airflow DAG definitions. Designed to be modular, separating ingestion logic from transformation logic.
* `soda/`: Configuration files and YAML-based data quality checks. This structure allows for defining rules without altering the pipeline code.
* `plugins/`: Stores necessary JDBC drivers (e.g., PostgreSQL) and custom Airflow plugins to enable connectivity between Spark and the Data Warehouse.
* `docker-compose.yaml`: Defines the multi-container environment, including Airflow services, the metadata database, and the processing execution environment.
* `Dockerfile`: Custom image definition that extends the official Airflow image to include Java (OpenJDK) and Spark dependencies.

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
    Create a `.env` file in the root directory to manage sensitive credentials (database users, passwords, and API keys).

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
    Navigate to `http://localhost:8080` to access the Airflow UI and trigger the DAGs.

## Customization

To adapt this framework to your specific use case:
1.  Modify the extraction logic in the `dags/` folder to point to your specific data source.
2.  Update the Spark transformation scripts to reflect your target schema.
3.  Edit the `soda/checks/` YAML files to define quality rules relevant to your dataset.

## Maintainer

**Tawan Silva** *Senior Software Developer & Data Engineering Enthusiast*

This project is maintained by Tawan Silva as part of a continuous study on high-performance data architectures.

* [LinkedIn](https://www.linkedin.com/in/tawansf/)
* [GitHub](https://github.com/tawansf)