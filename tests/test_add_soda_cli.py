"""Tests for alf add soda (CLI)."""

from pathlib import Path

from click.testing import CliRunner

from airlakeflow.cli import cli


def _project_with_etl_no_soda(tmp_path: Path) -> Path:
    """Project with dags/crypto/pipeline.py without Soda tasks."""
    (tmp_path / "dags").mkdir()
    (tmp_path / "dags" / "crypto").mkdir()
    (tmp_path / "dags" / "monitoring").mkdir()
    (tmp_path / "soda").mkdir()
    (tmp_path / "soda" / "contracts").mkdir()
    (tmp_path / "docker-compose.yaml").write_text("services: {}")
    # Pipeline without Soda
    (tmp_path / "dags" / "crypto" / "pipeline.py").write_text("""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from crypto.bronze import bronze_ingestion_data_bitcoin
from crypto.silver import silver_transformation_data_bitcoin
from crypto.gold import gold_aggregate_bitcoin_daily

load_dotenv()

default_args = {"retries": 2, "retry_delay": timedelta(minutes=1)}

with DAG(dag_id="crypto_pipeline", start_date=datetime(2026, 2, 10), schedule_interval="@hourly", catchup=False, default_args=default_args, tags=["crypto"]) as dag:
    ingestion = PythonOperator(task_id="bronze_ingestion_data_bitcoin", python_callable=bronze_ingestion_data_bitcoin)
    transformation = PythonOperator(task_id="silver_transformation_data_bitcoin", python_callable=silver_transformation_data_bitcoin)
    gold_aggregate = PythonOperator(task_id="gold_aggregate_bitcoin_daily", python_callable=gold_aggregate_bitcoin_daily)
    ingestion >> transformation >> gold_aggregate
""")
    return tmp_path


def test_add_soda_creates_contracts_and_injects(tmp_path):
    """alf add soda --etl crypto creates config/contracts and injects Soda into pipeline."""
    proj = _project_with_etl_no_soda(tmp_path)
    runner = CliRunner()
    r = runner.invoke(cli, ["add", "soda", "-e", "crypto", "-r", str(proj)])
    assert r.exit_code == 0
    assert (proj / "soda" / "configuration.yaml").exists()
    assert (proj / "soda" / "contracts" / "crypto_bronze.yaml").exists()
    assert (proj / "soda" / "contracts" / "crypto_silver.yaml").exists()
    pipeline_content = (proj / "dags" / "crypto" / "pipeline.py").read_text()
    assert "run_soda_scan_and_persist" in pipeline_content
    assert "quality_bronze" in pipeline_content or "soda_scan_bronze" in pipeline_content
