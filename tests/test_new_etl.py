"""Tests for alf new etl."""

from pathlib import Path

from click.testing import CliRunner

from airlakeflow.cli import main


def _minimal_project(tmp_path: Path) -> Path:
    """Create minimal project structure (dags, soda, migrations, docker-compose)."""
    (tmp_path / "dags").mkdir()
    (tmp_path / "dags" / "sql" / "migrations").mkdir(parents=True)
    (tmp_path / "soda").mkdir()
    (tmp_path / "docker-compose.yaml").write_text("services: {}")
    (tmp_path / ".airlakeflow.yaml").write_text("silver_backend: pandas\n")
    return tmp_path


def test_new_etl_creates_files(tmp_path):
    """alf new etl teste_etl --no-contracts --no-gold creates pipeline, bronze, silver, transformations."""
    proj = _minimal_project(tmp_path)
    runner = CliRunner()
    r = runner.invoke(
        main,
        [
            "new",
            "etl",
            "teste_etl",
            "-G",
            "-r",
            str(proj),
        ],
    )
    assert r.exit_code == 0
    domain = proj / "dags" / "teste_etl"
    assert (domain / "pipeline.py").exists()
    assert (domain / "bronze.py").exists()
    assert (domain / "silver.py").exists()
    assert not (domain / "gold.py").exists()
    assert (domain / "transformations" / "teste_etl.py").exists()
    assert "teste_etl" in (domain / "pipeline.py").read_text()
