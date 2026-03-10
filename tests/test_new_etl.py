"""Tests for alf new etl."""

from pathlib import Path

from click.testing import CliRunner

from airlakeflow.cli import cli


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
        cli,
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


def test_new_etl_partition_and_incremental(tmp_path):
    """alf new etl with --partition-by and --incremental-by adds hints in bronze and transformation."""
    proj = _minimal_project(tmp_path)
    runner = CliRunner()
    r = runner.invoke(
        cli,
        [
            "new",
            "etl",
            "part_etl",
            "-G",
            "--partition-by",
            "data_ref",
            "--incremental-by",
            "updated_at",
            "-r",
            str(proj),
        ],
    )
    assert r.exit_code == 0
    bronze = proj / "dags" / "part_etl" / "bronze.py"
    trans = proj / "dags" / "part_etl" / "transformations" / "part_etl.py"
    assert "data_ref" in bronze.read_text() and "updated_at" in bronze.read_text()
    assert "data_ref" in trans.read_text() and "updated_at" in trans.read_text()


def test_new_etl_snapshot_pattern(tmp_path):
    """alf new etl X --pattern snapshot creates transformation with SCD2 (valid_from, valid_to, is_current)."""
    proj = _minimal_project(tmp_path)
    runner = CliRunner()
    r = runner.invoke(
        cli,
        ["new", "etl", "snapshot_etl", "-G", "--pattern", "snapshot", "-r", str(proj)],
    )
    assert r.exit_code == 0
    trans = proj / "dags" / "snapshot_etl" / "transformations" / "snapshot_etl.py"
    assert trans.exists()
    content = trans.read_text()
    assert "valid_from" in content and "valid_to" in content and "is_current" in content
    assert "SCD2" in content
