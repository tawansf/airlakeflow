"""Tests for alf new migration (CLI)."""

from pathlib import Path

from click.testing import CliRunner

from airlakeflow.cli import main


def _minimal_project_with_dag(tmp_path: Path) -> Path:
    """Minimal project with dags/crypto/pipeline.py so discover_dags finds crypto."""
    (tmp_path / "dags").mkdir()
    (tmp_path / "dags" / "sql" / "migrations").mkdir(parents=True)
    (tmp_path / "dags" / "crypto").mkdir()
    (tmp_path / "dags" / "crypto" / "pipeline.py").write_text("# pipeline")
    (tmp_path / "soda").mkdir()
    (tmp_path / "docker-compose.yaml").write_text("services: {}")
    return tmp_path


def test_new_migration_creates_file(tmp_path):
    """alf new migration nova_migration --dag crypto --layer silver creates V00X__nova_migration.sql."""
    proj = _minimal_project_with_dag(tmp_path)
    runner = CliRunner()
    r = runner.invoke(
        main,
        [
            "new",
            "migration",
            "nova_migration",
            "--dag",
            "crypto",
            "--layer",
            "silver",
            "--project-root",
            str(proj),
        ],
    )
    assert r.exit_code == 0
    migrations = list((proj / "dags" / "sql" / "migrations").glob("V*.sql"))
    assert len(migrations) == 1
    content = migrations[0].read_text()
    assert "silver" in content.lower()
    assert "crypto" in content.lower() or "nova_migration" in content
