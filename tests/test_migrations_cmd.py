"""Tests for migrations_cmd: run_doctor, run_align (with/without drift, -F)."""

from pathlib import Path
from unittest.mock import patch

from airlakeflow.migrations_cmd import run_align, run_doctor


def _project_with_drift(tmp_path: Path) -> Path:
    """Project with one model and one migration that doesn't match (drift)."""
    (tmp_path / "config" / "models").mkdir(parents=True)
    (tmp_path / "dags" / "sql" / "migrations").mkdir(parents=True)
    (tmp_path / ".airlakeflow.yaml").write_text(
        "architecture: medallion\nmigration_driver: postgres\n"
    )
    (tmp_path / "config" / "models" / "example.py").write_text("""
from airlakeflow.models import Model, Field, layer
@layer("silver")
class ExampleModel(Model):
    __table__ = "example"
    id = Field.serial(primary_key=True)
    name = Field.varchar(255, nullable=False)
    created_at = Field.timestamp(default="CURRENT_TIMESTAMP")
    updated_at = Field.timestamp(nullable=False)
""")
    (tmp_path / "dags" / "sql" / "migrations" / "V001__setup_silver_example.sql").write_text(
        "CREATE TABLE silver.example (id INT);"
    )
    return tmp_path


def test_run_doctor_returns_zero_when_aligned(tmp_path):
    """When no issues, run_doctor returns 0."""
    # Empty migrations dir and no models -> no issues
    (tmp_path / "config" / "models").mkdir(parents=True)
    (tmp_path / "dags" / "sql" / "migrations").mkdir(parents=True)
    (tmp_path / ".airlakeflow.yaml").write_text("migration_driver: postgres\n")
    code = run_doctor(tmp_path, "postgres")
    assert code == 0


def test_run_doctor_returns_one_when_validation_issues(tmp_path):
    """When migrations contain forbidden statements, run_doctor returns 1."""
    (tmp_path / "config" / "models").mkdir(parents=True)
    (tmp_path / "dags" / "sql" / "migrations").mkdir(parents=True)
    (tmp_path / ".airlakeflow.yaml").write_text("migration_driver: postgres\n")
    (tmp_path / "dags" / "sql" / "migrations" / "V001__setup_schemas.sql").write_text(
        "CREATE SCHEMA IF NOT EXISTS silver;"
    )
    code = run_doctor(tmp_path, "postgres")
    assert code == 1


def test_run_align_with_drift_and_force_updates_files(tmp_path):
    """With -F, run_align overwrites migrations without prompting."""
    root = _project_with_drift(tmp_path)
    path = root / "dags" / "sql" / "migrations" / "V001__setup_silver_example.sql"
    before = path.read_text()
    code = run_align(root, "postgres", force=True)
    assert code == 0
    after = path.read_text()
    assert after != before
    assert "name" in after or "created_at" in after


def test_run_align_no_drift_returns_zero(tmp_path):
    """When doctor reports no issues (e.g. no migrations), align returns 0 and does nothing."""
    (tmp_path / "config" / "models").mkdir(parents=True)
    (tmp_path / "dags" / "sql" / "migrations").mkdir(parents=True)
    (tmp_path / ".airlakeflow.yaml").write_text("migration_driver: postgres\n")
    # No models -> doctor returns no drift for model/migration; may have no issues
    code = run_align(tmp_path, "postgres", force=True)
    assert code == 0


def test_run_align_with_drift_without_force_cancels_when_user_says_no(tmp_path):
    """Without -F, when user chooses No, align exits 0 without changing files."""
    root = _project_with_drift(tmp_path)
    path = root / "dags" / "sql" / "migrations" / "V001__setup_silver_example.sql"
    before = path.read_text()

    with patch("questionary.select") as m_select:
        m_select.return_value.ask.return_value = "no"
        code = run_align(root, "postgres", force=False)

    assert code == 0
    assert path.read_text() == before


def test_run_align_with_drift_without_force_applies_when_user_says_yes(tmp_path):
    """Without -F, when user chooses Yes, align overwrites migrations."""
    root = _project_with_drift(tmp_path)
    path = root / "dags" / "sql" / "migrations" / "V001__setup_silver_example.sql"
    before = path.read_text()

    with patch("questionary.select") as m_select:
        m_select.return_value.ask.return_value = "yes"
        code = run_align(root, "postgres", force=False)

    assert code == 0
    assert path.read_text() != before
