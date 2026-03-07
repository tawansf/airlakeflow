"""Tests for migration_doctor: drift detection and validation issues; align overwrites migrations."""

from pathlib import Path

from airlakeflow.migration_doctor import (
    align_migrations_to_models,
    doctor_models_vs_migrations,
)


def _project_model_and_migration(tmp_path: Path, table_ddl: str) -> Path:
    """Project with one model and one migration file (matching)."""
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
        table_ddl
    )
    return tmp_path


def test_doctor_returns_list(tmp_path):
    """Doctor runs without error and returns a list of issue strings."""
    root = _project_model_and_migration(
        tmp_path,
        "CREATE TABLE IF NOT EXISTS silver.example (id SERIAL, name VARCHAR(255), created_at TIMESTAMP, updated_at TIMESTAMP);",
    )
    issues = doctor_models_vs_migrations(root, "postgres")
    assert isinstance(issues, list)
    # With hand-written DDL, dialect may emit different format so we may get "diverges" or none
    for i in issues:
        assert isinstance(i, str)


def test_doctor_reports_validation_issues(tmp_path):
    """Doctor includes validator errors (e.g. CREATE SCHEMA in migration)."""
    (tmp_path / "config" / "models").mkdir(parents=True)
    (tmp_path / "dags" / "sql" / "migrations").mkdir(parents=True)
    (tmp_path / ".airlakeflow.yaml").write_text("migration_driver: postgres\n")
    (tmp_path / "dags" / "sql" / "migrations" / "V001__setup_schemas.sql").write_text(
        "CREATE SCHEMA IF NOT EXISTS silver;"
    )
    issues = doctor_models_vs_migrations(tmp_path, "postgres")
    assert any("[rule]" in i and "CREATE SCHEMA" in i for i in issues)


def test_doctor_reports_model_without_migration(tmp_path):
    """When a model has no matching migration file, doctor reports it."""
    root = _project_model_and_migration(tmp_path, "CREATE TABLE silver.other (id INT);")
    # Remove the migration that matches example so we have model without migration
    (root / "dags" / "sql" / "migrations" / "V001__setup_silver_example.sql").unlink()
    (root / "dags" / "sql" / "migrations" / "V001__setup_silver_other.sql").write_text(
        "CREATE TABLE silver.other (id INT);"
    )
    issues = doctor_models_vs_migrations(root, "postgres")
    assert any("no migration" in i.lower() or "has no migration" in i for i in issues)


def test_align_overwrites_migration_with_model_ddl(tmp_path):
    """align_migrations_to_models overwrites migration file with DDL from model."""
    root = _project_model_and_migration(tmp_path, "CREATE TABLE silver.example (id INT); -- old")
    path = root / "dags" / "sql" / "migrations" / "V001__setup_silver_example.sql"
    before = path.read_text()
    updated = align_migrations_to_models(root, "postgres")
    assert len(updated) == 1
    assert updated[0] == path
    after = path.read_text()
    assert after != before
    assert "CREATE TABLE" in after
    # Should contain columns from model (name, created_at, updated_at)
    assert "name" in after.lower() or "created_at" in after.lower()


def test_align_returns_empty_when_no_matching_migration(tmp_path):
    """When no migration file matches a model, align returns empty list."""
    root = _project_model_and_migration(tmp_path, "CREATE TABLE silver.example (id INT);")
    (root / "dags" / "sql" / "migrations" / "V001__setup_silver_example.sql").unlink()
    updated = align_migrations_to_models(root, "postgres")
    assert updated == []


def test_align_empty_dir_returns_empty(tmp_path):
    (tmp_path / "config" / "models").mkdir(parents=True)
    (tmp_path / "dags" / "sql" / "migrations").mkdir(parents=True)
    (tmp_path / ".airlakeflow.yaml").write_text("migration_driver: postgres\n")
    (tmp_path / "config" / "models" / "example.py").write_text("""
from airlakeflow.models import Model, Field, layer
@layer("silver")
class ExampleModel(Model):
    __table__ = "example"
    id = Field.serial(primary_key=True)
    created_at = Field.timestamp(default="CURRENT_TIMESTAMP")
    updated_at = Field.timestamp(nullable=False)
""")
    updated = align_migrations_to_models(tmp_path, "postgres")
    assert updated == []
