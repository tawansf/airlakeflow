"""Tests for migration_gen: generate migrations only for models that don't have one."""

from pathlib import Path

from airlakeflow.migration_gen import generate_migrations


def _project_with_one_model(tmp_path: Path) -> Path:
    """Create minimal project with one model (silver.example)."""
    (tmp_path / "config" / "models").mkdir(parents=True)
    (tmp_path / "dags" / "sql" / "migrations").mkdir(parents=True)
    (tmp_path / ".airlakeflow.yaml").write_text(
        "architecture: medallion\nmigration_driver: postgres\n"
    )
    (tmp_path / "config" / "models" / "example.py").write_text('''
"""Example model."""
from airlakeflow.models import Model, Field, layer

@layer("silver")
class ExampleModel(Model):
    __table__ = "example"
    id = Field.serial(primary_key=True)
    name = Field.varchar(255, nullable=False)
    created_at = Field.timestamp(default="CURRENT_TIMESTAMP")
    updated_at = Field.timestamp(nullable=False)
''')
    return tmp_path


def test_generate_migrations_no_models_returns_empty(tmp_path):
    (tmp_path / "config" / "models").mkdir(parents=True)
    (tmp_path / "dags" / "sql" / "migrations").mkdir(parents=True)
    (tmp_path / ".airlakeflow.yaml").write_text("migration_driver: postgres\n")
    created = generate_migrations(tmp_path, driver="postgres")
    assert created == []


def test_generate_migrations_creates_one_file_for_one_model(tmp_path):
    _project_with_one_model(tmp_path)
    created = generate_migrations(tmp_path, driver="postgres")
    assert len(created) == 1
    assert created[0].name.startswith("V")
    assert "setup_silver_example" in created[0].name
    assert created[0].suffix == ".sql"
    content = created[0].read_text()
    assert "CREATE TABLE" in content
    assert "silver.example" in content or "silver . example" in content.replace(" ", "")


def test_generate_migrations_skips_model_that_already_has_migration(tmp_path):
    root = _project_with_one_model(tmp_path)
    # First run: creates V001__setup_silver_example.sql
    first = generate_migrations(root, driver="postgres")
    assert len(first) == 1
    # Second run: should create nothing (migration already exists)
    second = generate_migrations(root, driver="postgres")
    assert len(second) == 0


def test_generate_migrations_creates_only_for_new_model(tmp_path):
    root = _project_with_one_model(tmp_path)
    # Generate for example
    generate_migrations(root, driver="postgres")
    # Add second model
    (root / "config" / "models" / "vendas.py").write_text('''
"""Vendas model."""
from airlakeflow.models import Model, Field, layer

@layer("silver")
class VendasModel(Model):
    __table__ = "vendas"
    id = Field.serial(primary_key=True)
    total = Field.numeric(18, 8)
    created_at = Field.timestamp(default="CURRENT_TIMESTAMP")
    updated_at = Field.timestamp(nullable=False)
''')
    created = generate_migrations(root, driver="postgres")
    # Should create only the new one (vendas)
    assert len(created) == 1
    assert "vendas" in created[0].name


def test_generate_migrations_uses_given_migrations_dir(tmp_path):
    root = _project_with_one_model(tmp_path)
    custom_dir = tmp_path / "custom" / "migrations"
    custom_dir.mkdir(parents=True)
    created = generate_migrations(root, driver="postgres", migrations_dir=custom_dir)
    assert len(created) == 1
    assert created[0].parent == custom_dir
