"""Compare models with migration files and report drift."""

from __future__ import annotations

import re
from pathlib import Path

from airlakeflow.dialects import get_dialect
from airlakeflow.model_loader import discover_models
from airlakeflow.models.base import Model


def _normalize_sql(text: str) -> str:
    """Normalize SQL for comparison: strip comments, collapse whitespace."""
    lines = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("--"):
            continue
        lines.append(" ".join(line.split()))
    return "\n".join(lines).strip()


def _migration_filename_to_schema_table(filename: str) -> tuple[str, str] | None:
    """Extract (schema, table) from V001__setup_silver_vendas.sql or None."""
    base = filename.replace(".sql", "")
    m = re.match(r"V\d+__setup_(\w+)_(\w+)$", base, re.IGNORECASE)
    if m:
        return (m.group(1).lower(), m.group(2).lower())
    return None


def _find_migration_for_model(migrations_dir: Path, schema: str, table: str) -> Path | None:
    """Find migration file for schema.table (e.g. V006__setup_silver_vendas.sql)."""
    needle = f"__setup_{schema}_{table}.sql"
    for p in migrations_dir.glob("V*.sql"):
        if p.name.endswith(needle):
            return p
    return None


def doctor_models_vs_migrations(project_root: Path, driver: str) -> list[str]:
    """Compare models with migrations and migrations with models. Returns list of issue messages (empty if aligned).
    Also reports migrations that contain forbidden statements (only CREATE TABLE/VIEW/INDEX allowed).
    """
    root = Path(project_root).resolve()
    migrations_dir = root / "dags" / "sql" / "migrations"
    if not migrations_dir.exists():
        return []

    from airlakeflow.migration_validator import validate_migrations_dir

    issues: list[str] = []
    validation_errors = validate_migrations_dir(migrations_dir)
    for err in validation_errors:
        issues.append(f"[regra] {err}")

    dialect = get_dialect(driver)
    models = discover_models(root)
    model_by_key = {(m.get_schema().lower(), m.get_table_name().lower()): m for m in models}

    # 1) Model → Migration: each model should have a matching migration with same DDL
    for model in models:
        schema = model.get_schema().lower()
        table = model.get_table_name().lower()
        path = _find_migration_for_model(migrations_dir, schema, table)
        if not path:
            issues.append(f"Model {model.__name__} ({schema}.{table}) has no migration file.")
            continue
        expected = dialect.emit_create_table(model)
        actual = path.read_text(encoding="utf-8")
        if _normalize_sql(expected) != _normalize_sql(actual):
            issues.append(
                f"Model {model.__name__} ({schema}.{table}) diverges from {path.name}"
            )

    # 2) Migration → Model: each setup_<schema>_<table> migration should have a model and match
    for path in sorted(migrations_dir.glob("V*.sql")):
        key = _migration_filename_to_schema_table(path.name)
        if not key:
            continue
        schema, table = key
        model = model_by_key.get((schema, table))
        if not model:
            issues.append(f"Migration {path.name} has no corresponding model in config/models/.")
            continue
        expected = dialect.emit_create_table(model)
        actual = path.read_text(encoding="utf-8")
        if _normalize_sql(expected) != _normalize_sql(actual):
            issues.append(f"Migration {path.name} diverges from model {model.__name__}.")

    return issues


def align_migrations_to_models(project_root: Path, driver: str) -> list[Path]:
    """Overwrite migration files with DDL generated from models (model is the reference).
    Only updates existing migration files that match a model. Returns list of updated file paths.
    """
    root = Path(project_root).resolve()
    migrations_dir = root / "dags" / "sql" / "migrations"
    if not migrations_dir.exists():
        return []

    dialect = get_dialect(driver)
    models = discover_models(root)
    updated: list[Path] = []
    for model in models:
        schema = model.get_schema().lower()
        table = model.get_table_name().lower()
        path = _find_migration_for_model(migrations_dir, schema, table)
        if not path:
            continue
        ddl = dialect.emit_create_table(model)
        path.write_text(ddl.strip() + "\n", encoding="utf-8")
        updated.append(path)
    return updated
