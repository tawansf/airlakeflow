"""Generate migration SQL files from models using a dialect (e.g. Postgres)."""

from __future__ import annotations

from pathlib import Path

from airlakeflow.dialects import get_dialect
from airlakeflow.model_loader import discover_models
from airlakeflow.models.base import Model


def _next_migration_number(migrations_dir: Path) -> int:
    if not migrations_dir.exists():
        return 1
    import re

    existing = list(migrations_dir.glob("V*.sql"))
    numbers = []
    for f in existing:
        m = re.match(r"V(\d+)__", f.name)
        if m:
            numbers.append(int(m.group(1)))
    return max(numbers, default=0) + 1


def _dependency_order(models: list[type[Model]], layer_order: dict[str, int]) -> list[type[Model]]:
    """Order models so that referenced tables come first; tie-break by architecture layer order."""
    schema_table = {}
    for m in models:
        key = (m.get_schema(), m.get_table_name())
        schema_table[key] = m

    def deps(model: type[Model]) -> list[tuple[str, str]]:
        refs = []
        for _name, field in model.get_fields():
            if field.ref:
                refs.append((field.ref.schema, field.ref.table))
        return refs

    order: list[type[Model]] = []
    seen = set()

    def visit(m: type[Model]) -> None:
        key = (m.get_schema(), m.get_table_name())
        if key in seen:
            return
        seen.add(key)
        for s, t in deps(m):
            dep = schema_table.get((s, t))
            if dep and (s, t) not in seen:
                visit(dep)
        order.append(m)

    for m in sorted(
        models, key=lambda x: (layer_order.get(x.get_schema(), 99), x.get_table_name())
    ):
        visit(m)

    return order


def generate_migrations(
    project_root: Path,
    driver: str = "postgres",
    migrations_dir: Path | None = None,
    emit_schema_setup: bool = True,
) -> list[Path]:
    """Discover models, order by dependencies, generate one SQL file per model (and optional schema setup).
    Returns list of created file paths.
    """
    root = Path(project_root).resolve()
    migrations_dir = migrations_dir or root / "dags" / "sql" / "migrations"
    migrations_dir.mkdir(parents=True, exist_ok=True)
    dialect = get_dialect(driver)
    models = discover_models(root)
    if not models:
        return []

    from airlakeflow.config import get_architecture_from_config, load_config

    cfg = load_config(root)
    arch = get_architecture_from_config(cfg)
    layer_order = arch.layer_order()
    ordered = _dependency_order(models, layer_order)
    created: list[Path] = []
    next_num = _next_migration_number(migrations_dir)

    # Schema creation belongs in scripts/ (002_create_schemas.sql), not in migrations (tables/views only)

    def _migration_exists(schema: str, table: str) -> bool:
        needle = f"__setup_{schema}_{table}.sql"
        return any(p.name.endswith(needle) for p in migrations_dir.glob("V*.sql"))

    for model in ordered:
        schema = model.get_schema()
        table = model.get_table_name()
        if _migration_exists(schema, table):
            continue
        ddl = dialect.emit_create_table(model)
        desc = f"setup_{schema}_{table}"
        path = migrations_dir / f"V{next_num:03d}__{desc}.sql"
        path.write_text(ddl.strip() + "\n", encoding="utf-8")
        created.append(path)
        next_num += 1

    return created
