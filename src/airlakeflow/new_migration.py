import re
from pathlib import Path

from airlakeflow.style import secho_info, secho_ok


def _next_migration_number(project_root: Path) -> int:
    migrations_dir = project_root / "dags" / "sql" / "migrations"
    if not migrations_dir.exists():
        return 1
    existing = list(migrations_dir.glob("V*.sql"))
    numbers = []
    for f in existing:
        m = re.match(r"V(\d+)__", f.name)
        if m:
            numbers.append(int(m.group(1)))
    return max(numbers, default=0) + 1


def discover_dags(project_root: Path) -> list[str]:
    """List DAG names (folders in dags/ that contain pipeline.py)."""
    dags_dir = project_root / "dags"
    if not dags_dir.exists():
        return []
    result = []
    for path in sorted(dags_dir.iterdir()):
        if path.is_dir() and not path.name.startswith("_"):
            if (path / "pipeline.py").exists():
                result.append(path.name)
    return result


def _snake(s: str) -> str:
    return s.replace("-", "_").lower()


_TPL_BRONZE = """-- Migration: {{ name }}
-- DAG: {{ dag }}
-- Layer: bronze
-- Adjust the table and fields as needed.

CREATE TABLE IF NOT EXISTS bronze.{{ table }}_raw (
    id SERIAL PRIMARY KEY,
    ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    payload JSONB
);

CREATE INDEX IF NOT EXISTS idx_bronze_{{ table }}_data ON bronze.{{ table }}_raw(ingestion_date);
"""

_TPL_SILVER = """-- Migration: {{ name }}
-- DAG: {{ dag }}
-- Layer: silver
-- Adjust the table and fields as needed.

CREATE TABLE IF NOT EXISTS silver.{{ table }} (
    id SERIAL PRIMARY KEY,
    field1 INT,
    field2 VARCHAR(255),
    updated_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_silver_{{ table }}_updated ON silver.{{ table }}(updated_at);
"""

_TPL_GOLD = """-- Migration: {{ name }}
-- DAG: {{ dag }}
-- Layer: gold
-- Adjust the table and fields as needed.

CREATE TABLE IF NOT EXISTS gold.{{ table }}_daily (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    entity_id VARCHAR(50) NOT NULL,
    campo1 INT,
    campo2 VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (date, entity_id)
);

CREATE INDEX IF NOT EXISTS idx_gold_{{ table }}_daily_date ON gold.{{ table }}_daily(date);
"""

_TEMPLATES = {
    "bronze": _TPL_BRONZE,
    "silver": _TPL_SILVER,
    "gold": _TPL_GOLD,
}


def run_new_migration(
    name: str,
    dag: str,
    layer: str,
    project_root: str,
) -> None:
    project_root = Path(project_root).resolve()
    migrations_dir = project_root / "dags" / "sql" / "migrations"
    migrations_dir.mkdir(parents=True, exist_ok=True)

    next_ver = _next_migration_number(project_root)
    table_snake = _snake(dag)
    name_snake = _snake(name).replace(" ", "_")
    desc = name_snake or f"setup_{layer}_{table_snake}"
    filename = f"V{next_ver:03d}__{desc}.sql"

    tpl = _TEMPLATES.get(layer, _TPL_SILVER)
    content = (
        tpl.replace("{{ name }}", name)
        .replace("{{ dag }}", dag)
        .replace("{{ table }}", table_snake)
    )

    filepath = migrations_dir / filename
    filepath.write_text(content.strip(), encoding="utf-8")

    secho_ok(f"Migration created: dags/sql/migrations/{filename}")
    secho_info(f"  DAG: {dag} | Layer: {layer}")
    secho_info("  Edit the file to adjust columns and indices.")
