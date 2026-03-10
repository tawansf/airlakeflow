"""Docs command: generate static catalog of tables/layers from models and migrations."""

from pathlib import Path

from jinja2 import Environment, PackageLoader, select_autoescape

from airlakeflow.model_loader import discover_models
from airlakeflow.style import secho_ok, secho_info


def _migrations_entries(migrations_dir: Path) -> list[dict]:
    """List migration files and infer schema.table from filename (e.g. V001__setup_silver_example.sql)."""
    entries = []
    if not migrations_dir.exists():
        return entries
    import re
    for path in sorted(migrations_dir.glob("V*.sql")):
        name = path.stem  # V001__setup_silver_example
        m = re.match(r"V\d+__.*_(?P<schema>bronze|silver|gold)_(?P<table>\w+)", name, re.I)
        if m:
            entries.append(
                {
                    "schema": m.group("schema").lower(),
                    "table": m.group("table").lower(),
                    "source": "migration",
                    "file": path.name,
                }
            )
        else:
            entries.append({"schema": "-", "table": name, "source": "migration", "file": path.name})
    return entries


def run_docs(project_root: Path, output_dir: str | None = None, fmt: str = "html") -> None:
    """Generate catalog from config/models and dags/sql/migrations; write to docs/ (or output_dir)."""
    root = Path(project_root).resolve()
    out = root / (output_dir or "docs")
    out.mkdir(parents=True, exist_ok=True)

    models = discover_models(root)
    catalog_from_models = [
        {
            "schema": m.get_schema(),
            "table": m.get_table_name(),
            "layer": m.get_schema(),
            "source": "model",
            "file": getattr(m, "__module__", "").replace("config.models.", ""),
        }
        for m in models
    ]

    migrations_dir = root / "dags" / "sql" / "migrations"
    catalog_from_migrations = _migrations_entries(migrations_dir)

    # Merge: model entries first, then migration-only (by schema.table)
    seen = {(e["schema"], e["table"]) for e in catalog_from_models}
    for e in catalog_from_migrations:
        if (e["schema"], e["table"]) not in seen:
            e["layer"] = e["schema"]
            catalog_from_models.append(e)
            seen.add((e["schema"], e["table"]))

    catalog = sorted(catalog_from_models, key=lambda x: (x["schema"], x["table"]))

    env = Environment(
        loader=PackageLoader("airlakeflow", "templates"),
        autoescape=select_autoescape(),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    if fmt == "html":
        t = env.get_template("catalog.html.j2")
        out_file = out / "catalog.html"
        out_file.write_text(t.render(catalog=catalog, project_root=str(root)), encoding="utf-8")
        secho_ok(f"Catalog written to {out_file}")
    else:
        import json
        out_file = out / "catalog.json"
        out_file.write_text(json.dumps(catalog, indent=2), encoding="utf-8")
        secho_ok(f"Catalog written to {out_file}")
    secho_info("  Sources: config/models and dags/sql/migrations.")
