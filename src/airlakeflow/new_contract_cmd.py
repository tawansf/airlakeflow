"""Generate a single Soda contract for an existing schema/table."""

from __future__ import annotations

from pathlib import Path

from jinja2 import Environment, PackageLoader, select_autoescape

from airlakeflow.config import get_contracts_dir, load_config
from airlakeflow.style import secho_ok, secho_warn


def _snake(s: str) -> str:
    return s.replace("-", "_").lower()


def run_new_contract(schema: str, table: str, layer: str, project_root: Path) -> None:
    """Create a Soda contract file for the given schema.table in the given layer (bronze or silver)."""
    project_root = project_root.resolve()
    cfg = load_config(project_root)
    contracts_dir = project_root / get_contracts_dir(cfg)
    contracts_dir.mkdir(parents=True, exist_ok=True)

    entity_snake = _snake(table)
    table_name = f"{schema}.{table}"

    env = Environment(
        loader=PackageLoader("airlakeflow", "templates"),
        autoescape=select_autoescape(),
    )
    if layer == "bronze":
        template_name = "contract_bronze.yaml.j2"
        out_name = f"{entity_snake}_bronze.yaml"
    else:
        template_name = "contract_silver.yaml.j2"
        out_name = f"{entity_snake}_silver.yaml"

    out_path = contracts_dir / out_name
    if out_path.exists():
        secho_warn(f"Contract already exists: {out_path}. Not overwriting.")
        return
    content = env.get_template(template_name).render(
        entity_snake=entity_snake,
        table_name=table_name,
    )
    out_path.write_text(content, encoding="utf-8")
    secho_ok(f"Contract created: soda/contracts/{out_name}")
