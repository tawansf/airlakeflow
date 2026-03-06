"""Create a minimal new layer (DAG folder with pipeline and optional stubs)."""

from __future__ import annotations

from pathlib import Path

from jinja2 import Environment, FileSystemLoader, PackageLoader, select_autoescape

from airlakeflow.config import load_config
from airlakeflow.style import secho_info, secho_ok, secho_warn


def _snake(s: str) -> str:
    return s.replace("-", "_").lower()


def run_new_layer(
    name: str,
    project_root: Path,
    with_bronze: bool = True,
    with_silver: bool = True,
    with_gold: bool = True,
) -> None:
    """Create dags/<name>/ with pipeline.py and optional bronze.py, silver.py, gold.py stubs.
    use_spark is read from .airlakeflow.yaml silver_backend (pyspark -> True, pandas -> False).
    """
    project_root = project_root.resolve()
    cfg = load_config(project_root)
    use_spark = cfg.get("silver_backend", "pandas") == "pyspark"
    dags_dir = project_root / "dags"
    dags_dir.mkdir(parents=True, exist_ok=True)

    name_snake = _snake(name)
    domain_dir = dags_dir / name_snake
    if domain_dir.exists() and (domain_dir / "pipeline.py").exists():
        secho_warn(f"Layer '{name}' already exists in dags/{name_snake}/. Not overwriting.")
        return

    domain_dir.mkdir(parents=True, exist_ok=True)
    (domain_dir / "transformations").mkdir(parents=True, exist_ok=True)

    # Prefer project templates if present
    project_templates = project_root / "templates"
    if project_templates.is_dir():
        env = Environment(
            loader=FileSystemLoader(str(project_templates)), autoescape=select_autoescape()
        )
    else:
        env = Environment(
            loader=PackageLoader("airlakeflow", "templates"),
            autoescape=select_autoescape(),
            trim_blocks=True,
            lstrip_blocks=True,
        )

    ctx = {
        "name": name,
        "name_snake": name_snake,
        "table_name": name,
        "entity_snake": name_snake,
        "with_contracts": False,
        "with_gold": with_gold,
        "source": "api",
        "use_spark": use_spark,
    }

    def render_to(path: Path, template_name: str):
        t = env.get_template(template_name)
        path.write_text(t.render(**ctx), encoding="utf-8")

    render_to(domain_dir / "pipeline.py", "pipeline.py.j2")
    render_to(domain_dir / "bronze.py", "bronze.py.j2")
    render_to(domain_dir / "silver.py", "silver.py.j2")
    if with_gold:
        render_to(domain_dir / "gold.py", "gold.py.j2")
    render_to(domain_dir / "transformations" / f"{name_snake}.py", "transformation.py.j2")

    secho_ok(f"Layer '{name}' created in dags/{name_snake}/")
    secho_info("  - pipeline.py, bronze.py, silver.py" + (", gold.py" if with_gold else ""))
    secho_info(f"  - transformations/{name_snake}.py")
    secho_info(
        f"  Create migrations with: alf new migration <name> --dag {name_snake} --layer bronze|silver|gold"
    )
