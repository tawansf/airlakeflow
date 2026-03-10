from pathlib import Path

from jinja2 import Environment, FileSystemLoader, PackageLoader, select_autoescape

from airlakeflow.style import secho_info, secho_ok


def _snake(s: str) -> str:
    return s.replace("-", "_").lower()


def run_new_etl(
    name: str,
    table_name: str,
    with_contracts: bool,
    with_gold: bool,
    source: str,
    use_spark: bool,
    project_root: str,
    pattern: str = "default",
    partition_by: str | None = None,
    incremental_by: str | None = None,
) -> None:
    project_root = Path(project_root).resolve()
    dags_dir = project_root / "dags"
    soda_contracts_dir = project_root / "soda" / "contracts"
    migrations_dir = project_root / "dags" / "sql" / "migrations"

    for d in (dags_dir, soda_contracts_dir, migrations_dir):
        d.mkdir(parents=True, exist_ok=True)

    entity_snake = _snake(table_name)
    name_snake = _snake(name)

    ctx = {
        "name": name,
        "name_snake": name_snake,
        "table_name": table_name,
        "entity_snake": entity_snake,
        "with_contracts": with_contracts,
        "with_gold": with_gold,
        "source": source,
        "use_spark": use_spark,
        "pattern": pattern,
        "partition_by": partition_by,
        "incremental_by": incremental_by,
    }

    # Prefer project-specific templates if present (templates/ in project root)
    project_templates = project_root / "templates"
    if project_templates.is_dir():
        env = Environment(
            loader=FileSystemLoader(str(project_templates)),
            autoescape=select_autoescape(),
            trim_blocks=True,
            lstrip_blocks=True,
        )
    else:
        env = Environment(
            loader=PackageLoader("airlakeflow", "templates"),
            autoescape=select_autoescape(),
            trim_blocks=True,
            lstrip_blocks=True,
        )

    domain_dir = dags_dir / name_snake
    domain_dir.mkdir(parents=True, exist_ok=True)
    (domain_dir / "transformations").mkdir(parents=True, exist_ok=True)

    def render_to(path: Path, template_name: str, **extra):
        t = env.get_template(template_name)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(t.render(**ctx, **extra), encoding="utf-8")

    render_to(domain_dir / "pipeline.py", "pipeline.py.j2")
    render_to(domain_dir / "bronze.py", "bronze.py.j2")
    render_to(domain_dir / "silver.py", "silver.py.j2")
    if with_gold:
        render_to(domain_dir / "gold.py", "gold.py.j2")
    transformation_tpl = "transformation_snapshot.py.j2" if pattern == "snapshot" else "transformation.py.j2"
    render_to(domain_dir / "transformations" / f"{entity_snake}.py", transformation_tpl)

    if with_contracts:
        render_to(
            soda_contracts_dir / f"{entity_snake}_bronze.yaml",
            "contract_bronze.yaml.j2",
        )
        render_to(
            soda_contracts_dir / f"{entity_snake}_silver.yaml",
            "contract_silver.yaml.j2",
        )

    secho_ok(f"ETL '{name}' created in dags/{name_snake}/")
    secho_info("  - pipeline.py, bronze.py, silver.py" + (", gold.py" if with_gold else ""))
    secho_info(f"  - transformations/{entity_snake}.py")
    if with_contracts:
        secho_info(f"  - soda/contracts/{entity_snake}_bronze.yaml, {entity_snake}_silver.yaml")
    secho_info(
        "  Create the migrations later with: alf new migration <name> --dag "
        + name_snake
        + " --layer bronze|silver|gold"
    )
