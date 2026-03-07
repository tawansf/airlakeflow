"""
Soda integration: ensure config/monitoring and add tasks + contracts by ETL.
Not Licensed: This code is not licensed.
"""

from __future__ import annotations

import re
from pathlib import Path

import click
from jinja2 import Environment, PackageLoader, select_autoescape

from airlakeflow.config import (
    get_contracts_dir,
    get_soda_config_path,
    get_soda_data_source,
    load_config,
)
from airlakeflow.new_migration import discover_dags


# Minimum stub for configuration.yaml (don't overwrite if it already exists)
def _soda_config_yaml(data_source_name: str) -> str:
    return f"""# Soda data source config (name, type, connection)
name: {data_source_name}
type: postgres
connection:
  host: postgres
  port: 5432
  user: airflow
  password: airflow
  database: datawarehouse
"""


def _ensure_project_soda(project_root: Path) -> None:
    """Ensure soda/configuration.yaml, soda/contracts/ and dags/monitoring/soda_persistence.py."""
    cfg = load_config(project_root)
    soda_config_path = project_root / get_soda_config_path(cfg)
    soda_config_path.parent.mkdir(parents=True, exist_ok=True)
    contracts_dir = project_root / get_contracts_dir(cfg)
    contracts_dir.mkdir(parents=True, exist_ok=True)

    if not soda_config_path.exists():
        data_source = get_soda_data_source(cfg)
        soda_config_path.write_text(_soda_config_yaml(data_source), encoding="utf-8")

    monitoring_dir = project_root / "dags" / "monitoring"
    persistence_path = monitoring_dir / "soda_persistence.py"
    if not persistence_path.exists():
        monitoring_dir.mkdir(parents=True, exist_ok=True)
        # Minimum stub to avoid import break; user can copy from the framework
        stub = '''"""Soda scan + persist. Copie o conteúdo completo do framework se necessário."""
def run_soda_scan_and_persist(data_source, config_path, contract_path, dag_id, task_id):
    raise NotImplementedError("Copie dags/monitoring/soda_persistence.py do repositório do framework.")
'''
        persistence_path.write_text(stub, encoding="utf-8")


def _render_contract(env: Environment, template_name: str, entity_snake: str) -> str:
    table_name = entity_snake.replace("_", " ").title()
    return env.get_template(template_name).render(entity_snake=entity_snake, table_name=table_name)


def _ensure_etl_contracts(project_root: Path, etl_name: str, env: Environment) -> tuple[bool, bool]:
    """Create stub contracts if they don't exist. Returns (created_bronze, created_silver)."""
    cfg = load_config(project_root)
    contracts_dir = project_root / get_contracts_dir(cfg)
    contracts_dir.mkdir(parents=True, exist_ok=True)
    bronze_path = contracts_dir / f"{etl_name}_bronze.yaml"
    silver_path = contracts_dir / f"{etl_name}_silver.yaml"
    created_bronze = False
    created_silver = False
    if not bronze_path.exists():
        bronze_path.write_text(
            _render_contract(env, "contract_bronze.yaml.j2", etl_name), encoding="utf-8"
        )
        created_bronze = True
    if not silver_path.exists():
        silver_path.write_text(
            _render_contract(env, "contract_silver.yaml.j2", etl_name), encoding="utf-8"
        )
        created_silver = True
    return created_bronze, created_silver


def _inject_soda_into_pipeline(project_root: Path, etl_name: str) -> bool:
    """
    Add import, SODA_PATH, callables _soda_scan_bronze/_soda_scan_silver,
    tasks quality_bronze/quality_silver and dependencies in the pipeline.
    Returns True if the file was modified.
    """
    pipeline_path = project_root / "dags" / etl_name / "pipeline.py"
    if not pipeline_path.exists():
        return False
    content = pipeline_path.read_text(encoding="utf-8")
    if (
        "run_soda_scan_and_persist" in content
        or "soda_scan_bronze" in content
        or "quality_bronze" in content
    ):
        return False

    cfg = load_config(project_root)
    data_source = get_soda_data_source(cfg)
    # Injected paths assume SODA_PATH is the soda dir (e.g. /opt/airflow/soda) with configuration.yaml and contracts/
    # 1) Import: after the last "from X import" in the same module (e.g.: from csgostats.gold import ...)
    last_from = None
    for m in re.finditer(r"^from [\w.]+ import .+$", content, re.MULTILINE):
        last_from = m
    if last_from:
        insert_import = "\nfrom monitoring.soda_persistence import run_soda_scan_and_persist"
        content = content[: last_from.end()] + insert_import + content[last_from.end() :]

    # 2) SODA_PATH and callables: after load_dotenv() or after SODA_PATH if it already exists
    callables_block = f"""

def _soda_scan_bronze(**context):
    run_soda_scan_and_persist(
        data_source="{data_source}",
        config_path=f"{{SODA_PATH}}/configuration.yaml",
        contract_path=f"{{SODA_PATH}}/contracts/{etl_name}_bronze.yaml",
        dag_id=context["dag"].dag_id,
        task_id=context["task"].task_id,
    )


def _soda_scan_silver(**context):
    run_soda_scan_and_persist(
        data_source="{data_source}",
        config_path=f"{{SODA_PATH}}/configuration.yaml",
        contract_path=f"{{SODA_PATH}}/contracts/{etl_name}_silver.yaml",
        dag_id=context["dag"].dag_id,
        task_id=context["task"].task_id,
    )

"""
    if "SODA_PATH" not in content:
        load_dotenv_pos = content.find("load_dotenv()")
        if load_dotenv_pos != -1:
            after = content.index("\n", load_dotenv_pos) + 1
            content = (
                content[:after]
                + '\n\nSODA_PATH = os.getenv("SODA_PATH", "/opt/airflow/soda")'
                + callables_block
                + content[after:]
            )
    else:
        # SODA_PATH already exists; insert callables after the SODA_PATH line
        match = re.search(r"SODA_PATH = os\.getenv\([^)]+\)\s*\n", content)
        if match:
            after = match.end()
            if "_soda_scan_bronze" not in content:
                content = content[:after] + callables_block + content[after:]

    # 3) Tasks quality_bronze and quality_silver: insert after the "transformation = PythonOperator(...)" block
    task_block = f"""
    quality_bronze = PythonOperator(
        task_id="soda_scan_bronze_{etl_name}",
        python_callable=_soda_scan_bronze,
    )

    quality_silver = PythonOperator(
        task_id="soda_scan_silver_{etl_name}",
        python_callable=_soda_scan_silver,
    )

"""
    # Insert before "gold_aggregate = " (keep newline + indentation) or before the dependency line
    if "gold_aggregate = PythonOperator" in content:
        match = re.search(r"\n    gold_aggregate = PythonOperator", content)
        if match:
            pos = match.start()
            content = content[:pos] + task_block + content[pos:]
    else:
        # No gold: insert before the line with ">>"
        match = re.search(r"\n(    ingestion >> .+)\n", content)
        if match:
            pos = match.start()
            content = content[:pos] + task_block + content[pos:]
        else:
            return False

    # 4) Replace dependency: ingestion >> transformation [>> gold_aggregate] -> ingestion >> quality_bronze >> transformation >> quality_silver [>> gold_aggregate]
    has_gold = "gold_aggregate" in content
    old_chain = re.compile(r"(\s+)ingestion >> transformation >> gold_aggregate\s*$", re.MULTILINE)
    new_chain = (
        r"\1ingestion >> quality_bronze >> transformation >> quality_silver >> gold_aggregate"
    )
    if has_gold and old_chain.search(content):
        content = old_chain.sub(new_chain, content, count=1)
    else:
        old_no_gold = re.compile(r"(\s+)ingestion >> transformation\s*$", re.MULTILINE)
        new_no_gold = r"\1ingestion >> quality_bronze >> transformation >> quality_silver"
        if old_no_gold.search(content):
            content = old_no_gold.sub(new_no_gold, content, count=1)
        else:
            return False

    pipeline_path.write_text(content, encoding="utf-8")
    return True


def run_add_soda(
    project_root: str | Path,
    etl_name: str | None = None,
    all_etls: bool = False,
) -> None:
    """
    Execute the Soda integration: ensure config/monitoring in the project and, by ETL,
    create stub contracts (if they don't exist) and inject Soda tasks into the pipeline (if they don't already have them).
    etl_name: None = interactive, "ALL" or all_etls = all ETLs, otherwise the ETL name.
    """
    root = Path(project_root).resolve()
    dags = discover_dags(root)
    if not dags:
        raise SystemExit(1)  # CLI message

    if etl_name is None and not all_etls:
        _run_add_soda_interactive(root, dags)
        return

    targets = dags if (all_etls or (etl_name and etl_name.upper() == "ALL")) else [etl_name]
    for name in targets:
        if name not in dags:
            continue
        _ensure_project_soda(root)
        env = Environment(
            loader=PackageLoader("airlakeflow", "templates"),
            autoescape=select_autoescape(),
        )
        cb, cs = _ensure_etl_contracts(root, name, env)
        if cb or cs:
            pass  # optional: echo "Contracts created: ..."
        injected = _inject_soda_into_pipeline(root, name)
        if injected:
            pass  # optional: echo "Soda added to the pipeline ..."
    # Summary messages can be done in the CLI
    return


def _run_add_soda_interactive(project_root: Path, dags: list[str]) -> None:
    options = dags + ["All"]
    choice = click.prompt(
        "Choose the ETL or All",
        type=click.Choice(options),
        show_choices=True,
    )
    if choice == "All":
        run_add_soda(project_root, all_etls=True)
    else:
        run_add_soda(project_root, etl_name=choice)
