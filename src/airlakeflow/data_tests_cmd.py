"""Data tests command: scaffold config/data_tests.yaml and generate DAG 01_data_tests."""

from pathlib import Path

from jinja2 import Environment, PackageLoader, select_autoescape

from airlakeflow.data_tests import load_data_tests_config
from airlakeflow.style import secho_ok, secho_info


def run_data_tests_cmd(project_root: Path) -> None:
    """Ensure config/data_tests.yaml exists (with example if new) and generate dags/01_data_tests.py."""
    root = Path(project_root).resolve()
    config_dir = root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_file = config_dir / "data_tests.yaml"

    if not config_file.exists():
        example = """# Data tests (run by DAG 01_data_tests)
# See docs: not_null, row_count, unique
connection_id: postgres_datawarehouse
tables:
  # - schema: bronze
  #   table: example_raw
  #   checks:
  #     - type: not_null
  #       columns: [data_ingestao]
  #     - type: row_count
  #       min: 0
"""
        config_file.write_text(example, encoding="utf-8")
        secho_info(f"Created {config_file} with example. Edit and add your tables.")

    env = Environment(
        loader=PackageLoader("airlakeflow", "templates"),
        autoescape=select_autoescape(),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    dags_dir = root / "dags"
    dags_dir.mkdir(parents=True, exist_ok=True)
    t = env.get_template("data_tests_dag.py.j2")
    out_file = dags_dir / "01_data_tests.py"
    out_file.write_text(t.render(), encoding="utf-8")
    secho_ok(f"DAG written to {out_file}")
    secho_info("  Define tables and checks in config/data_tests.yaml.")
