"""ALF-Checks command: scaffold config/checks/ (generic.yaml + layer folders) and generate DAG 01_alf_checks."""

from pathlib import Path

from jinja2 import Environment, PackageLoader, select_autoescape

from airlakeflow.style import secho_info, secho_ok


def create_alf_check_file(project_root: Path, schema: str, table: str) -> Path:
    """Ensure config/checks/ structure and DAG exist, then create config/checks/{schema}/{table}.yaml with default checks.
    Returns the path to the created file.
    """
    run_data_tests_cmd(project_root)
    root = Path(project_root).resolve()
    layer_dir = root / "config" / "checks" / schema
    layer_dir.mkdir(parents=True, exist_ok=True)
    out_path = layer_dir / f"{table}.yaml"
    if out_path.exists():
        return out_path
    default_content = (
        f"# ALF-Checks: {schema}.{table}\n"
        "checks:\n"
        "  - type: not_null\n"
        "    columns: []  # ex: [id, data_ingestao]\n"
        "  - type: row_count\n"
        "    min: 0\n"
    )
    out_path.write_text(default_content, encoding="utf-8")
    return out_path


def run_data_tests_cmd(project_root: Path) -> None:
    """Create config/checks/generic.yaml, layer folders (bronze, silver, gold), and dags/01_alf_checks.py."""
    root = Path(project_root).resolve()
    checks_dir = root / "config" / "checks"
    checks_dir.mkdir(parents=True, exist_ok=True)

    generic_path = checks_dir / "generic.yaml"
    if not generic_path.exists():
        generic_path.write_text(
            "# ALF-Checks: connection and optional global checks\n"
            "connection_id: postgres_datawarehouse\n"
            "# global_checks: []  # future: schema existence, etc.\n",
            encoding="utf-8",
        )
        secho_info(f"Created {generic_path}")

    for layer in ("bronze", "silver", "gold"):
        layer_dir = checks_dir / layer
        layer_dir.mkdir(parents=True, exist_ok=True)
        gitkeep = layer_dir / ".gitkeep"
        if not gitkeep.exists():
            gitkeep.write_text("")

    env = Environment(
        loader=PackageLoader("airlakeflow", "templates"),
        autoescape=select_autoescape(),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    dags_dir = root / "dags"
    dags_dir.mkdir(parents=True, exist_ok=True)
    t = env.get_template("data_tests_dag.py.j2")
    out_file = dags_dir / "01_alf_checks.py"
    out_file.write_text(t.render(), encoding="utf-8")
    secho_ok(f"DAG written to {out_file}")
    secho_info(
        "  Add table checks under config/checks/bronze/, silver/, gold/ (one YAML per table)."
    )
