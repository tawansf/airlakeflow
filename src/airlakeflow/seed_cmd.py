"""Seed command: ensure data/seeds/ exists and generate DAG that loads CSVs into bronze."""

from pathlib import Path

from jinja2 import Environment, PackageLoader, select_autoescape

from airlakeflow.style import secho_ok, secho_info


def run_seed(project_root: Path) -> None:
    """Ensure data/seeds/ exists and generate dags/00_seeds.py (loads data/seeds/*.csv into bronze)."""
    root = Path(project_root).resolve()
    seeds_dir = root / "data" / "seeds"
    seeds_dir.mkdir(parents=True, exist_ok=True)
    (seeds_dir / ".gitkeep").touch()

    dags_dir = root / "dags"
    dags_dir.mkdir(parents=True, exist_ok=True)
    out_path = dags_dir / "00_seeds.py"

    env = Environment(
        loader=PackageLoader("airlakeflow", "templates"),
        autoescape=select_autoescape(),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    t = env.get_template("seed_loader_dag.py.j2")
    out_path.write_text(t.render(), encoding="utf-8")

    secho_ok("Seeds: data/seeds/ ready and dags/00_seeds.py generated.")
    secho_info("  Put CSV files in data/seeds/ and run the DAG 00_seeds in Airflow.")
