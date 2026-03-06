import shutil
from pathlib import Path

from airlakeflow.docker_cmd import create_env_from_example
from airlakeflow.style import secho_heading, secho_info, secho_ok


def run_init(dest: str, with_demo: bool, with_monitoring: bool, backend: str = "pandas") -> None:
    """Create a new project: folder with framework structure (from repo or package skeleton).
    backend: 'pandas' (lighter, default) or 'pyspark' (distributed processing).
    """
    cwd = Path.cwd().resolve()
    # If dest is a simple name (no path sep), create folder inside cwd
    if dest != "." and "/" not in dest and "\\" not in dest:
        dest_path = (cwd / dest).resolve()
    else:
        dest_path = Path(dest).resolve()

    if dest_path == cwd:
        # With src layout: __file__ is src/airlakeflow/init_cmd.py -> parent.parent.parent = repo root
        framework_root = Path(__file__).resolve().parent.parent.parent
    else:
        framework_root = cwd
        # If cwd doesn't look like the framework repo, use package skeleton
        if not (framework_root / "dags").is_dir() or not (framework_root / "soda").is_dir():
            framework_root = Path(__file__).resolve().parent / "skeleton"

    dirs_to_copy = [
        "dags",
        "soda",
        "scripts",
        "config",
        "plugins",
        "data",
        "logs",
    ]
    files_to_copy = [
        "docker-compose.yaml",
        "Dockerfile",
        ".env.example",
        "requirements.txt",
        "README.md",
    ]

    dest_path.mkdir(parents=True, exist_ok=True)
    for d in dirs_to_copy:
        src = framework_root / d
        if src.exists():
            dst = dest_path / d
            if dst.exists():
                shutil.rmtree(dst)
            shutil.copytree(
                src,
                dst,
                ignore=shutil.ignore_patterns("__pycache__", "*.pyc", ".git", "logs", "*.log"),
            )
    for f in files_to_copy:
        src = framework_root / f
        if src.exists():
            shutil.copy2(src, dest_path / f)

    # Silver backend: write config and adjust requirements
    backend = backend.lower().strip()
    if backend not in ("pandas", "pyspark"):
        backend = "pandas"
    config_path = dest_path / ".airlakeflow.yaml"
    config_path.write_text(
        f"# AirLakeFlow project config\nsilver_backend: {backend}\n", encoding="utf-8"
    )
    req_path = dest_path / "requirements.txt"
    if req_path.exists():
        text = req_path.read_text(encoding="utf-8")
        if backend == "pyspark" and "pyspark" not in text:
            text = text.rstrip() + "\n\n# Distributed processing (chosen at init)\npyspark>=3.5.0\n"
            req_path.write_text(text, encoding="utf-8")
        # If pandas, leave requirements as-is (skeleton has no pyspark by default)

    # Create .env with AIRFLOW_UID = current user (Unix) or 50000 (Windows) so alf run works without permission errors
    if create_env_from_example(dest_path):
        secho_info("  - .env created with AIRFLOW_UID for this machine")

    if not with_demo:
        for name in ("crypto",):
            demo_dir = dest_path / "dags" / name
            if demo_dir.exists():
                shutil.rmtree(demo_dir)
        for c in ("bitcoin_bronze", "bitcoin_silver"):
            contract = dest_path / "soda" / "contracts" / f"{c}.yaml"
            if contract.exists():
                contract.unlink()

    if not with_monitoring:
        monitoring_dir = dest_path / "dags" / "monitoring"
        if monitoring_dir.exists():
            shutil.rmtree(monitoring_dir)
        for m in list((dest_path / "dags" / "sql" / "migrations").glob("V005*.sql")) + list(
            (dest_path / "dags" / "sql" / "migrations").glob("V006*.sql")
        ):
            m.unlink()

    secho_ok(f"Project created: {dest_path}")
    if framework_root.name == "skeleton":
        secho_info("  (structure from AirLakeFlow package)")
    if with_demo and (dest_path / "dags" / "crypto").exists():
        secho_info("  - DAG demo (crypto) included")
    if with_monitoring and (dest_path / "dags" / "monitoring").exists():
        secho_info("  - Monitoring and Soda report included")
    secho_info(f"  - Silver layer backend: {backend} (edit .airlakeflow.yaml to change)")
