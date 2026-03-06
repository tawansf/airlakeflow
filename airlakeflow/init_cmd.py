import shutil
from pathlib import Path

from airlakeflow.docker_cmd import create_env_from_example


def run_init(dest: str, with_demo: bool, with_monitoring: bool) -> None:
    """Create a new project: folder with framework structure (from repo or package skeleton)."""
    cwd = Path.cwd().resolve()
    # If dest is a simple name (no path sep), create folder inside cwd
    if dest != "." and "/" not in dest and "\\" not in dest:
        dest_path = (cwd / dest).resolve()
    else:
        dest_path = Path(dest).resolve()

    if dest_path == cwd:
        framework_root = Path(__file__).resolve().parent.parent
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
            shutil.copytree(src, dst, ignore=shutil.ignore_patterns("__pycache__", "*.pyc", ".git", "logs", "*.log"))
    for f in files_to_copy:
        src = framework_root / f
        if src.exists():
            shutil.copy2(src, dest_path / f)

    # Create .env with AIRFLOW_UID = current user (Unix) or 50000 (Windows) so alf run works without permission errors
    if create_env_from_example(dest_path):
        print("  - .env created with AIRFLOW_UID for this machine")

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
        for m in list((dest_path / "dags" / "sql" / "migrations").glob("V005*.sql")) + list((dest_path / "dags" / "sql" / "migrations").glob("V006*.sql")):
            m.unlink()

    print(f"Project created: {dest_path}")
    if framework_root.name == "skeleton":
        print("  (structure from AirLakeFlow package)")
    if with_demo and (dest_path / "dags" / "crypto").exists():
        print("  - DAG demo (crypto) included")
    if with_monitoring and (dest_path / "dags" / "monitoring").exists():
        print("  - Monitoring and Soda report included")
