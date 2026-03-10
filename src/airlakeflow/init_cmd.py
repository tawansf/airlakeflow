import shutil
import subprocess
import sys
from pathlib import Path

from airlakeflow.docker_cmd import create_env_from_example
from airlakeflow.style import SYM_OK, secho_info, secho_ok


def _default_model_content(layer: str) -> str:
    return f'''"""Example model ({layer}). Edit or create new ones with 'alf new model NAME'."""

from airlakeflow.models import Model, Field, layer


@layer("{layer}")
class ExampleModel(Model):
    __table__ = "example"

    id = Field.serial(primary_key=True)
    name = Field.varchar(255, nullable=False)
    created_at = Field.timestamp(default="CURRENT_TIMESTAMP")
    updated_at = Field.timestamp(nullable=False)
'''


def _write_default_model(models_dir: Path, default_layer: str = "silver") -> None:
    """Write config/models/example.py so init leaves model + migrations in sync."""
    (models_dir / "example.py").write_text(_default_model_content(default_layer), encoding="utf-8")


def run_init(
    dest: str,
    with_demo: bool,
    with_monitoring: bool,
    backend: str = "pandas",
    use_minimal_stack: bool = False,
    use_docker: bool = True,
) -> None:
    """Create a new project: folder with framework structure (from repo or package skeleton).
    backend: 'pandas' (lighter, default) or 'pyspark' (distributed processing).
    use_minimal_stack: True = LocalExecutor, 4 containers; False = CeleryExecutor, 7 containers (Docker only).
    use_docker: True = Docker Compose stack (default); False = local run (no compose). Runtime is locked per project.
    """
    cwd = Path.cwd().resolve()
    # If dest is a simple name (no path sep), create folder inside cwd
    if dest != "." and "/" not in dest and "\\" not in dest:
        dest_path = (cwd / dest).resolve()
    else:
        dest_path = Path(dest).resolve()

    _skeleton_dir = Path(__file__).resolve().parent / "skeleton"
    if dest_path == cwd:
        # With src layout: __file__ is src/airlakeflow/init_cmd.py -> parent.parent.parent = repo root
        framework_root = Path(__file__).resolve().parent.parent.parent
    else:
        framework_root = cwd
        # If cwd doesn't look like the framework repo, use package skeleton
        if not (framework_root / "dags").is_dir() or not (framework_root / "soda").is_dir():
            framework_root = _skeleton_dir

    dirs_to_copy = [
        "dags",
        "soda",
        "scripts",
        "config",
        "plugins",
        "data",
        "logs",
    ]
    runtime = "local" if not use_docker else "docker"
    files_to_copy = ["README.md"]
    if use_docker:
        files_to_copy.append(".env.example")

    dest_path.mkdir(parents=True, exist_ok=True)
    for d in dirs_to_copy:
        src = framework_root / d
        if not src.exists() and (_skeleton_dir / d).exists():
            src = _skeleton_dir / d
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
        if not src.exists() and (_skeleton_dir / f).exists():
            src = _skeleton_dir / f
        if src.exists():
            shutil.copy2(src, dest_path / f)

    # Docker path: copy compose, Dockerfile, .env
    if use_docker:
        if use_minimal_stack:
            _minimal_yaml = framework_root / "docker-compose.minimal.yaml"
            compose_src = (
                _minimal_yaml
                if _minimal_yaml.exists()
                else _skeleton_dir / "docker-compose.minimal.yaml"
            )
            use_light_image = backend == "pandas" and not with_monitoring
        else:
            compose_src = framework_root / "docker-compose.yaml"
            use_light_image = False
        if compose_src.exists():
            shutil.copy2(compose_src, dest_path / "docker-compose.yaml")
        elif (framework_root / "docker-compose.yaml").exists():
            shutil.copy2(framework_root / "docker-compose.yaml", dest_path / "docker-compose.yaml")
        _df_min = framework_root / "Dockerfile.minimal"
        if not _df_min.exists():
            _df_min = _skeleton_dir / "Dockerfile.minimal"
        if use_light_image and _df_min.exists():
            shutil.copy2(_df_min, dest_path / "Dockerfile")
            _req_min = framework_root / "requirements.minimal.txt"
            req_src = _req_min if _req_min.exists() else _skeleton_dir / "requirements.minimal.txt"
            if req_src.exists():
                shutil.copy2(req_src, dest_path / "requirements.txt")
            elif (framework_root / "requirements.txt").exists():
                shutil.copy2(framework_root / "requirements.txt", dest_path / "requirements.txt")
        else:
            if (framework_root / "Dockerfile").exists():
                shutil.copy2(framework_root / "Dockerfile", dest_path / "Dockerfile")
            if (framework_root / "requirements.txt").exists():
                shutil.copy2(framework_root / "requirements.txt", dest_path / "requirements.txt")
        if create_env_from_example(dest_path):
            secho_info("  ▸ .env created with AIRFLOW_UID for this machine")
    else:
        # Local: requirements for Airflow + backend only
        _req_local = _skeleton_dir / "requirements.local.txt"
        if _req_local.exists():
            shutil.copy2(_req_local, dest_path / "requirements.txt")
        else:
            # Fallback: minimal deps for local run
            (dest_path / "requirements.txt").write_text(
                "apache-airflow>=2.7.0\n" "psycopg2-binary>=2.9.0\n" "pandas>=2.0.0\n",
                encoding="utf-8",
            )

    # Silver backend and runtime (locked per project)
    backend = backend.lower().strip()
    if backend not in ("pandas", "pyspark"):
        backend = "pandas"
    config_path = dest_path / ".airlakeflow.yaml"
    config_path.write_text(
        f"""# AirLakeFlow project config (runtime is set at init and should not be changed)
runtime: {runtime}
silver_backend: {backend}
architecture: medallion
# soda_data_source: postgres_datawarehouse
# soda_config_path: soda/configuration.yaml
# contracts_dir: soda/contracts
""",
        encoding="utf-8",
    )
    req_path = dest_path / "requirements.txt"
    if req_path.exists():
        text = req_path.read_text(encoding="utf-8")
        if backend == "pyspark" and "pyspark" not in text:
            text = text.rstrip() + "\n\n# Distributed processing (chosen at init)\npyspark>=3.5.0\n"
            req_path.write_text(text, encoding="utf-8")

    if not with_demo:
        for name in ("demo",):
            demo_dir = dest_path / "dags" / name
            if demo_dir.exists():
                shutil.rmtree(demo_dir)
        for c in ("user_bronze", "user_silver", "task_bronze", "task_silver"):
            contract = dest_path / "soda" / "contracts" / f"{c}.yaml"
            if contract.exists():
                contract.unlink()
        # Remove demo models so init can create default example.py when no models left
        for m in ("user.py", "task.py", "user_bronze.py", "task_bronze.py"):
            model_file = dest_path / "config" / "models" / m
            if model_file.exists():
                model_file.unlink()

    if not with_monitoring:
        monitoring_dir = dest_path / "dags" / "monitoring"
        if monitoring_dir.exists():
            shutil.rmtree(monitoring_dir)
        for m in list((dest_path / "dags" / "sql" / "migrations").glob("V005*.sql")) + list(
            (dest_path / "dags" / "sql" / "migrations").glob("V006*.sql")
        ):
            m.unlink()
        # When no Soda: add ALF-Checks (native) for the demo (User + Task)
        if with_demo:
            try:
                from airlakeflow.data_tests_cmd import create_alf_check_file, run_data_tests_cmd

                run_data_tests_cmd(dest_path)
                for schema, table in (
                    ("bronze", "user"),
                    ("silver", "user"),
                    ("bronze", "task"),
                    ("silver", "task"),
                ):
                    create_alf_check_file(dest_path, schema, table)
            except Exception:
                pass

    # Create default model if config/models/ has no model files (so migrations have a reference)
    models_dir = dest_path / "config" / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    existing_models = [
        p for p in models_dir.glob("*.py") if p.name != "__init__.py" and not p.name.startswith("_")
    ]
    if not existing_models:
        from airlakeflow.config import get_architecture_from_config, load_config

        cfg = load_config(dest_path)
        arch = get_architecture_from_config(cfg)
        _write_default_model(models_dir, arch.default_layer)
        secho_info("  ▸ Default model created: config/models/example.py")

    # Generate default migrations from config/models/ so model and migrations stay in sync
    try:
        from airlakeflow.config import get_migration_driver, load_config
        from airlakeflow.migration_gen import generate_migrations

        cfg = load_config(dest_path)
        driver = get_migration_driver(cfg)
        created = generate_migrations(dest_path, driver=driver)
        if created:
            secho_info(f"  ▸ {len(created)} migration(s) generated from models")
    except Exception:
        pass  # do not fail init if migration generation fails (e.g. no dialect)

    # With demo: generate 00_seeds DAG so the project runs without manual steps (setup → seeds → demo_pipeline)
    if with_demo:
        try:
            from airlakeflow.seed_cmd import run_seed

            run_seed(dest_path)
        except Exception:
            pass

    # Create a venv in the project so the user can install deps right away
    venv_dir = dest_path / ".venv"
    if not venv_dir.exists():
        try:
            subprocess.run(
                [sys.executable, "-m", "venv", str(venv_dir)],
                check=True,
                capture_output=True,
            )
            secho_info(
                "  ▸ venv created: .venv (activate with source .venv/bin/activate or .venv\\Scripts\\activate)"
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            pass  # do not fail init if venv creation fails

    secho_ok(f"{SYM_OK} Project created: {dest_path}")
    if framework_root.name == "skeleton":
        secho_info("  (AirLakeFlow package structure)")
    if use_docker:
        if use_minimal_stack:
            secho_info("  ▸ Stack: minimal (LocalExecutor, 4 containers)")
        if backend == "pandas" and not with_monitoring and use_minimal_stack:
            secho_info("  ▸ Image: minimal (no Java, requirements.minimal)")
    else:
        secho_info(
            "  ▸ Runtime: local (no Docker). Run: alf run  (or: pip install -r requirements.txt then airflow db init && airflow standalone)"
        )
    if with_demo and (dest_path / "dags" / "demo").exists():
        secho_info("  ▸ Demo pipeline (User + Task) included")
    if with_monitoring and (dest_path / "dags" / "monitoring").exists():
        secho_info("  ▸ Monitoring and Soda included")
    secho_info(f"  ▸ Silver backend: {backend} (edit .airlakeflow.yaml to change)")
