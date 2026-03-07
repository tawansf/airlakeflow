import shutil
from pathlib import Path

from airlakeflow.docker_cmd import create_env_from_example
from airlakeflow.style import SYM_OK, secho_heading, secho_info, secho_ok


def run_init(
    dest: str,
    with_demo: bool,
    with_monitoring: bool,
    backend: str = "pandas",
    use_minimal_stack: bool = False,
) -> None:
    """Create a new project: folder with framework structure (from repo or package skeleton).
    backend: 'pandas' (lighter, default) or 'pyspark' (distributed processing).
    use_minimal_stack: True = LocalExecutor, 4 containers; False = CeleryExecutor, 7 containers.
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
    # Choose compose and image: minimal stack = LocalExecutor, lighter image when pandas + no Soda
    if use_minimal_stack:
        _minimal_yaml = framework_root / "docker-compose.minimal.yaml"
        compose_src = _minimal_yaml if _minimal_yaml.exists() else _skeleton_dir / "docker-compose.minimal.yaml"
        use_light_image = backend == "pandas" and not with_monitoring
    else:
        compose_src = framework_root / "docker-compose.yaml"
        use_light_image = False

    files_to_copy = [
        ".env.example",
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

    # Compose: minimal or full
    if compose_src.exists():
        shutil.copy2(compose_src, dest_path / "docker-compose.yaml")
    elif (framework_root / "docker-compose.yaml").exists():
        shutil.copy2(framework_root / "docker-compose.yaml", dest_path / "docker-compose.yaml")

    # Dockerfile + requirements: light image only for minimal + pandas + no monitoring
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

    # Silver backend: write config and adjust requirements
    backend = backend.lower().strip()
    if backend not in ("pandas", "pyspark"):
        backend = "pandas"
    config_path = dest_path / ".airlakeflow.yaml"
    config_path.write_text(
        f"""# AirLakeFlow project config
silver_backend: {backend}
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
        # If pandas, leave requirements as-is (skeleton has no pyspark by default)

    # Create .env with AIRFLOW_UID = current user (Unix) or 50000 (Windows) so alf run works without permission errors
    if create_env_from_example(dest_path):
        secho_info("  ▸ .env criado com AIRFLOW_UID para esta máquina")

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

    secho_ok(f"{SYM_OK} Projeto criado: {dest_path}")
    if framework_root.name == "skeleton":
        secho_info("  (estrutura do pacote AirLakeFlow)")
    if use_minimal_stack:
        secho_info("  ▸ Stack: mínima (LocalExecutor, 4 containers)")
    if use_light_image:
        secho_info("  ▸ Imagem: mínima (sem Java, requirements.minimal)")
    if with_demo and (dest_path / "dags" / "crypto").exists():
        secho_info("  ▸ DAG demo (crypto) incluída")
    if with_monitoring and (dest_path / "dags" / "monitoring").exists():
        secho_info("  ▸ Monitoring e Soda incluídos")
    secho_info(f"  ▸ Backend Silver: {backend} (edite .airlakeflow.yaml para alterar)")
