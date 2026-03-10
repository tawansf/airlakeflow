import sys
from pathlib import Path

import click

from airlakeflow import __version__ as _pkg_version
from airlakeflow.add_soda import run_add_soda
from airlakeflow.data_tests_cmd import run_data_tests_cmd
from airlakeflow.config import (
    get_architecture_from_config,
    get_runtime,
    load_config,
    resolve_project_root,
)
from airlakeflow.docker_cmd import (
    run_down,
    run_exec,
    run_logs,
    run_ps,
    run_restart,
    run_status,
    run_stop,
    run_up,
)
from airlakeflow.docs_cmd import run_docs
from airlakeflow.doctor_cmd import run_doctor
from airlakeflow.init_cmd import run_init
from airlakeflow.local_cmd import run_local
from airlakeflow.migrations_cmd import (
    run_align as run_migrations_align,
)
from airlakeflow.migrations_cmd import (
    run_doctor as run_migrations_doctor,
)
from airlakeflow.migrations_cmd import (
    run_down as run_migrations_down,
)
from airlakeflow.migrations_cmd import (
    run_gen as run_migrations_gen,
)
from airlakeflow.migrations_cmd import (
    run_up as run_migrations_up,
)
from airlakeflow.new_etl import run_new_etl
from airlakeflow.new_migration import discover_dags, run_new_migration
from airlakeflow.new_model_cmd import run_new_model
from airlakeflow.seed_cmd import run_seed
from airlakeflow.style import SYM_LIST, print_banner, secho_fail, secho_ok, secho_warn
from airlakeflow.upgrade_cmd import run_upgrade
from airlakeflow.validate_cmd import run_validate

# Command groups for help sections (same order as in alf --help)
DOCKER_COMMANDS = frozenset({"down", "exec", "logs", "ps", "restart", "run", "status", "stop"})
QUALITY_COMMANDS = frozenset({"add"})
FRAMEWORK_PROJETO = frozenset({"init", "upgrade", "validate", "doctor", "help", "version"})
FRAMEWORK_RECURSOS = frozenset({"new", "list", "migrations", "seed", "docs"})
HIDDEN_ALIASES = frozenset({"m"})  # migrations alias; show only "migrations"


class AlfGroup(click.Group):
    """Group that lists commands in sections: Project, Resources, Quality, Docker (stack)."""

    def format_commands(self, ctx, formatter):
        commands = sorted(self.list_commands(ctx))
        projeto = [c for c in commands if c in FRAMEWORK_PROJETO]
        recursos = [c for c in commands if c in FRAMEWORK_RECURSOS]
        quality = [c for c in commands if c in QUALITY_COMMANDS]
        docker = [c for c in commands if c in DOCKER_COMMANDS]
        max_len = max((len(name) for name in commands), default=0)
        limit = formatter.width - 6 - max_len if formatter.width else 72

        def write_block(rows, section_title):
            if not rows:
                return
            with formatter.section(section_title):
                formatter.write_dl(rows)

        def rows_for(names):
            out = []
            for name in names:
                cmd = self.get_command(ctx, name)
                if cmd is None or getattr(cmd, "hidden", False):
                    continue
                help_str = (
                    cmd.get_short_help_str(limit)
                    if hasattr(cmd, "get_short_help_str")
                    else (cmd.help or "")
                )
                out.append((name, help_str))
            return out

        write_block(rows_for(projeto), "Project")
        write_block(rows_for(recursos), "Resources")
        write_block(rows_for(quality), "Quality")
        write_block(rows_for(docker), "Docker (stack)")


def _get_version() -> str:
    try:
        from importlib.metadata import version

        return version("airlakeflow")
    except Exception:
        return _pkg_version


@click.group(cls=AlfGroup, invoke_without_command=True, add_help_option=False)
@click.pass_context
def _cli(ctx: click.Context):
    """AirLakeFlow — CLI for the framework (Bronze / Silver / Gold)."""
    if ctx.invoked_subcommand is None:
        print_banner(_get_version())
        click.echo(ctx.get_help())
        ctx.exit(0)


# Public alias for testing and programmatic invocation (CliRunner expects a Command, not main()).
cli = _cli


@_cli.command("help", help="Show this message and exit.")
@click.pass_context
def _show_help(ctx: click.Context):
    """Show main group help (alf help / alf h)."""
    click.echo(ctx.parent.get_help())
    ctx.exit(0)


@_cli.command("h", hidden=True)
@click.pass_context
def _show_help_h(ctx: click.Context):
    click.echo(ctx.parent.get_help())
    ctx.exit(0)


@_cli.command("version", help="Show version and exit.")
@click.pass_context
def _show_version(ctx: click.Context):
    click.echo(f"AirLakeFlow {_get_version()}")
    ctx.exit(0)


@_cli.command("v", hidden=True)
@click.pass_context
def _show_version_v(ctx: click.Context):
    click.echo(f"AirLakeFlow {_get_version()}")
    ctx.exit(0)


@_cli.group()
def new():
    """Create new resource (ETL, Migration, Contract, Model)."""
    pass


@_cli.group("list")
def list_group():
    """List project resources (ETLs, etc.)."""
    pass


# Only Medallion (3 layers) for now; other architectures later.
MEDALLION_LAYERS = ["bronze", "silver", "gold"]


def _architecture_layers(project_root: str = ".") -> list[str]:
    """Return layer names (Medallion: bronze, silver, gold)."""
    try:
        root = Path(resolve_project_root(project_root))
        cfg = load_config(root)
        return get_architecture_from_config(cfg).layers
    except Exception:
        return list(MEDALLION_LAYERS)


def _architecture_default_layer(project_root: str = ".") -> str:
    """Return default layer from project architecture (e.g. silver for Medallion)."""
    try:
        root = Path(resolve_project_root(project_root))
        cfg = load_config(root)
        return get_architecture_from_config(cfg).default_layer
    except Exception:
        return "silver"


def _project_root_option(**kwargs):
    return click.option(
        "-r",
        "project_root",
        type=click.Path(exists=True, file_okay=False, dir_okay=True),
        default=".",
        help="Project root (default: current directory)",
        **kwargs,
    )


@list_group.command("etls")
@_project_root_option()
def list_etls(project_root: str):
    """List ETL pipelines (dags/ folders that contain pipeline.py)."""
    root = Path(resolve_project_root(project_root))
    etls = discover_dags(root)
    if not etls:
        secho_fail("No ETLs found in dags/. Create one with 'alf new etl NAME'.")
        raise SystemExit(1)
    for name in etls:
        click.echo(f"  {SYM_LIST} {name}")


@new.command("etl")
@click.argument("name", type=str)
@click.option("-t", "table_name", default=None, help="Table name. Omit to be asked.")
@click.option(
    "-c",
    "with_contracts",
    is_flag=True,
    default=False,
    help="Generate Soda contracts. Omit to be asked.",
)
@click.option("-g", "with_gold", is_flag=True, default=True, help="Include Gold layer. Omit to be asked.")
@click.option("-G", "no_gold", is_flag=True, default=False, help="Exclude Gold")
@click.option(
    "-s",
    "source",
    type=click.Choice(["api", "file", "jdbc", "kafka", "s3", "gcs"]),
    default="api",
    help="Bronze source. Omit to be asked.",
)
@click.option(
    "-p",
    "no_spark",
    is_flag=True,
    default=None,
    help="Silver with pandas only (no Spark). Default from .airlakeflow.yaml (silver_backend)",
)
@click.option(
    "-k",
    "use_spark_flag",
    is_flag=True,
    default=None,
    help="Silver with PySpark. Default from .airlakeflow.yaml",
)
@click.option(
    "--pattern",
    "pattern",
    type=click.Choice(["default", "snapshot"], case_sensitive=False),
    default="default",
    help="Silver pattern (default or snapshot). Omit to be asked.",
)
@click.option(
    "--partition-by",
    "partition_by",
    type=str,
    default=None,
    help="Partition column. Omit to be asked.",
)
@click.option(
    "--incremental-by",
    "incremental_by",
    type=str,
    default=None,
    help="Incremental column. Omit to be asked.",
)
@click.option(
    "-r",
    "project_root",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    default=".",
    help="Project root (default: current directory)",
)
def new_etl(
    name: str,
    table_name: str | None,
    with_contracts: bool,
    with_gold: bool,
    no_gold: bool,
    source: str,
    no_spark: bool | None,
    use_spark_flag: bool | None,
    pattern: str,
    partition_by: str | None,
    incremental_by: str | None,
    project_root: str,
):
    """Create a new ETL pipeline (Bronze -> Silver -> Gold) in the current project."""
    if no_gold:
        with_gold = False
    project_root = str(resolve_project_root(project_root))
    if sys.stdin.isatty():
        try:
            import questionary
            table = table_name or (questionary.text("Table name?", default=name).ask() or name)
            with_gold = questionary.select(
                "Include Gold layer?",
                choices=[questionary.Choice("Yes", True), questionary.Choice("No", False)],
                default=with_gold,
            ).ask()
            if with_gold is None:
                raise KeyboardInterrupt
            with_contracts = questionary.select(
                "Include Soda contracts?",
                choices=[questionary.Choice("No", False), questionary.Choice("Yes", True)],
                default=with_contracts,
            ).ask()
            if with_contracts is None:
                raise KeyboardInterrupt
            source = questionary.select(
                "Bronze source type",
                choices=["api", "file", "jdbc", "kafka", "s3", "gcs"],
                default=source,
            ).ask()
            if source is None:
                raise KeyboardInterrupt
            pattern = questionary.select(
                "Pattern Silver",
                choices=[questionary.Choice("default", "default"), questionary.Choice("snapshot (SCD2)", "snapshot")],
                default=pattern,
            ).ask() or "default"
            if pattern is None:
                raise KeyboardInterrupt
            p = questionary.text("Partition by column (optional, Enter to skip):", default=partition_by or "").ask()
            partition_by = p.strip() or None if p is not None else partition_by
            inc = questionary.text("Incremental by column (optional, Enter to skip):", default=incremental_by or "").ask()
            incremental_by = inc.strip() or None if inc is not None else incremental_by
        except ImportError:
            table = table_name or name
    else:
        table = table_name or name
    if use_spark_flag:
        use_spark = True
    elif no_spark:
        use_spark = False
    else:
        cfg = load_config(Path(project_root))
        use_spark = cfg.get("silver_backend", "pandas") == "pyspark"
    run_new_etl(
        name=name,
        table_name=table,
        with_contracts=with_contracts,
        with_gold=with_gold,
        source=source,
        use_spark=use_spark,
        pattern=pattern.lower(),
        partition_by=partition_by,
        incremental_by=incremental_by,
        project_root=project_root,
    )


@new.command("migration")
@click.argument("name", type=str)
@click.option(
    "-d", "dag", default=None, help="DAG name. Omit to be asked interactively."
)
@click.option(
    "-l",
    "layer",
    type=click.Choice(MEDALLION_LAYERS),
    default=None,
    help="Layer (bronze/silver/gold). Omit to be asked interactively.",
)
@click.option(
    "-r",
    "project_root",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    default=".",
    help="Project root (default: current directory)",
)
def new_migration(name: str, dag: str | None, layer: str | None, project_root: str):
    """Create a migration for an existing DAG. DAG and layer are asked interactively if omitted."""
    project_root = str(resolve_project_root(project_root))
    root = Path(project_root).resolve()
    dags = discover_dags(root)
    if not dags:
        secho_fail("No DAGs found in dags/. Create one with 'alf new etl NAME'.")
        raise SystemExit(1)
    if dag is None:
        if sys.stdin.isatty():
            try:
                import questionary
                dag = questionary.select("Which DAG?", choices=dags).ask()
                if dag is None:
                    raise KeyboardInterrupt
            except ImportError:
                dag = click.prompt("Choose the DAG", type=click.Choice(dags))
        else:
            dag = click.prompt("Choose the DAG", type=click.Choice(dags))
    if layer is None:
        if sys.stdin.isatty():
            try:
                import questionary
                layer = questionary.select("Layer?", choices=MEDALLION_LAYERS).ask()
                if layer is None:
                    raise KeyboardInterrupt
            except ImportError:
                layer = click.prompt("Layer", type=click.Choice(MEDALLION_LAYERS))
        else:
            layer = click.prompt("Layer", type=click.Choice(MEDALLION_LAYERS))
    run_new_migration(
        name=name, dag=dag, layer=layer.lower() if layer else "bronze", project_root=project_root
    )


def _tables_by_schema(project_root: Path) -> dict[str, list[str]]:
    """Return {schema: [table1, table2, ...]} from config/models."""
    from airlakeflow.model_loader import discover_models
    out: dict[str, list[str]] = {}
    for m in discover_models(Path(project_root)):
        s, t = m.get_schema(), m.get_table_name()
        out.setdefault(s, []).append(t)
    for s in out:
        out[s] = sorted(set(out[s]))
    return out


@new.command("contract")
@click.argument("schema", type=str, required=False, default=None)
@click.argument("table", type=str, required=False, default=None)
@click.option(
    "-r",
    "project_root",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    default=".",
    help="Project root",
)
def new_contract(schema: str | None, table: str | None, project_root: str):
    """Create a new contract (Soda or ALF-Checks). Schema = layer. Type, schema and table are asked if omitted."""
    project_root = str(resolve_project_root(project_root))
    root = Path(project_root)
    contract_type: str | None = None  # "soda" | "alf_checks"
    if sys.stdin.isatty() and (schema is None or table is None):
        try:
            import questionary
            schema = schema or questionary.select(
                "Schema (layer)?",
                choices=MEDALLION_LAYERS,
                default="bronze",
            ).ask()
            if schema is None:
                raise KeyboardInterrupt
            tables_by_schema = _tables_by_schema(root)
            tables = tables_by_schema.get(schema, [])
            if tables:
                table = table or questionary.select("Table?", choices=tables).ask()
            else:
                table = table or questionary.text("Table name?").ask()
            if not table:
                raise click.UsageError("Table is required.")
            contract_type = questionary.select(
                "Contract type?",
                choices=[
                    questionary.Choice("Soda (soda/contracts/)", "soda"),
                    questionary.Choice("ALF-Checks (config/checks/)", "alf_checks"),
                ],
            ).ask()
            if contract_type is None:
                raise KeyboardInterrupt
        except ImportError:
            if schema is None:
                schema = "bronze"
            table = table or click.prompt("Table name?")
            contract_type = "soda"
    if not schema or not table:
        raise click.UsageError(
            "SCHEMA and TABLE are required (or use interactive mode). "
            "Example: alf new contract bronze my_table"
        )
    if contract_type is None and sys.stdin.isatty():
        try:
            import questionary
            contract_type = questionary.select(
                "Contract type?",
                choices=[
                    questionary.Choice("Soda (soda/contracts/)", "soda"),
                    questionary.Choice("ALF-Checks (config/checks/)", "alf_checks"),
                ],
            ).ask()
            if contract_type is None:
                raise KeyboardInterrupt
        except ImportError:
            contract_type = "soda"
    if contract_type is None:
        contract_type = "soda"
    if contract_type == "alf_checks":
        from airlakeflow.data_tests_cmd import create_alf_check_file
        path = create_alf_check_file(root, schema, table)
        secho_ok(f"ALF-Check created: {path}")
        return
    from airlakeflow.new_contract_cmd import run_new_contract
    run_new_contract(schema=schema, table=table, layer=schema, project_root=root)


@new.command("model")
@click.argument("name", type=str)
@click.option(
    "-l",
    "layer",
    type=click.Choice(MEDALLION_LAYERS, case_sensitive=False),
    default=None,
    help="Layer (bronze/silver/gold). Omit to be asked interactively.",
)
@click.option(
    "--partition-by",
    "partition_by",
    type=str,
    default=None,
    help="Partition key column. Omit to be asked interactively.",
)
@_project_root_option()
def new_model(name: str, layer: str | None, partition_by: str | None, project_root: str):
    """Create a new model in config/models/ and generate its migration (dags/sql/migrations/)."""
    project_root = str(resolve_project_root(project_root))
    root = Path(project_root)
    if sys.stdin.isatty():
        try:
            import questionary
            layer = questionary.select(
                "Layer?",
                choices=MEDALLION_LAYERS,
                default=layer or _architecture_default_layer(project_root),
            ).ask()
            if layer is None:
                raise KeyboardInterrupt
            p = questionary.text("Partition by column (optional, Enter to skip):", default=partition_by or "").ask()
            partition_by = p.strip() or None if p is not None else partition_by
        except ImportError:
            if layer is None:
                layer = _architecture_default_layer(project_root)
    elif layer is None:
        layer = _architecture_default_layer(project_root)
    run_new_model(name=name, layer_name=layer, project_root=root, partition_by=partition_by)


@_cli.group()
def migrations_group():
    """Generate, apply, list, and check migrations (models ↔ SQL)."""
    pass


_cli.add_command(migrations_group, "migrations")
_cli.add_command(migrations_group, "m")


@migrations_group.command("generate")
@click.option(
    "-D",
    "driver",
    type=str,
    default=None,
    help="SQL dialect (postgres, etc.). Default: from config migration_driver",
)
@_project_root_option()
def migrations_generate(project_root: str, driver: str | None):
    """Generate migration SQL files from config/models/."""
    root = Path(resolve_project_root(project_root))
    raise SystemExit(run_migrations_gen(root, driver))


@migrations_group.command("up")
@click.option(
    "-u",
    "uri",
    type=str,
    default=None,
    help="Postgres URI. Default: AIRFLOW_CONN_POSTGRES_DATAWAREHOUSE or config",
)
@_project_root_option()
def migrations_up(project_root: str, uri: str | None):
    """Apply pending migrations to the data warehouse."""
    root = Path(resolve_project_root(project_root))
    raise SystemExit(run_migrations_up(root, uri))


@migrations_group.command("down")
@click.option("-n", "dry_run", is_flag=True, help="Only show what would be dropped")
@click.option("-F", "force", is_flag=True, help="Skip confirmation prompt")
@click.option("-u", "uri", type=str, default=None, help="Postgres URI")
@_project_root_option()
def migrations_down(project_root: str, dry_run: bool, force: bool, uri: str | None):
    """Rollback last applied migration (with confirmation)."""
    root = Path(resolve_project_root(project_root))
    raise SystemExit(run_migrations_down(root, uri, dry_run=dry_run, force=force))


@migrations_group.command("doctor")
@click.option("-D", "driver", type=str, default=None, help="SQL dialect for comparison")
@_project_root_option()
def migrations_doctor(project_root: str, driver: str | None):
    """Compare models with migrations; report drift."""
    root = Path(resolve_project_root(project_root))
    raise SystemExit(run_migrations_doctor(root, driver))


@migrations_group.command("align")
@click.option("-D", "driver", type=str, default=None, help="SQL dialect. Default: from config")
@click.option("-F", "force", is_flag=True, help="Skip confirmation prompt")
@_project_root_option()
def migrations_align(project_root: str, driver: str | None, force: bool):
    """Align migrations to models (model is reference; overwrites migration files). Asks to confirm when there are differences."""
    root = Path(resolve_project_root(project_root))
    raise SystemExit(run_migrations_align(root, driver, force=force))


@_cli.group()
def add():
    """Add quality integration (Soda, Great Expectations, etc.) to the project."""
    pass


@add.command("soda")
@click.option("-e", "etl_name", default=None, help="Apply only to this ETL (directory in dags/)")
@click.option("-a", "all_etls", is_flag=True, help="Apply to all ETLs (Project complete)")
@click.option(
    "-r",
    "project_root",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    default=".",
    help="Project root",
)
def add_soda(etl_name: str | None, all_etls: bool, project_root: str):
    """Integrate Soda into the project: config, contracts and scan tasks in the ETLs."""
    project_root = str(resolve_project_root(project_root))
    root = Path(project_root).resolve()
    dags = discover_dags(root)
    if not dags:
        secho_fail("No ETLs found in dags/. Create one with 'alf new etl NAME'.")
        raise SystemExit(1)
    run_add_soda(project_root=root, etl_name=etl_name, all_etls=all_etls)


@add.command("greatxp")
@click.option("-e", "etl", default=None, help="Specific ETL (in development)")
@click.option("-a", "all_etls", is_flag=True, help="All ETLs (in development)")
@click.option(
    "-r",
    "project_root",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    default=".",
    help="Project root (default: current directory)",
)
def add_greatxp(etl: str | None, all_etls: bool, project_root: str):
    """Integrate Great Expectations into the project (in development)."""
    secho_warn("alf add greatxp: in development.")


@add.command("alf-checks")
@click.option(
    "-r",
    "project_root",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    default=".",
    help="Project root (default: current directory)",
)
def add_alf_checks(project_root: str):
    """Add ALF-Checks (native data checks): config/checks/ and DAG 01_alf_checks. Alternative to Soda."""
    project_root = str(resolve_project_root(project_root))
    run_data_tests_cmd(Path(project_root))


@_cli.command("validate")
@click.option(
    "-r",
    "project_root",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    default=".",
    help="Project root (default: current directory)",
)
@click.option(
    "-N", "no_docker", is_flag=True, help="Skip Docker and stack checks (only validate structure)"
)
@click.option("-S", "no_stack", is_flag=True, help="Skip check that Docker Compose stack is up")
@click.option("-q", "quiet", is_flag=True, help="Only exit with code 0/1, minimal output")
def validate(project_root: str, no_docker: bool, no_stack: bool, quiet: bool):
    """Check project structure (dags/, soda/, docker-compose) and Docker (daemon, compose, stack up)."""
    project_root = str(resolve_project_root(project_root))
    ok = run_validate(
        project_root=project_root,
        check_docker=not no_docker,
        check_structure=True,
        check_stack_up=not no_stack,
        verbose=not quiet,
    )
    if not ok:
        raise SystemExit(1)


@_cli.command("doctor")
@click.option(
    "-r",
    "project_root",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    default=".",
    help="Project root (default: current directory)",
)
@click.option("-q", "quiet", is_flag=True, help="Minimal output")
def doctor(project_root: str, quiet: bool):
    """Extended validation: structure, Docker, Python, permissions; suggests fixes."""
    project_root = str(resolve_project_root(project_root))
    ok = run_doctor(Path(project_root), verbose=not quiet)
    if not ok:
        raise SystemExit(1)


@_cli.command("seed")
@click.option(
    "-r",
    "project_root",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    default=".",
    help="Project root (default: current directory)",
)
def seed(project_root: str):
    """Ensure data/seeds/ exists and generate DAG 00_seeds to load CSVs into bronze."""
    project_root = str(resolve_project_root(project_root))
    run_seed(Path(project_root))


@_cli.command("docs")
@click.option(
    "-r",
    "project_root",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    default=".",
    help="Project root (default: current directory)",
)
@click.option(
    "-o",
    "output_dir",
    type=str,
    default=None,
    help="Output directory (default: docs/)",
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["html", "json"], case_sensitive=False),
    default="html",
    help="Catalog format: html or json",
)
def docs(project_root: str, output_dir: str | None, fmt: str):
    """Generate static catalog from config/models and dags/sql/migrations (docs/catalog.html or .json)."""
    project_root = str(resolve_project_root(project_root))
    run_docs(Path(project_root), output_dir=output_dir, fmt=fmt)


@_cli.command("status")
@_project_root_option()
def status(project_root: str):
    """Show stack status summary (how many services running)."""
    project_root = str(resolve_project_root(project_root))
    code = run_status(Path(project_root))
    raise SystemExit(code)


@_cli.command("exec")
@_project_root_option()
@click.argument("service", type=str)
@click.argument("command", nargs=-1, required=True)
def exec_cmd(project_root: str, service: str, command: tuple[str, ...]):
    """Run a command inside a service container (e.g. alf exec airflow-scheduler airflow dags list)."""
    project_root = str(resolve_project_root(project_root))
    code = run_exec(Path(project_root), service, list(command))
    raise SystemExit(code)


@_cli.command("upgrade")
@_project_root_option()
@click.option("-n", "dry_run", is_flag=True, help="Only show what would be updated")
@click.option("-B", "no_backup", is_flag=True, help="Do not backup files before overwriting")
def upgrade(project_root: str, dry_run: bool, no_backup: bool):
    """Update project files from the framework skeleton (optional backup in .airlakeflow_backup/)."""
    project_root = str(resolve_project_root(project_root))
    run_upgrade(Path(project_root), dry_run=dry_run, backup=not no_backup)


@_cli.command("run")
@_project_root_option()
@click.option("-b", "build", is_flag=True, help="Build images before starting (Docker only)")
@click.option(
    "-f",
    "foreground",
    is_flag=True,
    help="Run in foreground (Docker: no -d; local: always foreground)",
)
def run(project_root: str, build: bool, foreground: bool):
    """Start the application (Docker: compose up; local: install deps, airflow db init, airflow standalone)."""
    root = Path(resolve_project_root(project_root))
    if get_runtime(root) == "local":
        code = run_local(root)
    else:
        code = run_up(root, detach=not foreground, build=build)
    raise SystemExit(code)


@_cli.command("stop")
@_project_root_option()
def stop(project_root: str):
    """Stop the application (docker compose stop)."""
    project_root = str(resolve_project_root(project_root))
    code = run_stop(Path(project_root))
    raise SystemExit(code)


@_cli.command("restart")
@_project_root_option()
def restart(project_root: str):
    """Restart the application (stop then up -d)."""
    project_root = str(resolve_project_root(project_root))
    code = run_restart(Path(project_root))
    raise SystemExit(code)


@_cli.command("down")
@_project_root_option()
@click.option("-v", "volumes", is_flag=True, help="Remove named Docker volumes")
def down(project_root: str, volumes: bool):
    """Tear down the stack (Docker Compose down)."""
    project_root = str(resolve_project_root(project_root))
    code = run_down(Path(project_root), volumes=volumes)
    raise SystemExit(code)


@_cli.command("logs")
@_project_root_option()
@click.option("-f", "follow", is_flag=True, help="Follow Docker logs output")
@click.argument("service", required=False, default=None)
def logs(project_root: str, follow: bool, service: str | None):
    """Show container logs (Docker Compose logs)."""
    project_root = str(resolve_project_root(project_root))
    code = run_logs(Path(project_root), follow=follow, service=service)
    raise SystemExit(code)


@_cli.command("ps")
@_project_root_option()
def ps(project_root: str):
    """List running services (docker compose ps)."""
    project_root = str(resolve_project_root(project_root))
    code = run_ps(Path(project_root))
    raise SystemExit(code)


@_cli.command("init")
@click.argument("project_name", type=str, default=".", required=False)
@click.option(
    "-d",
    "demo",
    is_flag=True,
    default=None,
    flag_value=True,
    help="Include demo (User+Tasks pipeline). Default: yes.",
)
@click.option(
    "-D",
    "no_demo",
    is_flag=True,
    default=None,
    help="Exclude demo (minimal project, no demo DAGs).",
)
@click.option(
    "-m",
    "with_monitoring",
    is_flag=True,
    default=None,
    flag_value=True,
    help="Include Soda (data quality). If omitted, init will ask interactively.",
)
@click.option(
    "-M",
    "no_monitoring",
    is_flag=True,
    default=None,
    help="Exclude monitoring. If omitted, init will ask interactively.",
)
@click.option(
    "-b",
    "backend",
    type=click.Choice(["pandas", "pyspark"], case_sensitive=False),
    default=None,
    help="Silver layer: pandas or pyspark. If omitted, init will ask interactively.",
)
@click.option(
    "-w",
    "use_docker",
    is_flag=True,
    default=None,
    flag_value=True,
    help="Use Docker (compose). If omitted, init will ask.",
)
@click.option(
    "-W",
    "no_docker",
    is_flag=True,
    default=None,
    help="No Docker (local run). Runtime is locked per project.",
)
@click.option(
    "-s",
    "use_minimal_stack",
    type=click.Choice(["minimal", "full"], case_sensitive=False),
    default=None,
    help="Stack: minimal (4 containers) or full (7). Docker only; if omitted, init will ask when Docker.",
)
def init(
    project_name: str,
    demo: bool | None,
    no_demo: bool | None,
    with_monitoring: bool | None,
    no_monitoring: bool | None,
    backend: str | None,
    use_docker: bool | None,
    no_docker: bool | None,
    use_minimal_stack: str | None,
):
    """Create a new project. Run without flags to choose options interactively."""
    if no_demo:
        demo = False
    if no_monitoring:
        with_monitoring = False
    if no_docker:
        use_docker = False
    if use_minimal_stack is not None:
        use_minimal_stack = use_minimal_stack == "minimal"
    interactive = sys.stdin.isatty()
    try:
        import questionary
        from questionary import Style

        _has_select = True
        # First item in each list is the default (no default= needed — avoids the
        # "selected" token branch in InquirerControl which conflicts with highlighted).
        _select_style = Style(
            [
                ("qmark", "fg:#5f819d"),
                ("question", "bold"),
                ("answer", "fg:#FF9D00 bold"),
                ("pointer", "bold fg:cyan"),
                ("highlighted", "bold fg:cyan"),
                ("selected", ""),
                ("text", ""),
                ("instruction", ""),
            ]
        )
    except ImportError:
        _has_select = False
        _select_style = None

    # 1. Runtime (Docker vs local) — defines how the project runs
    if use_docker is None:
        if interactive and _has_select:
            choice = questionary.select(
                "Run with Docker or locally (no Docker)?",
                choices=[
                    # Docker first = default
                    questionary.Choice("Docker (compose)", value=True),
                    questionary.Choice("Local (no Docker)", value=False),
                ],
                pointer="→",
                style=_select_style,
            ).ask()
            if choice is None:
                raise KeyboardInterrupt
            use_docker = choice
        elif interactive:
            use_docker = click.confirm("Run with Docker (compose)?", default=True)
        else:
            use_docker = True
    # 2. Stack version (Docker only)
    if use_docker and use_minimal_stack is None:
        if interactive and _has_select:
            choice = questionary.select(
                "Stack version",
                choices=[
                    # minimal first = default
                    questionary.Choice("minimal (4 containers, LocalExecutor)", value="minimal"),
                    questionary.Choice("full (7 containers, Celery)", value="full"),
                ],
                pointer="→",
                style=_select_style,
            ).ask()
            if choice is None:
                raise KeyboardInterrupt
            use_minimal_stack = (choice or "minimal") == "minimal"
        elif interactive:
            stack_choice = click.prompt(
                "Stack: minimal (4 containers) or full (7)?",
                type=click.Choice(["minimal", "full"], case_sensitive=False),
                default="minimal",
                show_choices=True,
                show_default=True,
            )
            use_minimal_stack = stack_choice == "minimal"
        else:
            use_minimal_stack = True
    # 3. Silver layer backend
    if backend is None:
        if interactive and _has_select:
            choice = questionary.select(
                "Silver layer backend",
                choices=[
                    # pandas first = default
                    questionary.Choice("pandas (lightweight, single machine)", value="pandas"),
                    questionary.Choice("pyspark (distributed)", value="pyspark"),
                ],
                pointer="→",
                style=_select_style,
            ).ask()
            if choice is None:
                raise KeyboardInterrupt
            backend = choice
        elif interactive:
            backend = click.prompt(
                "Silver layer backend",
                type=click.Choice(["pandas", "pyspark"], case_sensitive=False),
                default="pandas",
                show_choices=True,
            )
        else:
            backend = "pandas"
    # 4. Soda (data quality)
    if with_monitoring is None:
        if interactive and _has_select:
            choice = questionary.select(
                "Add Soda (data quality)?",
                choices=[
                    # No first = default (was default=False)
                    questionary.Choice("No", value=False),
                    questionary.Choice("Yes", value=True),
                ],
                pointer="→",
                style=_select_style,
            ).ask()
            if choice is None:
                raise KeyboardInterrupt
            with_monitoring = choice
        else:
            with_monitoring = (
                click.confirm("Add Soda (data quality)?", default=False) if interactive else False
            )
    # Demo is always included unless --no-demo (no question)
    if demo is None:
        demo = True
    run_init(
        dest=project_name,
        with_demo=demo,
        with_monitoring=with_monitoring,
        backend=backend,
        use_minimal_stack=use_minimal_stack if use_docker else False,
        use_docker=use_docker,
    )


def main():
    """Entry point: run CLI; on Ctrl+C exit with code 130."""
    try:
        _cli()
    except KeyboardInterrupt:
        click.echo("\nAborted.", err=True)
        sys.exit(130)
