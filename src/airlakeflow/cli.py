from pathlib import Path
import sys

import click

from airlakeflow import __version__ as _pkg_version
from airlakeflow.add_soda import run_add_soda
from airlakeflow.config import (
    get_architecture_from_config,
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
from airlakeflow.doctor_cmd import run_doctor
from airlakeflow.init_cmd import run_init
from airlakeflow.migrations_cmd import (
    run_align as run_migrations_align,
    run_doctor as run_migrations_doctor,
    run_down as run_migrations_down,
    run_gen as run_migrations_gen,
    run_up as run_migrations_up,
)
from airlakeflow.new_etl import run_new_etl
from airlakeflow.new_migration import discover_dags, run_new_migration
from airlakeflow.new_model_cmd import run_new_model
from airlakeflow.style import SYM_LIST, print_banner, secho_fail, secho_warn
from airlakeflow.upgrade_cmd import run_upgrade
from airlakeflow.validate_cmd import run_validate

# Commands that operate the Docker/Compose stack (shown in a separate block in help)
DOCKER_COMMANDS = frozenset({"down", "exec", "logs", "ps", "restart", "run", "status", "stop"})


class AlfGroup(click.Group):
    """Group that lists Docker-related commands in a separate section in help."""

    def format_commands(self, ctx, formatter):
        commands = sorted(self.list_commands(ctx))
        # Hide aliases from help (e.g. "m"); show only full names like "migrations"
        HIDDEN_ALIASES = frozenset({"m"})
        rest = [c for c in commands if c not in DOCKER_COMMANDS and c not in HIDDEN_ALIASES]
        docker = [c for c in commands if c in DOCKER_COMMANDS]
        max_len = max((len(name) for name in commands), default=0)
        limit = formatter.width - 6 - max_len if formatter.width else 72

        def write_block(rows, section_title):
            if not rows:
                return
            with formatter.section(section_title):
                formatter.write_dl(rows)

        rest_rows = []
        for name in rest:
            cmd = self.get_command(ctx, name)
            if cmd is None or getattr(cmd, "hidden", False):
                continue
            help_str = (
                cmd.get_short_help_str(limit)
                if hasattr(cmd, "get_short_help_str")
                else (cmd.help or "")
            )
            rest_rows.append((name, help_str))
        write_block(rest_rows, "Commands")

        docker_rows = []
        for name in docker:
            cmd = self.get_command(ctx, name)
            if cmd is None or getattr(cmd, "hidden", False):
                continue
            help_str = (
                cmd.get_short_help_str(limit)
                if hasattr(cmd, "get_short_help_str")
                else (cmd.help or "")
            )
            docker_rows.append((name, help_str))
        write_block(docker_rows, "Docker (stack)")


def _get_version() -> str:
    try:
        from importlib.metadata import version

        return version("airlakeflow")
    except Exception:
        return _pkg_version


@click.group(cls=AlfGroup, invoke_without_command=True, add_help_option=False)
@click.pass_context
def main(ctx: click.Context):
    """AirLakeFlow — CLI for the framework (Bronze / Silver / Gold)."""
    if ctx.invoked_subcommand is None:
        print_banner(_get_version())
        click.echo(ctx.get_help())
        ctx.exit(0)


@main.command("help", help="Show this message and exit.")
@click.pass_context
def _show_help(ctx: click.Context):
    """Show main group help (alf help / alf h)."""
    click.echo(ctx.parent.get_help())
    ctx.exit(0)


@main.command("h", hidden=True)
@click.pass_context
def _show_help_h(ctx: click.Context):
    click.echo(ctx.parent.get_help())
    ctx.exit(0)


@main.command("version", help="Show version and exit.")
@click.pass_context
def _show_version(ctx: click.Context):
    click.echo(f"AirLakeFlow {_get_version()}")
    ctx.exit(0)


@main.command("v", hidden=True)
@click.pass_context
def _show_version_v(ctx: click.Context):
    click.echo(f"AirLakeFlow {_get_version()}")
    ctx.exit(0)


@main.group()
def new():
    """Create new resource (ETL, Migration, Contract, Layer)."""
    pass


@main.group("list")
def list_group():
    """List project resources (ETLs, etc.)."""
    pass


def _architecture_layers(project_root: str = ".") -> list[str]:
    """Return layer names from project architecture (for CLI choices). Falls back to medallion layers."""
    try:
        root = Path(resolve_project_root(project_root))
        cfg = load_config(root)
        return get_architecture_from_config(cfg).layers
    except Exception:
        return ["bronze", "silver", "gold"]


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
@click.option("-t", "table_name", default=None, help="Table name (default: equal to NAME)")
@click.option(
    "-c",
    "with_contracts",
    is_flag=True,
    default=False,
    help="Generate Soda contracts (Bronze + Silver)",
)
@click.option("-g", "with_gold", is_flag=True, default=True, help="Include Gold layer")
@click.option("-G", "no_gold", is_flag=True, default=False, help="Exclude Gold layer")
@click.option(
    "-s",
    "source",
    type=click.Choice(["api", "file", "jdbc"]),
    default="api",
    help="Bronze ingestion type",
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
    project_root: str,
):
    """Create a new ETL pipeline (Bronze -> Silver -> Gold) in the current project."""
    if no_gold:
        with_gold = False
    project_root = str(resolve_project_root(project_root))
    if use_spark_flag:
        use_spark = True
    elif no_spark:
        use_spark = False
    else:
        cfg = load_config(Path(project_root))
        use_spark = cfg.get("silver_backend", "pandas") == "pyspark"
    table = table_name or name
    run_new_etl(
        name=name,
        table_name=table,
        with_contracts=with_contracts,
        with_gold=with_gold,
        source=source,
        use_spark=use_spark,
        project_root=project_root,
    )


@new.command("migration")
@click.argument("name", type=str)
@click.option("-d", "dag", default=None, help="DAG name (directory in dags/). If omitted, list to choose.")
@click.option(
    "-l",
    "layer",
    type=click.Choice(_architecture_layers()),
    default=None,
    help="Layer (from project architecture). If omitted, ask.",
)
@click.option(
    "-r",
    "project_root",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    default=".",
    help="Project root (default: current directory)",
)
def new_migration(name: str, dag: str | None, layer: str | None, project_root: str):
    """Create a migration for an existing DAG. Choose the DAG and the layer (or use -d and -l)."""
    project_root = str(resolve_project_root(project_root))
    root = Path(project_root).resolve()
    dags = discover_dags(root)
    if not dags:
        secho_fail("No DAGs found in dags/. Create one with 'alf new etl NAME'.")
        raise SystemExit(1)
    if dag is None:
        dag = click.prompt("Choose the DAG", type=click.Choice(dags))
    if layer is None:
        layer = click.prompt("Layer", type=click.Choice(_architecture_layers(project_root or ".")))
    run_new_migration(
        name=name, dag=dag, layer=layer.lower() if layer else "bronze", project_root=project_root
    )


@new.command("contract")
@click.argument("schema", type=str)
@click.argument("table", type=str)
@click.option(
    "-l",
    "layer",
    type=click.Choice(_architecture_layers()),
    default=None,
    help="Layer for the contract",
)
@click.option(
    "-r",
    "project_root",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    default=".",
    help="Project root",
)
def new_contract(schema: str, table: str, layer: str | None, project_root: str):
    """Create a new Soda contract for an existing table in the given layer."""
    project_root = str(resolve_project_root(project_root))
    if layer is None:
        layer = _architecture_default_layer(project_root)
    from airlakeflow.new_contract_cmd import run_new_contract

    run_new_contract(schema=schema, table=table, layer=layer, project_root=Path(project_root))


@new.command("layer")
@click.argument("name", type=str)
@click.option(
    "-r",
    "project_root",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    default=".",
    help="Project root",
)
def new_layer(name: str, project_root: str):
    """Create a new layer (minimal DAG folder with pipeline and optional bronze/silver/gold stubs)."""
    project_root = str(resolve_project_root(project_root))
    from airlakeflow.new_layer_cmd import run_new_layer

    run_new_layer(name=name, project_root=Path(project_root))


@new.command("model")
@click.argument("name", type=str)
@click.option(
    "-l",
    "layer",
    type=click.Choice(_architecture_layers(), case_sensitive=False),
    default=_architecture_default_layer(),
    help="Layer (from project architecture). Default from architecture.",
)
@_project_root_option()
def new_model(name: str, layer: str, project_root: str):
    """Create a new model in config/models/ and generate its migration (dags/sql/migrations/)."""
    root = Path(resolve_project_root(project_root))
    run_new_model(name=name, layer_name=layer, project_root=root)


@main.group()
def migrations_group():
    """Generate, apply, list, and check migrations (models ↔ SQL)."""
    pass


main.add_command(migrations_group, "migrations")
main.add_command(migrations_group, "m")


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


@main.group()
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


@main.command("validate")
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


@main.command("doctor")
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


@main.command("status")
@_project_root_option()
def status(project_root: str):
    """Show stack status summary (how many services running)."""
    project_root = str(resolve_project_root(project_root))
    code = run_status(Path(project_root))
    raise SystemExit(code)


@main.command("exec")
@_project_root_option()
@click.argument("service", type=str)
@click.argument("command", nargs=-1, required=True)
def exec_cmd(project_root: str, service: str, command: tuple[str, ...]):
    """Run a command inside a service container (e.g. alf exec airflow-scheduler airflow dags list)."""
    project_root = str(resolve_project_root(project_root))
    code = run_exec(Path(project_root), service, list(command))
    raise SystemExit(code)


@main.command("upgrade")
@_project_root_option()
@click.option("-n", "dry_run", is_flag=True, help="Only show what would be updated")
@click.option("-B", "no_backup", is_flag=True, help="Do not backup files before overwriting")
def upgrade(project_root: str, dry_run: bool, no_backup: bool):
    """Update project files from the framework skeleton (optional backup in .airlakeflow_backup/)."""
    project_root = str(resolve_project_root(project_root))
    run_upgrade(Path(project_root), dry_run=dry_run, backup=not no_backup)


@main.command("run")
@_project_root_option()
@click.option("-b", "build", is_flag=True, help="Build images before starting")
@click.option("-f", "foreground", is_flag=True, help="Run in foreground (no -d)")
def run(project_root: str, build: bool, foreground: bool):
    """Start the application (docker compose up -d)."""
    project_root = str(resolve_project_root(project_root))
    code = run_up(Path(project_root), detach=not foreground, build=build)
    raise SystemExit(code)


@main.command("stop")
@_project_root_option()
def stop(project_root: str):
    """Stop the application (docker compose stop)."""
    project_root = str(resolve_project_root(project_root))
    code = run_stop(Path(project_root))
    raise SystemExit(code)


@main.command("restart")
@_project_root_option()
def restart(project_root: str):
    """Restart the application (stop then up -d)."""
    project_root = str(resolve_project_root(project_root))
    code = run_restart(Path(project_root))
    raise SystemExit(code)


@main.command("down")
@_project_root_option()
@click.option("-v", "volumes", is_flag=True, help="Remove named Docker volumes")
def down(project_root: str, volumes: bool):
    """Tear down the stack (Docker Compose down)."""
    project_root = str(resolve_project_root(project_root))
    code = run_down(Path(project_root), volumes=volumes)
    raise SystemExit(code)


@main.command("logs")
@_project_root_option()
@click.option("-f", "follow", is_flag=True, help="Follow Docker logs output")
@click.argument("service", required=False, default=None)
def logs(project_root: str, follow: bool, service: str | None):
    """Show container logs (Docker Compose logs)."""
    project_root = str(resolve_project_root(project_root))
    code = run_logs(Path(project_root), follow=follow, service=service)
    raise SystemExit(code)


@main.command("ps")
@_project_root_option()
def ps(project_root: str):
    """List running services (docker compose ps)."""
    project_root = str(resolve_project_root(project_root))
    code = run_ps(Path(project_root))
    raise SystemExit(code)


@main.command("init")
@click.argument("project_name", type=str, default=".", required=False)
@click.option(
    "-d",
    "demo",
    is_flag=True,
    default=None,
    flag_value=True,
    help="Include DAG demo (crypto). If omitted, init will ask interactively.",
)
@click.option(
    "-D",
    "no_demo",
    is_flag=True,
    default=None,
    help="Exclude DAG demo. If omitted, init will ask interactively.",
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
    "-s",
    "use_minimal_stack",
    type=click.Choice(["minimal", "full"], case_sensitive=False),
    default=None,
    help="Stack: minimal (4 containers) or full (7). If omitted, init will ask.",
)
def init(
    project_name: str,
    demo: bool | None,
    no_demo: bool | None,
    with_monitoring: bool | None,
    no_monitoring: bool | None,
    backend: str | None,
    use_minimal_stack: str | None,
):
    """Create a new project. Run without flags to choose options interactively."""
    if no_demo:
        demo = False
    if no_monitoring:
        with_monitoring = False
    if use_minimal_stack is not None:
        use_minimal_stack = use_minimal_stack == "minimal"
    interactive = sys.stdin.isatty()
    try:
        import questionary

        _has_select = True
    except ImportError:
        _has_select = False

    if demo is None:
        if interactive and _has_select:
            choice = questionary.select(
                "Incluir DAGs de demonstração (crypto)?",
                choices=[
                    questionary.Choice("Yes", value=True),
                    questionary.Choice("No", value=False),
                ],
                default=True,
                pointer="→",
            ).ask()
            demo = choice if choice is not None else True
        else:
            demo = (
                click.confirm("Incluir DAGs de demonstração (crypto)?", default=True)
                if interactive
                else True
            )
    if with_monitoring is None:
        if interactive and _has_select:
            choice = questionary.select(
                "Adicionar Soda (qualidade de dados)?",
                choices=[
                    questionary.Choice("Yes", value=True),
                    questionary.Choice("No", value=False),
                ],
                default=False,
                pointer="→",
            ).ask()
            with_monitoring = choice if choice is not None else False
        else:
            with_monitoring = (
                click.confirm("Adicionar Soda (qualidade de dados)?", default=False)
                if interactive
                else False
            )
    if backend is None:
        if interactive and _has_select:
            choice = questionary.select(
                "Backend da camada Silver",
                choices=[
                    questionary.Choice("pandas (lightweight, single machine)", value="pandas"),
                    questionary.Choice("pyspark (distributed)", value="pyspark"),
                ],
                default="pandas",
                pointer="→",
            ).ask()
            backend = choice if choice else "pandas"
        elif interactive:
            backend = click.prompt(
                "Backend da camada Silver",
                type=click.Choice(["pandas", "pyspark"], case_sensitive=False),
                default="pandas",
                show_choices=True,
            )
        else:
            backend = "pandas"
    if use_minimal_stack is None:
        if interactive and _has_select:
            choice = questionary.select(
                "Versão do projeto",
                choices=[
                    questionary.Choice("minimal (4 containers, LocalExecutor)", value="minima"),
                    questionary.Choice("full (7 containers, Celery)", value="completa"),
                ],
                default="minima",
                pointer="→",
            ).ask()
            use_minimal_stack = (choice or "minima") == "minima"
        elif interactive:
            stack_choice = click.prompt(
                "Versão do projeto: mínima (4 containers, LocalExecutor) ou completa (7 containers, Celery)?",
                type=click.Choice(["minima", "completa"], case_sensitive=False),
                default="minima",
                show_choices=True,
                show_default=True,
            )
            use_minimal_stack = stack_choice == "minima"
        else:
            use_minimal_stack = True
    run_init(
        dest=project_name,
        with_demo=demo,
        with_monitoring=with_monitoring,
        backend=backend,
        use_minimal_stack=use_minimal_stack,
    )
