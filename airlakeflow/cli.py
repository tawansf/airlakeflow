import click
from pathlib import Path

from airlakeflow.new_etl import run_new_etl
from airlakeflow.init_cmd import run_init
from airlakeflow.new_migration import run_new_migration, discover_dags
from airlakeflow.add_soda import run_add_soda
from airlakeflow.validate_cmd import run_validate
from airlakeflow.docker_cmd import (
    run_up,
    run_stop,
    run_restart,
    run_down,
    run_logs,
    run_ps,
)


@click.group()
@click.version_option(version="0.1.0", prog_name="AirLakeFlow")
def main():
    """AirLakeFlow — CLI for the framework (bronze / silver / gold + Soda)."""
    pass


@main.group()
def new():
    """Create new resource (ETL, contract, layer)."""
    pass


@new.command("etl")
@click.argument("name", type=str)
@click.option("--table-name", default=None, help="Nome da tabela (default: igual a NAME)")
@click.option("--contracts/--no-contracts", "with_contracts", default=False, help="Gerar contratos Soda (bronze + silver)")
@click.option("--gold/--no-gold", "with_gold", default=True, help="Incluir camada gold")
@click.option("--source", type=click.Choice(["api", "file", "jdbc"]), default="api", help="Tipo de ingestão bronze")
@click.option("--no-spark", is_flag=True, help="Silver sem Spark (apenas Python/SQL)")
@click.option("--project-root", type=click.Path(exists=True, file_okay=False, dir_okay=True), default=".", help="Raiz do projeto (default: diretório atual)")
def new_etl(name: str, table_name: str | None, with_contracts: bool, with_gold: bool, source: str, no_spark: bool, project_root: str):
    """Create a new ETL pipeline (bronze -> silver -> gold) in the current project."""
    table = table_name or name
    run_new_etl(
        name=name,
        table_name=table,
        with_contracts=with_contracts,
        with_gold=with_gold,
        source=source,
        use_spark=not no_spark,
        project_root=project_root,
    )


@new.command("migration")
@click.argument("name", type=str)
@click.option("--dag", default=None, help="Nome do DAG (pasta em dags/). Se omitido, lista para escolher.")
@click.option("--layer", type=click.Choice(["bronze", "silver", "gold"]), default=None, help="Camada (bronze, silver, gold). Se omitido, pergunta.")
@click.option("--project-root", type=click.Path(exists=True, file_okay=False, dir_okay=True), default=".", help="Raiz do projeto")
def new_migration(name: str, dag: str | None, layer: str | None, project_root: str):
    """Create a migration for an existing DAG. Choose the DAG and the layer (or use --dag and --layer)."""
    root = Path(project_root).resolve()
    dags = discover_dags(root)
    if not dags:
        click.echo("No DAGs found in dags/. Create one with 'alf new etl NAME'.")
        raise SystemExit(1)
    if dag is None:
        dag = click.prompt("Choose the DAG", type=click.Choice(dags))
    if layer is None:
        layer = click.prompt("Layer", type=click.Choice(["bronze", "silver", "gold"]))
    run_new_migration(name=name, dag=dag, layer=layer, project_root=project_root)


@new.command("contract")
@click.argument("schema", type=str)
@click.argument("table", type=str)
@click.option("--layer", type=click.Choice(["bronze", "silver"]), default="silver", help="Layer for the contract")
@click.option("--project-root", type=click.Path(exists=True, file_okay=False, dir_okay=True), default=".", help="Raiz do projeto")
def new_contract(schema: str, table: str, layer: str, project_root: str):
    """Create a new Soda contract for an existing table."""
    click.echo("new contract: in development. Use 'alf new etl NAME --contracts' to generate contracts along with the ETL.")


@new.command("layer")
@click.argument("name", type=str)
@click.option("--project-root", type=click.Path(exists=True, file_okay=False, dir_okay=True), default=".", help="Raiz do projeto")
def new_layer(name: str, project_root: str):
    """Create a new layer or resource."""
    click.echo("new layer: in development.")


@main.group()
def add():
    """Add quality integration (Soda, Great Expectations, etc.)."""
    pass


@add.command("soda")
@click.option("--etl", "etl_name", default=None, help="Aplicar só neste ETL (nome da pasta em dags/)")
@click.option("--all", "all_etls", is_flag=True, help="Aplicar em todos os ETLs (Projeto completo)")
@click.option("--project-root", type=click.Path(exists=True, file_okay=False, dir_okay=True), default=".", help="Raiz do projeto")
def add_soda(etl_name: str | None, all_etls: bool, project_root: str):
    """Integrate Soda into the project: config, contracts and scan tasks in the pipelines."""
    root = Path(project_root).resolve()
    dags = discover_dags(root)
    if not dags:
        click.echo("No ETLs found in dags/. Create one with 'alf new etl NAME'.")
        raise SystemExit(1)
    run_add_soda(project_root=root, etl_name=etl_name, all_etls=all_etls)


@add.command("greatxp")
@click.option("--etl", default=None, help="Specific ETL (in development)")
@click.option("--all", "all_etls", is_flag=True, help="All ETLs (in development)")
@click.option("--project-root", type=click.Path(exists=True, file_okay=False, dir_okay=True), default=".", help="Raiz do projeto")
def add_greatxp(etl: str | None, all_etls: bool, project_root: str):
    """Integrate Great Expectations (in development)."""
    click.echo("alf add greatxp: in development.")


@main.command("validate")
@click.option("--project-root", type=click.Path(exists=True, file_okay=False, dir_okay=True), default=".", help="Project root (default: current directory)")
@click.option("--no-docker", is_flag=True, help="Skip Docker and stack checks (only validate structure)")
@click.option("--no-stack", is_flag=True, help="Skip check that docker compose stack is up")
@click.option("-q", "quiet", is_flag=True, help="Only exit with code 0/1, minimal output")
def validate(project_root: str, no_docker: bool, no_stack: bool, quiet: bool):
    """Check project structure (dags/, soda/, docker-compose) and Docker (daemon, compose, stack up)."""
    ok = run_validate(
        project_root=project_root,
        check_docker=not no_docker,
        check_structure=True,
        check_stack_up=not no_stack,
        verbose=not quiet,
    )
    if not ok:
        raise SystemExit(1)


def _project_root_option(**kwargs):
    return click.option(
        "--project-root",
        type=click.Path(exists=True, file_okay=False, dir_okay=True),
        default=".",
        help="Project root (default: current directory)",
        **kwargs,
    )


@main.command("run")
@_project_root_option()
@click.option("--build", is_flag=True, help="Build images before starting")
@click.option("--foreground", "-f", is_flag=True, help="Run in foreground (no -d)")
def run(project_root: str, build: bool, foreground: bool):
    """Start the application (docker compose up -d)."""
    code = run_up(Path(project_root), detach=not foreground, build=build)
    raise SystemExit(code)


@main.command("stop")
@_project_root_option()
def stop(project_root: str):
    """Stop the application (docker compose stop)."""
    code = run_stop(Path(project_root))
    raise SystemExit(code)


@main.command("restart")
@_project_root_option()
def restart(project_root: str):
    """Restart the application (stop then up -d)."""
    code = run_restart(Path(project_root))
    raise SystemExit(code)


@main.command("down")
@_project_root_option()
@click.option("--volumes", "-v", is_flag=True, help="Remove named volumes")
def down(project_root: str, volumes: bool):
    """Tear down the stack (docker compose down)."""
    code = run_down(Path(project_root), volumes=volumes)
    raise SystemExit(code)


@main.command("logs")
@_project_root_option()
@click.option("--follow", "-f", is_flag=True, help="Follow log output")
@click.argument("service", required=False, default=None)
def logs(project_root: str, follow: bool, service: str | None):
    """Show container logs (docker compose logs)."""
    code = run_logs(Path(project_root), follow=follow, service=service)
    raise SystemExit(code)


@main.command("ps")
@_project_root_option()
def ps(project_root: str):
    """List running services (docker compose ps)."""
    code = run_ps(Path(project_root))
    raise SystemExit(code)


@main.command("init")
@click.argument("project_name", type=str, default=".", required=False)
@click.option("--demo/--no-demo", default=True, help="Include DAG demo (crypto)")
@click.option("--with-monitoring/--no-monitoring", default=True, help="Include schema monitoring and Soda report")
def init(project_name: str, demo: bool, with_monitoring: bool):
    """Create a new project: pass a name to create a folder in the current directory with the framework structure (dags, soda, docker-compose, etc.). Use '.' to init in the current directory."""
    run_init(dest=project_name, with_demo=demo, with_monitoring=with_monitoring)
