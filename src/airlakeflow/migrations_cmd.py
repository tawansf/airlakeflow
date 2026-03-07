"""Commands for alf migrations (gen, up, down, list, doctor)."""

from __future__ import annotations

from pathlib import Path

from airlakeflow.config import get_migration_driver, load_config
from airlakeflow.migration_gen import generate_migrations
from airlakeflow.style import secho_fail, secho_info, secho_ok, secho_warn


def run_gen(project_root: Path, driver: str | None) -> int:
    """Generate migration SQL files from models. Returns 0 on success, 1 on error."""
    root = Path(project_root).resolve()
    cfg = load_config(root)
    driver_name = (driver or get_migration_driver(cfg)).strip().lower()
    try:
        created = generate_migrations(root, driver=driver_name)
    except KeyError as e:
        secho_fail(str(e))
        return 1
    if not created:
        secho_warn("No models found in config/models/. Create one with 'alf new model NAME'.")
        return 0
    secho_ok(f"Generated {len(created)} migration(s) with driver '{driver_name}'")
    for p in created:
        rel = p.relative_to(root) if root in p.parents else p
        secho_info(f"  {rel}")
    return 0


def run_list(project_root: Path, connection_uri: str | None) -> int:
    """List migration files and their status (applied/pending). Returns 0 on success."""
    from airlakeflow.migration_runner import list_migrations_with_status

    root = Path(project_root).resolve()
    migrations_dir = root / "dags" / "sql" / "migrations"
    if not migrations_dir.exists():
        secho_warn("No dags/sql/migrations/ directory.")
        return 0
    uri = connection_uri or _get_connection_uri(root)
    rows = list_migrations_with_status(migrations_dir, uri)
    if not rows:
        secho_info("No migration files (V*.sql) found.")
        return 0
    for filename, status in rows:
        if status == "applied":
            secho_ok(f"  {filename}  applied")
        else:
            secho_info(f"  {filename}  pending")
    return 0


def run_up(project_root: Path, connection_uri: str | None) -> int:
    """Apply pending migrations. Returns 0 on success, 1 on error."""
    from airlakeflow.migration_runner import apply_pending

    root = Path(project_root).resolve()
    uri = connection_uri or _get_connection_uri(root)
    if not uri:
        secho_fail(
            "No connection URI. Set AIRFLOW_CONN_POSTGRES_DATAWAREHOUSE or migration_connection_uri in .airlakeflow.yaml"
        )
        return 1
    migrations_dir = root / "dags" / "sql" / "migrations"
    if not migrations_dir.exists():
        secho_fail("No dags/sql/migrations/ directory.")
        return 1
    try:
        applied = apply_pending(migrations_dir, uri)
        if applied:
            secho_ok(f"Applied {len(applied)} migration(s).")
            for f in applied:
                secho_info(f"  {f}")
        else:
            secho_info("No pending migrations.")
        return 0
    except Exception as e:
        secho_fail(str(e))
        return 1


def run_down(
    project_root: Path,
    connection_uri: str | None,
    *,
    dry_run: bool = False,
    force: bool = False,
) -> int:
    """Rollback last applied migrations (DROP) with confirmation. Returns 0 on success, 1 on error or abort."""
    from airlakeflow.migration_runner import rollback_last

    root = Path(project_root).resolve()
    uri = connection_uri or _get_connection_uri(root)
    if not uri:
        secho_fail(
            "No connection URI. Set AIRFLOW_CONN_POSTGRES_DATAWAREHOUSE or migration_connection_uri in .airlakeflow.yaml"
        )
        return 1
    migrations_dir = root / "dags" / "sql" / "migrations"
    if not migrations_dir.exists():
        secho_fail("No dags/sql/migrations/ directory.")
        return 1
    try:
        return rollback_last(migrations_dir, uri, dry_run=dry_run, force=force)
    except Exception as e:
        secho_fail(str(e))
        return 1


def run_doctor(project_root: Path, driver: str | None) -> int:
    """Compare models with migrations and report drift. Returns 0 if aligned, 1 if drift."""
    from airlakeflow.migration_doctor import doctor_models_vs_migrations

    root = Path(project_root).resolve()
    cfg = load_config(root)
    driver_name = (driver or get_migration_driver(cfg)).strip().lower()
    issues = doctor_models_vs_migrations(root, driver_name)
    if not issues:
        secho_ok("Models and migrations are aligned.")
        return 0
    secho_fail("Drift detected between models and migrations:")
    for msg in issues:
        secho_info(f"  • {msg}")
    return 1


def run_align(project_root: Path, driver: str | None, force: bool = False) -> int:
    """Overwrite migration files with DDL from models (model as reference).
    When there are differences, asks for confirmation unless -F. Returns 0 on success, 1 on error.
    """
    from airlakeflow.migration_doctor import align_migrations_to_models, doctor_models_vs_migrations

    root = Path(project_root).resolve()
    cfg = load_config(root)
    driver_name = (driver or get_migration_driver(cfg)).strip().lower()
    issues = doctor_models_vs_migrations(root, driver_name)

    if not issues:
        secho_ok("Models and migrations are aligned. Nothing to do.")
        return 0

    secho_warn("There are differences between model(s) and migration(s):")
    for msg in issues:
        secho_info(f"  • {msg}")

    if not force:
        try:
            import questionary

            choice = questionary.select(
                "Do you want to align anyway? Migrations will be overwritten with model DDL (model is the reference).",
                choices=[
                    questionary.Choice("No (cancel)", value="no"),
                    questionary.Choice("Yes, align migrations to models", value="yes"),
                ],
                default="no",
                pointer="→",
            ).ask()
            if choice != "yes":
                secho_info("Cancelled.")
                return 0
        except ImportError:
            secho_fail("Install 'questionary' for interactive confirmation or use -F.")
            return 1

    try:
        updated = align_migrations_to_models(root, driver_name)
    except KeyError as e:
        secho_fail(str(e))
        return 1
    if not updated:
        secho_warn("No migration was updated (no file matches the models).")
        return 0
    secho_ok(f"Aligned {len(updated)} migration(s) to models.")
    for p in updated:
        rel = p.relative_to(root) if root in p.parents else p
        secho_info(f"  {rel}")
    return 0


def _get_connection_uri(project_root: Path) -> str | None:
    """Get Postgres connection URI from env or config."""
    import os

    uri = os.environ.get("AIRFLOW_CONN_POSTGRES_DATAWAREHOUSE")
    if uri:
        return uri
    cfg = load_config(project_root)
    return cfg.get("migration_connection_uri") or cfg.get("connection_uri")
