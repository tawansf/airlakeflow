"""Run migrations up/down and track applied state in the data warehouse."""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
    from psycopg2.extensions import connection as PgConnection  # noqa: F401

CONTROL_SCHEMA = "alf"
CONTROL_TABLE = "schema_migrations"


def _parse_uri(uri: str) -> dict:
    """Parse postgresql:// or postgres:// URI into kwargs for psycopg2.connect."""
    if not uri:
        return {}
    parsed = urlparse(uri)
    if parsed.scheme in ("postgresql", "postgres"):
        return {
            "host": parsed.hostname or "localhost",
            "port": parsed.port or 5432,
            "user": parsed.username,
            "password": parsed.password,
            "dbname": (parsed.path or "/").lstrip("/") or "postgres",
        }
    return {}


def _connect(uri: str):
    import psycopg2

    kwargs = _parse_uri(uri)
    if not kwargs:
        raise ValueError("Invalid or empty connection URI")
    return psycopg2.connect(**kwargs)


def _ensure_control_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {CONTROL_SCHEMA}")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.{CONTROL_TABLE} (
                version VARCHAR(255) PRIMARY KEY,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
    conn.commit()


def _applied_versions(conn) -> set[str]:
    with conn.cursor() as cur:
        try:
            # CONTROL_* are constants; no user input in this query
            cur.execute(
                f"SELECT version FROM {CONTROL_SCHEMA}.{CONTROL_TABLE} ORDER BY version"  # nosec B608
            )
            return {row[0] for row in cur.fetchall()}
        except Exception:
            return set()


def _discover_files(migrations_dir: Path) -> list[Path]:
    files = sorted(migrations_dir.glob("V*.sql"), key=lambda p: p.name)
    return files


def apply_pending(migrations_dir: Path, uri: str) -> list[str]:
    """Apply all pending migration files. Fails if any migration contains forbidden statements.
    Returns list of applied filenames. Migrations may only contain CREATE TABLE, CREATE VIEW, CREATE INDEX, DROP TABLE, DROP VIEW.
    """
    from airlakeflow.migration_validator import validate_migrations_dir

    validation_errors = validate_migrations_dir(migrations_dir)
    if validation_errors:
        raise ValueError(
            "Invalid migrations (only tables/views allowed). Use scripts/ for layer commands.\n  "
            + "\n  ".join(validation_errors)
        )

    conn = _connect(uri)
    try:
        _ensure_control_table(conn)
        applied = _applied_versions(conn)
        files = _discover_files(migrations_dir)
        newly_applied: list[str] = []
        with conn.cursor() as cur:
            for path in files:
                if path.name in applied:
                    continue
                sql = path.read_text(encoding="utf-8")
                cur.execute(sql)
                # CONTROL_* are constants; path.name is parameterized
                cur.execute(
                    f"INSERT INTO {CONTROL_SCHEMA}.{CONTROL_TABLE} (version) VALUES (%s)",  # nosec
                    (path.name,),
                )
                newly_applied.append(path.name)
        conn.commit()
        return newly_applied
    finally:
        conn.close()


def list_migrations_with_status(migrations_dir: Path, uri: str | None) -> list[tuple[str, str]]:
    """Return list of (filename, 'applied'|'pending'). If uri is None, all are 'pending'."""
    files = _discover_files(migrations_dir)
    if not uri:
        return [(p.name, "pending") for p in files]
    try:
        conn = _connect(uri)
        try:
            _ensure_control_table(conn)
            applied = _applied_versions(conn)
            return [(p.name, "applied" if p.name in applied else "pending") for p in files]
        finally:
            conn.close()
    except Exception:
        return [(p.name, "pending") for p in files]


def _infer_drop_sql(filename: str, migrations_dir: Path) -> list[str]:
    """Infer DROP statements from migration filename (e.g. V006__setup_silver_vendas.sql)."""
    base = filename.replace(".sql", "")
    m = re.match(r"V\d+__(.+)", base)
    if not m:
        return []
    desc = m.group(1).lower().replace("-", "_")
    if desc == "setup_schemas":
        from airlakeflow.config import get_architecture_from_config, load_config

        project_root = migrations_dir.parent.parent.parent
        cfg = load_config(project_root)
        arch = get_architecture_from_config(cfg)
        return [f"DROP SCHEMA IF EXISTS {s} CASCADE" for s in arch.drop_schema_order()]
    if desc.startswith("setup_") and "_" in desc:
        parts = desc.replace("setup_", "").split("_", 1)
        if len(parts) == 2:
            schema, table = parts
            return [f"DROP TABLE IF EXISTS {schema}.{table} CASCADE"]
    return []


def rollback_last(
    migrations_dir: Path,
    uri: str,
    *,
    dry_run: bool = False,
    force: bool = False,
) -> int:
    """Rollback the last applied migration(s). Asks for confirmation unless force. Returns 0 on success, 1 on abort."""
    from airlakeflow.style import secho_fail, secho_info, secho_ok, secho_warn

    conn = _connect(uri)
    try:
        _ensure_control_table(conn)
        applied = _applied_versions(conn)
    finally:
        conn.close()

    files = _discover_files(migrations_dir)
    applied_list = [f.name for f in files if f.name in applied]
    if not applied_list:
        secho_info("No applied migrations to roll back.")
        return 0

    last_file = applied_list[-1]
    drop_sqls = _infer_drop_sql(last_file, migrations_dir)
    if not drop_sqls:
        secho_warn(
            f"Cannot infer DROP for {last_file}; only setup_<schema>_<table> migrations are rolled back."
        )
        return 0

    if dry_run:
        secho_info(f"Would rollback: {last_file}")
        for s in drop_sqls:
            secho_info(f"  {s}")
        return 0

    if not force:
        try:
            import questionary

            choice = questionary.select(
                "Are you sure you want to perform the drop? This may delete tables/data.",
                choices=[
                    questionary.Choice("No (cancel)", value="no"),
                    questionary.Choice("Yes, perform drop", value="yes"),
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

    conn = _connect(uri)
    try:
        with conn.cursor() as cur:
            for sql in drop_sqls:
                cur.execute(sql)
            # CONTROL_* are constants; last_file is parameterized
            cur.execute(
                f"DELETE FROM {CONTROL_SCHEMA}.{CONTROL_TABLE} WHERE version = %s",  # nosec
                (last_file,),
            )
        conn.commit()
        secho_ok(f"Rolled back: {last_file}")
        return 0
    finally:
        conn.close()
