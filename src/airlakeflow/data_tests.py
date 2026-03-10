"""Execute ALF-Checks from config/checks/ (generic.yaml + layer folders with one YAML per table)."""

from __future__ import annotations

from pathlib import Path

import yaml

KNOWN_LAYERS = ("bronze", "silver", "gold")


def load_data_tests_config(project_root: Path) -> dict:
    """Load config/checks/: generic.yaml for connection_id, layer folders for tables (one YAML per table).
    Returns dict with connection_id and tables (each: schema, table, checks).
    """
    root = Path(project_root).resolve()
    checks_dir = root / "config" / "checks"
    connection_id = "postgres_datawarehouse"
    tables: list[dict] = []

    generic_path = checks_dir / "generic.yaml"
    if generic_path.exists():
        with open(generic_path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        connection_id = data.get("connection_id", connection_id)

    if not checks_dir.exists():
        return {"connection_id": connection_id, "tables": tables}

    for layer in KNOWN_LAYERS:
        layer_dir = checks_dir / layer
        if not layer_dir.is_dir():
            continue
        for yaml_path in sorted(layer_dir.glob("*.yaml")):
            table_name = yaml_path.stem
            if not table_name or yaml_path.name.startswith("."):
                continue
            with open(yaml_path, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            tables.append({
                "schema": layer,
                "table": table_name,
                "checks": data.get("checks", []),
            })

    return {"connection_id": connection_id, "tables": tables}


def run_data_tests(project_root: Path, conn_id: str | None = None) -> int:
    """Run all data tests defined in config/data_tests.yaml. Returns 0 on success, 1 on failure."""
    config = load_data_tests_config(project_root)
    connection_id = conn_id or config["connection_id"]
    tables = config["tables"]
    if not tables:
        return 0

    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
    except ImportError:
        raise RuntimeError("airflow.providers.postgres is required for data tests") from None

    hook = PostgresHook(postgres_conn_id=connection_id)
    conn = hook.get_conn()
    cursor = conn.cursor()
    failed = []

    try:
        for entry in tables:
            schema = entry.get("schema", "public")
            table = entry.get("table", "")
            if not table:
                continue
            full_name = f'"{schema}"."{table}"'
            for check in entry.get("checks", []):
                kind = check.get("type", "")
                columns = check.get("columns") or ([check["column"]] if check.get("column") else [])
                if kind == "not_null":
                    for col in columns:
                        sql = f'SELECT COUNT(*) FROM {full_name} WHERE "{col}" IS NULL'
                        cursor.execute(sql)
                        (n,) = cursor.fetchone()
                        if n and n > 0:
                            failed.append(f"{full_name}.{col}: not_null failed ({n} nulls)")
                elif kind == "row_count":
                    cursor.execute(f"SELECT COUNT(*) FROM {full_name}")
                    (n,) = cursor.fetchone()
                    if "min" in check and n < check["min"]:
                        failed.append(f"{full_name}: row_count min={check['min']} (got {n})")
                    if "max" in check and n > check["max"]:
                        failed.append(f"{full_name}: row_count max={check['max']} (got {n})")
                elif kind == "unique" and columns:
                    cols_str = ", ".join(f'"{c}"' for c in columns)
                    sql = f"SELECT COUNT(*) FROM (SELECT {cols_str} FROM {full_name} GROUP BY {cols_str} HAVING COUNT(*) > 1) t"
                    cursor.execute(sql)
                    (n,) = cursor.fetchone()
                    if n and n > 0:
                        failed.append(f"{full_name}: unique({columns}) failed ({n} duplicate groups)")
    finally:
        cursor.close()
        conn.close()

    if failed:
        for msg in failed:
            print(f"FAIL: {msg}")
        return 1
    return 0
