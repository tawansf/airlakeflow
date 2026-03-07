"""Postgres dialect: emits PostgreSQL DDL (SERIAL, VARCHAR(n), REFERENCES, etc.)."""

from __future__ import annotations

from airlakeflow.dialects.base import BaseDialect
from airlakeflow.dialects.registry import register_dialect
from airlakeflow.models.base import FieldDesc, Model


class PostgresDialect(BaseDialect):
    """PostgreSQL DDL. First supported driver; others (Oracle, SQL Server) can follow the same interface."""

    name = "postgres"

    def emit_type(self, field: FieldDesc) -> str:
        if field.ref:
            return "INTEGER"
        kind = field.kind
        if kind == "serial":
            return "SERIAL"
        if kind == "int":
            return "INTEGER"
        if kind == "bigint":
            return "BIGINT"
        if kind == "varchar":
            n = field.params[0] if field.params else 255
            return f"VARCHAR({n})"
        if kind == "char":
            n = field.params[0] if field.params else 1
            return f"CHAR({n})"
        if kind == "text":
            return "TEXT"
        if kind == "numeric":
            p, s = (field.params[0], field.params[1]) if len(field.params) >= 2 else (18, 8)
            return f"NUMERIC({p}, {s})"
        if kind == "float":
            return "DOUBLE PRECISION"
        if kind == "boolean":
            return "BOOLEAN"
        if kind == "date":
            return "DATE"
        if kind == "time":
            return "TIME"
        if kind == "timestamp":
            return "TIMESTAMP"
        if kind == "datetime":
            return "TIMESTAMP"
        if kind == "jsonb":
            return "JSONB"
        return "TEXT"

    def emit_create_table(self, model: type[Model]) -> str:
        schema = model.get_schema()
        table = model.get_table_name()
        full = f"{schema}.{table}"
        lines = [f"CREATE TABLE IF NOT EXISTS {full} ("]
        parts = []
        pks = []
        for name, field in model.get_fields():
            typ = self.emit_type(field)
            not_null = " NOT NULL" if not field.nullable else ""
            default = f" DEFAULT {field.default}" if field.default else ""
            ref_clause = ""
            ref_sql = self.emit_references(field)
            if ref_sql:
                ref_clause = f" {ref_sql}"
            pk_suffix = " PRIMARY KEY" if field.primary_key else ""
            if field.primary_key:
                pks.append(name)
            parts.append(f"    {name} {typ}{not_null}{default}{ref_clause}{pk_suffix}")
        if len(pks) > 1:
            parts.append(f"    PRIMARY KEY ({', '.join(pks)})")
        lines.append(",\n".join(parts))
        lines.append(");")

        # Indexes on common columns
        for name, _field in model.get_fields():
            if name in ("updated_at", "created_at", "date", "data_ingestao", "ingestion_date"):
                idx = f"idx_{schema}_{table}_{name}".replace(".", "_")
                lines.append(f"\nCREATE INDEX IF NOT EXISTS {idx} ON {full}({name});")
        return "\n".join(lines)


register_dialect("postgres", PostgresDialect)
