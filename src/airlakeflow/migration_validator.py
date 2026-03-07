"""Validate that migration files contain only table/view DDL (no schema, grants, etc.)."""

from __future__ import annotations

import re
from pathlib import Path


# Statements allowed in dags/sql/migrations/ (only model-related DDL)
_ALLOWED_PATTERNS = [
    re.compile(r"^\s*CREATE\s+TABLE\b", re.IGNORECASE | re.DOTALL),
    re.compile(r"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\b", re.IGNORECASE | re.DOTALL),
    re.compile(r"^\s*CREATE\s+(?:UNIQUE\s+)?INDEX\b", re.IGNORECASE | re.DOTALL),
    re.compile(r"^\s*DROP\s+TABLE\b", re.IGNORECASE | re.DOTALL),
    re.compile(r"^\s*DROP\s+VIEW\b", re.IGNORECASE | re.DOTALL),
]

# Forbidden: schemas, DB, grants, DML, functions, etc.
_FORBIDDEN_PATTERNS = [
    (re.compile(r"^\s*CREATE\s+SCHEMA\b", re.IGNORECASE | re.DOTALL), "CREATE SCHEMA"),
    (re.compile(r"^\s*CREATE\s+DATABASE\b", re.IGNORECASE | re.DOTALL), "CREATE DATABASE"),
    (re.compile(r"^\s*DROP\s+SCHEMA\b", re.IGNORECASE | re.DOTALL), "DROP SCHEMA"),
    (re.compile(r"^\s*GRANT\b", re.IGNORECASE | re.DOTALL), "GRANT"),
    (re.compile(r"^\s*REVOKE\b", re.IGNORECASE | re.DOTALL), "REVOKE"),
    (re.compile(r"^\s*INSERT\s+INTO\b", re.IGNORECASE | re.DOTALL), "INSERT"),
    (re.compile(r"^\s*UPDATE\s+\w", re.IGNORECASE | re.DOTALL), "UPDATE"),
    (re.compile(r"^\s*DELETE\s+FROM\b", re.IGNORECASE | re.DOTALL), "DELETE"),
    (re.compile(r"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\b", re.IGNORECASE | re.DOTALL), "CREATE FUNCTION"),
    (re.compile(r"^\s*CREATE\s+PROCEDURE\b", re.IGNORECASE | re.DOTALL), "CREATE PROCEDURE"),
    (re.compile(r"^\s*CREATE\s+TYPE\b", re.IGNORECASE | re.DOTALL), "CREATE TYPE"),
    (re.compile(r"^\s*ALTER\s+(?:DATABASE|SCHEMA|USER|ROLE)\b", re.IGNORECASE | re.DOTALL), "ALTER (database/schema/user)"),
]


def _normalize_and_split_statements(sql: str) -> list[str]:
    """Strip comments and split by semicolon; return non-empty statement chunks."""
    lines = []
    for line in sql.splitlines():
        line = line.strip()
        if not line or line.startswith("--"):
            continue
        # Remove inline comment (-- to EOL)
        if "--" in line:
            line = line.split("--", 1)[0].strip()
        lines.append(line)
    text = " ".join(lines)
    parts = [p.strip() for p in text.split(";") if p.strip()]
    return parts


def _statement_kind(stmt: str) -> str | None:
    """Return forbidden kind if statement matches a forbidden pattern; else None if allowed."""
    for pattern, kind in _FORBIDDEN_PATTERNS:
        if pattern.search(stmt):
            return kind
    for pattern in _ALLOWED_PATTERNS:
        if pattern.search(stmt):
            return None
    # Unknown statement type (e.g. COMMENT ON, ALTER TABLE) - treat as forbidden to be safe
    if re.match(r"^\s*(ALTER|COMMENT|TRUNCATE|CREATE\s+(?!TABLE|VIEW|INDEX)\w+)", stmt, re.IGNORECASE):
        return "outro comando (apenas CREATE TABLE, CREATE VIEW, CREATE INDEX são permitidos)"
    return None


def validate_migration_content(content: str, filename: str = "") -> list[str]:
    """Validate migration SQL. Returns list of error messages (empty if valid).
    Migrations must contain only CREATE TABLE, CREATE VIEW, CREATE INDEX, DROP TABLE, DROP VIEW.
    Schema and other commands belong in scripts/ (e.g. 002_create_schemas.sql).
    """
    forbidden_kinds: set[str] = set()
    statements = _normalize_and_split_statements(content)
    for stmt in statements:
        kind = _statement_kind(stmt)
        if kind:
            forbidden_kinds.add(kind)
    if not forbidden_kinds:
        return []
    prefix = f"{filename}: " if filename else ""
    return [
        f"{prefix}Comandos não permitidos em migrations: {', '.join(sorted(forbidden_kinds))}. "
        "Use scripts/ para comandos de camada (ex.: 002_create_schemas.sql)."
    ]


def validate_migration_file(path: Path) -> list[str]:
    """Validate a single migration file. Returns list of error messages."""
    try:
        content = path.read_text(encoding="utf-8")
    except Exception as e:
        return [f"{path.name}: erro ao ler arquivo ({e})"]
    return validate_migration_content(content, path.name)


def validate_migrations_dir(migrations_dir: Path) -> list[str]:
    """Validate all V*.sql files in migrations dir. Returns flat list of all error messages."""
    if not migrations_dir.exists():
        return []
    errors: list[str] = []
    for path in sorted(migrations_dir.glob("V*.sql")):
        errors.extend(validate_migration_file(path))
    return errors
