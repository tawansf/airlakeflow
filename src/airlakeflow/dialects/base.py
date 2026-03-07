"""Abstract base for SQL dialects. Implement this to add Oracle, SQL Server, etc."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airlakeflow.models.base import FieldDesc, Model


class BaseDialect(ABC):
    """Interface for emitting DDL. Subclass and register to add a new provider (Postgres, Oracle, SQL Server, ...)."""

    name: str = "base"

    @abstractmethod
    def emit_type(self, field: FieldDesc) -> str:
        """Return the SQL type for this field (e.g. SERIAL, VARCHAR(255), NUMERIC(18,8))."""
        ...

    @abstractmethod
    def emit_create_table(self, model: type[Model]) -> str:
        """Return full CREATE TABLE statement (and optionally CREATE INDEX) for the model."""
        ...

    def emit_create_schema(self, schema: str) -> str:
        """Return CREATE SCHEMA IF NOT EXISTS schema. Override if the engine uses different syntax."""
        return f"CREATE SCHEMA IF NOT EXISTS {schema};"

    def emit_references(self, field: FieldDesc) -> str | None:
        """Return REFERENCES clause if field has ref; otherwise None. Override for dialect-specific FK syntax."""
        if not field.ref:
            return None
        return f"REFERENCES {field.ref.schema}.{field.ref.table}({field.ref.column})"
