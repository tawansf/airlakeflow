"""Base model and field definitions — dialect-agnostic."""

from __future__ import annotations

from typing import Any


class Ref:
    """Reference to another table.column (for FK). Resolved by dialect to REFERENCES clause."""

    __slots__ = ("schema", "table", "column")

    def __init__(self, schema: str, table: str, column: str = "id") -> None:
        self.schema = schema
        self.table = table
        self.column = column

    def __str__(self) -> str:
        return f"{self.schema}.{self.table}.{self.column}"


def ref(schema_table_column: str) -> Ref:
    """Build a Ref from a string like 'silver.vendas' or 'silver.vendas.id'."""
    parts = schema_table_column.split(".")
    if len(parts) == 2:
        return Ref(parts[0], parts[1], "id")
    if len(parts) == 3:
        return Ref(parts[0], parts[1], parts[2])
    raise ValueError(
        f"ref() expects 'schema.table' or 'schema.table.column', got {schema_table_column!r}"
    )


class FieldDesc:
    """Descriptor for a column: kind and options. Dialects use this to emit SQL types."""

    __slots__ = ("name", "kind", "params", "primary_key", "nullable", "default", "ref")

    def __init__(
        self,
        kind: str,
        *params: Any,
        primary_key: bool = False,
        nullable: bool = True,
        default: str | None = None,
        ref: Ref | None = None,
    ) -> None:
        self.name = ""
        self.kind = kind
        self.params = params
        self.primary_key = primary_key
        self.nullable = nullable
        self.default = default
        self.ref = ref

    def with_name(self, name: str) -> FieldDesc:
        self.name = name
        return self


class Field:
    """Agnostic column types. Dialect translates to native SQL (e.g. Postgres SERIAL, VARCHAR(n))."""

    @staticmethod
    def serial(*, primary_key: bool = True) -> FieldDesc:
        return FieldDesc("serial", primary_key=primary_key, nullable=False)

    @staticmethod
    def int(
        *, primary_key: bool = False, nullable: bool = True, ref: Ref | None = None
    ) -> FieldDesc:
        return FieldDesc("int", primary_key=primary_key, nullable=nullable, ref=ref)

    integer = int  # alias

    @staticmethod
    def bigint(*, primary_key: bool = False, nullable: bool = True) -> FieldDesc:
        return FieldDesc("bigint", primary_key=primary_key, nullable=nullable)

    @staticmethod
    def varchar(length: int = 255, *, nullable: bool = True) -> FieldDesc:
        return FieldDesc("varchar", length, nullable=nullable)

    @staticmethod
    def char(length: int = 1, *, nullable: bool = True) -> FieldDesc:
        return FieldDesc("char", length, nullable=nullable)

    @staticmethod
    def text(*, nullable: bool = True) -> FieldDesc:
        return FieldDesc("text", nullable=nullable)

    @staticmethod
    def numeric(precision: int = 18, scale: int = 8, *, nullable: bool = True) -> FieldDesc:
        return FieldDesc("numeric", precision, scale, nullable=nullable)

    @staticmethod
    def float(*, nullable: bool = True) -> FieldDesc:
        return FieldDesc("float", nullable=nullable)

    @staticmethod
    def boolean(*, nullable: bool = True, default: str | None = None) -> FieldDesc:
        return FieldDesc("boolean", nullable=nullable, default=default)

    @staticmethod
    def date(*, nullable: bool = True, default: str | None = None) -> FieldDesc:
        return FieldDesc("date", nullable=nullable, default=default)

    @staticmethod
    def timestamp(*, nullable: bool = True, default: str | None = None) -> FieldDesc:
        return FieldDesc("timestamp", nullable=nullable, default=default)

    @staticmethod
    def time(*, nullable: bool = True, default: str | None = None) -> FieldDesc:
        return FieldDesc("time", nullable=nullable, default=default)

    @staticmethod
    def datetime(*, nullable: bool = True, default: str | None = None) -> FieldDesc:
        return FieldDesc("datetime", nullable=nullable, default=default)

    @staticmethod
    def jsonb(*, nullable: bool = True) -> FieldDesc:
        return FieldDesc("jsonb", nullable=nullable)


class ModelMeta(type):
    """Metaclass that collects Field descriptors and layer from the class."""

    def __new__(mcs, name: str, bases: tuple, namespace: dict) -> type:
        cls = super().__new__(mcs, name, bases, namespace)
        fields: list[tuple[str, FieldDesc]] = []
        for k, v in namespace.items():
            if isinstance(v, FieldDesc):
                fields.append((k, v.with_name(k)))
        cls._alf_fields = fields
        cls._alf_layer = getattr(cls, "_alf_layer", None)
        cls._alf_partition_by = getattr(cls, "_alf_partition_by", None)
        cls._alf_table = getattr(cls, "__table__", None) or _to_snake(name)
        return cls


def _to_snake(name: str) -> str:
    import re

    s = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()
    return s.replace("-", "_")


class Model(metaclass=ModelMeta):
    """Base for table models. Set __table__ and use @layer('bronze'|'silver'|'gold')."""

    __table__: str = ""
    _alf_layer: str | None = None
    _alf_partition_by: str | None = None  # column name for PARTITION BY RANGE (e.g. data_registro)
    _alf_fields: list[tuple[str, FieldDesc]]
    _alf_table: str

    @classmethod
    def get_schema(cls) -> str:
        layer = cls._alf_layer
        if not layer:
            raise ValueError(
                f"Model {cls.__name__} has no layer; use @layer('bronze'|'silver'|'gold')"
            )
        return layer

    @classmethod
    def get_table_name(cls) -> str:
        return cls._alf_table

    @classmethod
    def get_fields(cls) -> list[tuple[str, FieldDesc]]:
        return list(cls._alf_fields)


def layer(schema: str, partition_by: str | None = None):
    """Decorator to assign a layer (bronze/silver/gold) and optional partition key to a model."""

    def deco(model_class: type) -> type:
        if not issubclass(model_class, Model):
            raise TypeError("@layer must be used on a Model subclass")
        model_class._alf_layer = schema
        if partition_by is not None:
            model_class._alf_partition_by = partition_by
        return model_class

    return deco
