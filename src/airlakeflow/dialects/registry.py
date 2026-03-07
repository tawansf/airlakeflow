"""Registry of SQL dialects. Add new providers (Oracle, SQL Server, etc.) by registering them."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airlakeflow.dialects.base import BaseDialect

_REGISTRY: dict[str, type[BaseDialect]] = {}


def register_dialect(name: str, dialect_class: type[BaseDialect]) -> None:
    """Register a dialect so it can be used by migration_driver (e.g. 'postgres', 'oracle')."""
    _REGISTRY[name.lower().strip()] = dialect_class


def get_dialect(name: str) -> BaseDialect:
    """Return an instance of the dialect for the given driver name. Raises KeyError if unknown."""
    key = name.lower().strip()
    if key not in _REGISTRY:
        available = ", ".join(sorted(_REGISTRY.keys())) or "(none registered)"
        raise KeyError(f"Unknown migration_driver {name!r}. Available: {available}")
    return _REGISTRY[key]()


def list_dialects() -> list[str]:
    """Return registered dialect names."""
    return sorted(_REGISTRY.keys())
