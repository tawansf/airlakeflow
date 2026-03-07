"""Registry of data architectures. Add new ones (Data Vault, Kimball, etc.) by registering them."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airlakeflow.architectures.base import BaseArchitecture

_REGISTRY: dict[str, type[BaseArchitecture]] = {}


def register_architecture(name: str, architecture_class: type[BaseArchitecture]) -> None:
    """Register an architecture so it can be selected via config (e.g. 'medallion', 'data_vault')."""
    _REGISTRY[name.lower().strip()] = architecture_class


def get_architecture(name: str) -> type[BaseArchitecture]:
    """Return the architecture class for the given name. Raises KeyError if unknown."""
    key = name.lower().strip()
    if key not in _REGISTRY:
        available = ", ".join(sorted(_REGISTRY.keys())) or "(none registered)"
        raise KeyError(f"Unknown architecture {name!r}. Available: {available}")
    return _REGISTRY[key]


def list_architectures() -> list[str]:
    """Return registered architecture names."""
    return sorted(_REGISTRY.keys())
