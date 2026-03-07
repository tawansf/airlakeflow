"""Pluggable data architectures (Medallion, Data Vault, etc.)."""

# Register built-in architectures so they are available via config
from airlakeflow.architectures import medallion  # noqa: F401
from airlakeflow.architectures.base import BaseArchitecture
from airlakeflow.architectures.registry import (
    get_architecture,
    list_architectures,
    register_architecture,
)

__all__ = [
    "BaseArchitecture",
    "get_architecture",
    "list_architectures",
    "register_architecture",
]
