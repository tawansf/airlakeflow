"""SQL dialects for migration generation. Register new providers here to keep it scalable."""

from airlakeflow.dialects.base import BaseDialect
from airlakeflow.dialects.registry import get_dialect, list_dialects, register_dialect

# Built-in drivers
from airlakeflow.dialects import postgres  # noqa: F401 — registers 'postgres'

__all__ = ["BaseDialect", "get_dialect", "list_dialects", "register_dialect"]
