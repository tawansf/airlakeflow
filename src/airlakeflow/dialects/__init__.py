"""SQL dialects for migration generation. Register new providers here to keep it scalable."""

# Built-in drivers
from airlakeflow.dialects import postgres  # noqa: F401
from airlakeflow.dialects.base import BaseDialect
from airlakeflow.dialects.registry import get_dialect, list_dialects, register_dialect

__all__ = ["BaseDialect", "get_dialect", "list_dialects", "register_dialect"]
