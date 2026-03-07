"""Abstract base for data architectures. Implement and register to support Medallion, Data Vault, etc."""

from __future__ import annotations

from abc import ABC
from typing import ClassVar


class BaseArchitecture(ABC):  # noqa: B024
    """Define layers (schemas) and ordering for an architecture (Medallion, Data Vault, Kimball, ...)."""

    name: ClassVar[str] = "base"
    """Identifier used in config (e.g. 'medallion', 'data_vault')."""

    layers: ClassVar[list[str]] = []
    """Ordered list of layer/schema names (e.g. ['bronze', 'silver', 'gold'])."""

    default_layer: ClassVar[str] = ""
    """Default layer for new models when not specified (e.g. 'silver')."""

    @classmethod
    def layer_order(cls) -> dict[str, int]:
        """Return a dict layer_name -> order index for sorting (lower = earlier)."""
        return {layer: i for i, layer in enumerate(cls.layers)}

    @classmethod
    def drop_schema_order(cls) -> list[str]:
        """Order for DROP SCHEMA on rollback (reverse of layers so dependencies go first)."""
        return list(reversed(cls.layers))
