"""Model layer: table definitions with fields and relationships (agnostic of SQL dialect).

Models are the source of truth for schema; migrations can be generated from them
via a dialect (e.g. Postgres). Add new drivers by implementing the dialect interface.
"""

from airlakeflow.models.base import Model, Field, layer, ref

__all__ = ["Model", "Field", "layer", "ref"]
