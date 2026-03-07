"""Medallion architecture: Bronze → Silver → Gold."""

from __future__ import annotations

from airlakeflow.architectures.base import BaseArchitecture
from airlakeflow.architectures.registry import register_architecture


class MedallionArchitecture(BaseArchitecture):
    """Bronze (raw), Silver (cleaned/conformed), Gold (business aggregates)."""

    name = "medallion"
    layers = ["bronze", "silver", "gold"]
    default_layer = "silver"


register_architecture(MedallionArchitecture.name, MedallionArchitecture)
