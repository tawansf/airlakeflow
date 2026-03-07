"""Tests for pluggable architectures (Medallion, registry)."""

import pytest

from airlakeflow.architectures import (
    get_architecture,
    list_architectures,
)
from airlakeflow.architectures.base import BaseArchitecture
from airlakeflow.architectures.registry import register_architecture


def test_list_architectures_includes_medallion():
    names = list_architectures()
    assert "medallion" in names


def test_get_architecture_medallion():
    arch = get_architecture("medallion")
    assert arch.name == "medallion"
    assert arch.layers == ["bronze", "silver", "gold"]
    assert arch.default_layer == "silver"


def test_get_architecture_case_insensitive():
    arch = get_architecture("MEDALLION")
    assert arch.name == "medallion"


def test_get_architecture_unknown_raises():
    with pytest.raises(KeyError) as exc_info:
        get_architecture("nonexistent")
    assert "Unknown architecture" in str(exc_info.value)
    assert "nonexistent" in str(exc_info.value)


def test_medallion_layer_order():
    arch = get_architecture("medallion")
    order = arch.layer_order()
    assert order == {"bronze": 0, "silver": 1, "gold": 2}


def test_medallion_drop_schema_order():
    arch = get_architecture("medallion")
    drop_order = arch.drop_schema_order()
    assert drop_order == ["gold", "silver", "bronze"]


def test_register_custom_architecture():
    """Registering a custom architecture makes it available via get/list."""

    class DummyArchitecture(BaseArchitecture):
        name = "dummy"
        layers = ["staging", "mart"]
        default_layer = "staging"

    try:
        register_architecture("dummy", DummyArchitecture)
        assert "dummy" in list_architectures()
        arch = get_architecture("dummy")
        assert arch.layers == ["staging", "mart"]
        assert arch.default_layer == "staging"
        assert arch.layer_order() == {"staging": 0, "mart": 1}
        assert arch.drop_schema_order() == ["mart", "staging"]
    finally:
        # Cleanup: remove from registry (registry doesn't support unregister, so we'd need to
        # import and mutate _REGISTRY or skip cleanup and accept one extra in list)
        import airlakeflow.architectures.registry as reg

        reg._REGISTRY.pop("dummy", None)
