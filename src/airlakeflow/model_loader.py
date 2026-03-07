"""Load all Model subclasses from a project's config/models/ (or config/models.py)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from airlakeflow.models.base import Model


def discover_models(project_root: Path) -> list[type[Model]]:
    """Discover all Model subclasses defined under config/models/ or config/models.py.
    Returns list of model classes (with _alf_layer set).
    """
    root = Path(project_root).resolve()
    models_dir = root / "config" / "models"
    models_file = root / "config" / "models.py"
    out: list[type[Model]] = []

    if models_dir.is_dir():
        if (models_dir / "__init__.py").exists():
            _load_package(models_dir, root, out)
        else:
            for py in sorted(models_dir.glob("*.py")):
                if py.name.startswith("_") or py.name == "__init__.py":
                    continue
                _load_module(py, root, out)
    elif models_file.is_file():
        _load_module(models_file, root, out)

    return out


def _load_module(filepath: Path, project_root: Path, out: list[type[Model]]) -> None:
    spec = importlib.util.spec_from_file_location(
        f"_alf_models_{filepath.stem}",
        filepath,
        submodule_search_locations=[str(project_root)],
    )
    if spec is None or spec.loader is None:
        return
    mod = importlib.util.module_from_spec(spec)
    if project_root not in sys.path:
        sys.path.insert(0, str(project_root))
    try:
        spec.loader.exec_module(mod)
    except Exception:
        return
    for attr in dir(mod):
        if attr.startswith("_"):
            continue
        obj = getattr(mod, attr)
        if (
            isinstance(obj, type)
            and issubclass(obj, Model)
            and obj is not Model
            and getattr(obj, "_alf_layer", None)
        ):
            out.append(obj)


def _load_package(models_dir: Path, project_root: Path, out: list[type[Model]]) -> None:
    """Load config.models package (import config.models)."""
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    try:
        import config.models as mod  # noqa: PLC0415
    except ImportError:
        return
    for attr in dir(mod):
        if attr.startswith("_"):
            continue
        obj = getattr(mod, attr)
        if (
            isinstance(obj, type)
            and issubclass(obj, Model)
            and obj is not Model
            and getattr(obj, "_alf_layer", None)
        ):
            out.append(obj)
