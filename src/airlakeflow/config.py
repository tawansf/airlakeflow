"""Resolve project root and options from .airlakeflow.yaml or pyproject.toml [tool.airlakeflow]."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any


def _find_config_dir(start: Path) -> Path | None:
    """Walk up from start until we find .airlakeflow.yaml or pyproject.toml with [tool.airlakeflow]."""
    current = start.resolve()
    for _ in range(20):
        if (current / ".airlakeflow.yaml").exists():
            return current
        pyproject = current / "pyproject.toml"
        if pyproject.exists():
            try:
                if "[tool.airlakeflow]" in pyproject.read_text(encoding="utf-8"):
                    return current
            except OSError:
                pass
        parent = current.parent
        if parent == current:
            break
        current = parent
    return None


def _parse_pyproject_airlakeflow(content: str) -> dict[str, Any]:
    """Extract [tool.airlakeflow] section from pyproject.toml (simple regex)."""
    out: dict[str, Any] = {}
    match = re.search(r"\[tool\.airlakeflow\](.*?)(?=\n\[|\Z)", content, re.DOTALL)
    if not match:
        return out
    block = match.group(1)
    m = re.search(r'project_root\s*=\s*["\']([^"\']+)["\']', block)
    if m:
        out["project_root"] = m.group(1).strip()
    m = re.search(r'silver_backend\s*=\s*["\'](pandas|pyspark)["\']', block)
    if m:
        out["silver_backend"] = m.group(1).strip()
    return out


def load_config(project_root: Path) -> dict[str, Any]:
    """Load config dict from project (.airlakeflow.yaml or pyproject.toml). Returns {} if none."""
    root = project_root.resolve()
    # .airlakeflow.yaml (optional: requires PyYAML)
    yaml_path = root / ".airlakeflow.yaml"
    if yaml_path.exists():
        try:
            import yaml

            data = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
            return data if isinstance(data, dict) else {}
        except Exception:
            pass
    # pyproject.toml [tool.airlakeflow]
    pyproject = root / "pyproject.toml"
    if pyproject.exists():
        return _parse_pyproject_airlakeflow(pyproject.read_text(encoding="utf-8"))
    return {}


def resolve_project_root(given: str | Path) -> Path:
    """
    Resolve project root: if given is '.' (default), search for config file in cwd and parents;
    the directory containing the config is the project root. Otherwise resolve the given path.
    """
    given_path = Path(given).resolve()
    if given != "." and str(given) != ".":
        return given_path
    config_dir = _find_config_dir(Path.cwd())
    if config_dir is not None:
        cfg = load_config(config_dir)
        root_val = cfg.get("project_root")
        if root_val is not None:
            p = Path(root_val)
            if not p.is_absolute():
                p = (config_dir / p).resolve()
            return p
        return config_dir
    return given_path
