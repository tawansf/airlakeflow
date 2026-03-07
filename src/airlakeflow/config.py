"""Resolve project root and options from .airlakeflow.yaml or pyproject.toml [tool.airlakeflow].

Supported config keys (all optional):
- silver_backend: pandas | pyspark
- project_root: path (in pyproject only)
- soda_data_source: Airflow connection / Soda data source name (default: postgres_datawarehouse)
- soda_config_path: path to configuration.yaml relative to project root (default: soda/configuration.yaml)
- contracts_dir: path to Soda contracts dir relative to project root (default: soda/contracts)
- migration_driver: SQL dialect for generating migrations from models (default: postgres; future: oracle, sqlserver, ...)
- architecture: data architecture (default: medallion; future: data_vault, kimball, ...)
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from airlakeflow.architectures.base import BaseArchitecture

DEFAULT_SODA_DATA_SOURCE = "postgres_datawarehouse"
DEFAULT_SODA_CONFIG_PATH = "soda/configuration.yaml"
DEFAULT_CONTRACTS_DIR = "soda/contracts"
DEFAULT_MIGRATION_DRIVER = "postgres"
DEFAULT_ARCHITECTURE = "medallion"
DEFAULT_RUNTIME = "docker"  # docker | local; set at init, locked per project


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
    m = re.search(r'migration_driver\s*=\s*["\']([^"\']+)["\']', block)
    if m:
        out["migration_driver"] = m.group(1).strip()
    m = re.search(r'architecture\s*=\s*["\']([^"\']+)["\']', block)
    if m:
        out["architecture"] = m.group(1).strip()
    return out


def get_soda_data_source(config: dict[str, Any]) -> str:
    """Return Soda data source name from config, or default."""
    return config.get("soda_data_source") or DEFAULT_SODA_DATA_SOURCE


def get_soda_config_path(config: dict[str, Any]) -> str:
    """Return path to Soda configuration.yaml from config, or default."""
    return config.get("soda_config_path") or DEFAULT_SODA_CONFIG_PATH


def get_contracts_dir(config: dict[str, Any]) -> str:
    """Return path to Soda contracts directory from config, or default."""
    return config.get("contracts_dir") or DEFAULT_CONTRACTS_DIR


def get_runtime(project_root: Path) -> str:
    """Return runtime: 'docker' or 'local'. Set at init; do not change per project."""
    cfg = load_config(Path(project_root).resolve())
    r = (cfg.get("runtime") or DEFAULT_RUNTIME).strip().lower()
    return r if r in ("docker", "local") else DEFAULT_RUNTIME


def get_migration_driver(config: dict[str, Any]) -> str:
    """Return migration dialect/driver from config (e.g. postgres). Used when generating migrations from models."""
    return (config.get("migration_driver") or DEFAULT_MIGRATION_DRIVER).strip().lower()


def get_architecture_from_config(config: dict[str, Any]) -> type[BaseArchitecture]:
    """Return the architecture class for the project (e.g. Medallion). Used for layer order, CLI choices, rollback."""
    from airlakeflow.architectures import get_architecture

    name = (config.get("architecture") or DEFAULT_ARCHITECTURE).strip().lower()
    return get_architecture(name)


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
