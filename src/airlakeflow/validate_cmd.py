"""Validate project structure and Docker environment."""

from __future__ import annotations

import subprocess
from pathlib import Path

import click

from airlakeflow.config import get_runtime
from airlakeflow.style import SYM_FAIL, SYM_OK, secho_fail, secho_heading, secho_ok

# Expected dirs and files for an AirLakeFlow project
REQUIRED_DIRS = ["dags", "soda", "scripts"]
REQUIRED_FILES_DOCKER = ["docker-compose.yaml"]
REQUIRED_FILES = REQUIRED_FILES_DOCKER  # default for backward compatibility
OPTIONAL_FILES = [".env", ".env.example", "Dockerfile", "requirements.txt"]
# Key files we can sanity-check
KEY_FILES = [
    "dags/setup_database.py",
    "soda/configuration.yaml",
]


def _docker_available() -> tuple[bool, str]:
    """Check if Docker daemon is running. Returns (ok, message)."""
    try:
        r = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if r.returncode == 0:
            return True, "Docker daemon is running"
        return False, r.stderr.strip() or "Docker daemon not available"
    except FileNotFoundError:
        return False, "Docker not installed or not in PATH"
    except subprocess.TimeoutExpired:
        return False, "Docker command timed out"


def _compose_available(project_root: Path) -> tuple[bool, str]:
    """Check if docker compose is available (V2). Returns (ok, message)."""
    try:
        r = subprocess.run(
            ["docker", "compose", "version"],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=project_root,
        )
        if r.returncode == 0:
            return True, (r.stdout.strip() or "Docker Compose available")
        return False, "Docker Compose not available"
    except FileNotFoundError:
        return False, "docker compose not found"
    except subprocess.TimeoutExpired:
        return False, "Command timed out"


def _compose_services_up(project_root: Path) -> tuple[bool, str]:
    """Check if docker compose stack is up (at least one service running). Returns (ok, message)."""
    compose_file = project_root / "docker-compose.yaml"
    if not compose_file.exists():
        return False, "No docker-compose.yaml"
    try:
        r = subprocess.run(
            ["docker", "compose", "ps"],
            capture_output=True,
            text=True,
            timeout=15,
            cwd=project_root,
        )
        if r.returncode != 0:
            return False, r.stderr.strip() or "Failed to run docker compose ps"
        out = r.stdout.strip().lower()
        if "up" in out and ("running" in out or "healthy" in out or "up (" in out):
            return True, "Stack is up (services running)"
        if not out or "name" not in out:
            return False, "Stack not running (run: docker compose up -d) or alf run"
        if "exited" in out:
            return False, "Some containers exited (run: docker compose up -d) or alf run"
        return False, "Stack not running (run: docker compose up -d) or alf run"
    except FileNotFoundError:
        return False, "docker compose not found"
    except subprocess.TimeoutExpired:
        return False, "Command timed out"


def _structure_ok(
    project_root: Path,
    required_files: list[str] | None = None,
) -> tuple[bool, list[str]]:
    """Check required dirs and files. Returns (all_ok, list of missing items)."""
    files = required_files if required_files is not None else REQUIRED_FILES
    missing = []
    for d in REQUIRED_DIRS:
        if not (project_root / d).is_dir():
            missing.append(f"directory: {d}/")
    for f in files:
        if not (project_root / f).is_file():
            missing.append(f"file: {f}")
    return (len(missing) == 0, missing)


def _key_files_ok(project_root: Path) -> tuple[bool, list[str]]:
    """Check that key files exist. Returns (all_ok, list of missing)."""
    missing = []
    for p in KEY_FILES:
        if not (project_root / p).is_file():
            missing.append(p)
    return (len(missing) == 0, missing)


def run_validate(
    project_root: str | Path,
    check_docker: bool = True,
    check_structure: bool = True,
    check_stack_up: bool = True,
    verbose: bool = True,
) -> bool:
    """
    Run validation checks. Returns True if all requested checks pass.
    If verbose, prints each check result to stdout.
    For runtime=local, docker-compose is not required and Docker/stack checks are skipped.
    """
    root = Path(project_root).resolve()
    runtime = get_runtime(root)
    all_ok = True
    required_files: list[str] = REQUIRED_FILES_DOCKER if runtime == "docker" else []

    if verbose:
        secho_heading(f"Validating project: {root}\n")

    if check_structure:
        ok, missing = _structure_ok(root, required_files=required_files)
        if verbose:
            desc = "dags/, soda/, scripts/" + (
                ", docker-compose.yaml" if runtime == "docker" else ""
            )
            if ok:
                secho_ok(f"  {SYM_OK} Project structure ({desc})")
            else:
                secho_fail(f"  {SYM_FAIL} Structure: missing " + ", ".join(missing))
        if not ok:
            all_ok = False

        ok2, missing2 = _key_files_ok(root)
        if verbose:
            if ok2:
                secho_ok(f"  {SYM_OK} Key files (setup_database.py, soda/configuration.yaml)")
            else:
                secho_fail(f"  {SYM_FAIL} Missing files: " + ", ".join(missing2))
        if not ok2:
            all_ok = False

    if runtime == "docker" and check_docker:
        ok, msg = _docker_available()
        if verbose:
            if ok:
                secho_ok(f"  {SYM_OK} Docker: " + msg)
            else:
                secho_fail(f"  {SYM_FAIL} Docker: " + msg)
        if not ok:
            all_ok = False
        else:
            ok2, msg2 = _compose_available(root)
            if verbose:
                if ok2:
                    secho_ok(f"  {SYM_OK} Docker Compose: " + msg2)
                else:
                    secho_fail(f"  {SYM_FAIL} Docker Compose: " + msg2)
            if not ok2:
                all_ok = False

    if runtime == "docker" and check_stack_up and (root / "docker-compose.yaml").exists():
        ok, msg = _compose_services_up(root)
        if verbose:
            if ok:
                secho_ok(f"  {SYM_OK} Stack: " + msg)
            else:
                secho_fail(f"  {SYM_FAIL} Stack: " + msg)
        if not ok:
            all_ok = False

    if verbose:
        click.echo()
        if all_ok:
            secho_ok(f"{SYM_OK} Validation passed.")
        else:
            secho_fail(
                f"{SYM_FAIL} Validation failed. Fix the items above and run 'alf validate' again."
            )
    return all_ok
