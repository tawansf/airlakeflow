"""Extended validation (doctor): structure, Docker, Python, ports, permissions + suggestions."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import click

from airlakeflow.style import secho_dim, secho_fail, secho_heading, secho_ok, secho_warn
from airlakeflow.validate_cmd import (
    _compose_available,
    _compose_services_up,
    _docker_available,
    _key_files_ok,
    _structure_ok,
)


def run_doctor(project_root: Path, verbose: bool = True) -> bool:
    """Run all checks and print suggestions for fixing issues. Returns True if everything OK."""
    root = Path(project_root).resolve()
    all_ok = True

    if verbose:
        secho_heading(f"Doctor: checking project at {root}\n")

    # 1. Python version
    ver = sys.version_info
    py_ok = ver >= (3, 10)
    if verbose:
        if py_ok:
            secho_ok(f"  [OK] Python {ver.major}.{ver.minor}.{ver.micro}")
        else:
            secho_fail(f"  [FAIL] Python {ver.major}.{ver.minor} (need 3.10+)")
    if not py_ok:
        all_ok = False

    # 2. Structure
    ok, missing = _structure_ok(root)
    if verbose:
        if ok:
            secho_ok("  [OK] Project structure (dags/, soda/, scripts/, docker-compose.yaml)")
        else:
            secho_fail("  [FAIL] Missing: " + ", ".join(missing))
            secho_dim("        Fix: run from an AirLakeFlow project root or use 'alf init <name>'.")
    if not ok:
        all_ok = False

    ok2, missing2 = _key_files_ok(root)
    if verbose:
        if ok2:
            secho_ok("  [OK] Key files (setup_database.py, soda/configuration.yaml)")
        else:
            secho_fail("  [FAIL] Missing key files: " + ", ".join(missing2))
    if not ok2:
        all_ok = False

    # 3. Docker
    ok, msg = _docker_available()
    if verbose:
        if ok:
            secho_ok("  [OK] Docker: " + msg)
        else:
            secho_fail("  [FAIL] Docker: " + msg)
            secho_dim("        Fix: install Docker and start the daemon.")
    if not ok:
        all_ok = False
    else:
        ok2, msg2 = _compose_available(root)
        if verbose:
            if ok2:
                secho_ok("  [OK] Docker Compose: " + msg2)
            else:
                secho_fail("  [FAIL] Docker Compose: " + msg2)
        if not ok2:
            all_ok = False

    # 4. Stack
    if (root / "docker-compose.yaml").exists():
        ok, msg = _compose_services_up(root)
        if verbose:
            if ok:
                secho_ok("  [OK] Stack: " + msg)
            else:
                secho_fail("  [FAIL] Stack: " + msg)
        if not ok:
            all_ok = False

    # 5. .env and AIRFLOW_UID (Unix)
    try:
        uid = os.getuid()
    except AttributeError:
        uid = None
    if uid is not None and (root / ".env").exists():
        content = (root / ".env").read_text()
        if "AIRFLOW_UID=" in content:
            import re

            m = re.search(r"AIRFLOW_UID=(\d+)", content)
            if m and int(m.group(1)) != uid:
                if verbose:
                    secho_warn(
                        f"  [WARN] AIRFLOW_UID in .env ({m.group(1)}) differs from current user ({uid})."
                    )
                    secho_dim(
                        "         Fix: set AIRFLOW_UID="
                        + str(uid)
                        + " in .env to avoid permission errors on logs/."
                    )
                # don't set all_ok = False for warning

    # 6. logs/ permissions
    logs_dir = root / "logs"
    if logs_dir.exists():
        try:
            if uid is not None and os.stat(logs_dir).st_uid != uid:
                if verbose:
                    secho_warn(
                        "  [WARN] logs/ is not owned by your user. Container may fail with PermissionError."
                    )
                    secho_dim("         Fix: sudo chown -R $(id -u):$(id -g) logs")
        except OSError:
            pass

    if verbose:
        click.echo()
        if all_ok:
            secho_ok("All checks passed.")
        else:
            secho_fail("Some checks failed. Apply the suggested fixes and run 'alf doctor' again.")
    return all_ok
