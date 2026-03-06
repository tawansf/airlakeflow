"""Upgrade project files from the current framework skeleton (optional backup)."""

from __future__ import annotations

import shutil
from pathlib import Path

from airlakeflow.style import secho_info, secho_ok


def run_upgrade(project_root: Path, dry_run: bool = False, backup: bool = True) -> bool:
    """
    Copy skeleton files over the project, optionally backing up existing ones.
    Only copies files that exist in the skeleton; does not remove extra files in the project.
    Returns True if any file was updated.
    """
    root = Path(project_root).resolve()
    skeleton = Path(__file__).resolve().parent / "skeleton"
    if not skeleton.is_dir():
        return False

    updated = False
    backup_dir = root / ".airlakeflow_backup" if backup else None

    def copy_if_exists(relative: str) -> None:
        nonlocal updated
        src = skeleton / relative
        if not src.exists():
            return
        dst = root / relative
        if dst.exists() and dst.read_bytes() != src.read_bytes():
            if backup_dir and dst.is_file():
                backup_dir.mkdir(parents=True, exist_ok=True)
                back = backup_dir / relative
                back.parent.mkdir(parents=True, exist_ok=True)
                if not dry_run:
                    shutil.copy2(dst, back)
            if not dry_run:
                shutil.copy2(src, dst)
            updated = True
            secho_ok(f"  Updated: {relative}")
        elif not dst.exists():
            dst.parent.mkdir(parents=True, exist_ok=True)
            if not dry_run:
                shutil.copy2(src, dst)
            updated = True
            secho_ok(f"  Created: {relative}")

    # Only copy specific framework files (do not overwrite user dags/, scripts/, etc.)
    for f in ["docker-compose.yaml", "Dockerfile", ".env.example", "requirements.txt", "README.md"]:
        copy_if_exists(f)
    copy_if_exists("dags/setup_database.py")
    for f in (skeleton / "dags" / "sql" / "migrations").glob("*.sql"):
        copy_if_exists(f"dags/sql/migrations/{f.name}")
    copy_if_exists("soda/configuration.yaml")

    if backup_dir and backup and updated and not dry_run:
        secho_info(f"  Backup in: {backup_dir}")
    return updated
