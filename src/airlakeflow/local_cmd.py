"""Run Airflow locally (no Docker): install deps, db init, standalone."""

from __future__ import annotations

import os
import signal
import subprocess
from pathlib import Path

from airlakeflow.style import secho_fail, secho_info


def _venv_python(project_root: Path) -> Path | None:
    """Return path to venv python (bin/python or Scripts/python.exe), or None."""
    root = Path(project_root).resolve()
    for rel in ("bin/python", "Scripts/python.exe"):
        p = root / ".venv" / rel
        if p.exists():
            return p
    return None


def _venv_env(venv_python: Path, project_root: Path) -> dict[str, str]:
    """Return os.environ with venv bin in PATH and AIRFLOW_HOME=project_root so db/logs stay in project."""
    venv_bin = venv_python.parent
    env = os.environ.copy()
    env["PATH"] = str(venv_bin) + os.pathsep + env.get("PATH", "")
    env["AIRFLOW_HOME"] = str(project_root)
    return env


def run_local(project_root: Path, skip_install: bool = False) -> int:
    """
    For runtime=local: ensure deps in project .venv, run airflow db init, then airflow standalone.
    Returns exit code. Blocks on standalone (foreground).
    """
    root = Path(project_root).resolve()
    venv_py = _venv_python(root)
    if not venv_py:
        secho_fail(
            "No .venv found. Create one: python -m venv .venv\n"
            "Then: .venv/bin/pip install -r requirements.txt  (or .venv\\Scripts\\pip on Windows)\n"
            "Then: .venv/bin/airflow db init  and  .venv/bin/airflow standalone"
        )
        return 1

    req_file = root / "requirements.txt"
    if not skip_install and req_file.exists():
        # Only install if airflow is not already in the venv (avoids reinstalling every run)
        r = subprocess.run(
            [str(venv_py), "-c", "import airflow"],
            cwd=root,
            capture_output=True,
        )
        if r.returncode != 0:
            secho_info("Installing dependencies from requirements.txt...")
            r = subprocess.run(
                [str(venv_py), "-m", "pip", "install", "-q", "-r", str(req_file)],
                cwd=root,
            )
            if r.returncode != 0:
                secho_fail(
                    "pip install failed. Run manually: .venv/bin/pip install -r requirements.txt"
                )
                return r.returncode

    env = _venv_env(venv_py, root)
    secho_info("Running: airflow db init")
    r = subprocess.run(
        [str(venv_py), "-m", "airflow", "db", "init"],
        cwd=root,
        env=env,
    )
    if r.returncode != 0:
        secho_fail("airflow db init failed.")
        return r.returncode

    secho_info("Running: airflow standalone (Ctrl+C to stop)")
    # Use Popen + signal handling so Ctrl+C terminates the child and its subprocesses (scheduler, webserver, triggerer)
    try:
        proc = subprocess.Popen(
            [str(venv_py), "-m", "airflow", "standalone"],
            cwd=root,
            env=env,
            start_new_session=(
                os.name != "nt"
            ),  # new process group on Unix so we can killpg on Ctrl+C
        )
    except OSError as e:
        secho_fail(f"Failed to start airflow standalone: {e}")
        return 1

    def _terminate_standalone() -> None:
        if proc.poll() is not None:
            return
        if os.name == "nt":
            proc.terminate()
        else:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except (ProcessLookupError, OSError):
                proc.terminate()

    try:
        proc.wait()
    except KeyboardInterrupt:
        secho_info("Stopping Airflow standalone...")
        _terminate_standalone()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        return 130  # common exit for SIGINT
    return proc.returncode if proc.returncode is not None else 0
