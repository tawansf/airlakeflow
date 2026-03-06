"""Run Docker Compose commands for the project (up, stop, restart, down)."""
from __future__ import annotations

import os
import socket
import subprocess
import sys
from pathlib import Path


def _ensure_compose(project_root: Path) -> None:
    if not (project_root / "docker-compose.yaml").is_file():
        print("No docker-compose.yaml in project. Run from an AirLakeFlow project root.", file=sys.stderr)
        sys.exit(1)


def _current_uid() -> int | None:
    """Return current user UID on Unix, None on Windows."""
    try:
        return os.getuid()
    except AttributeError:
        return None


def _current_gid() -> int | None:
    """Return current user GID on Unix, None on Windows."""
    try:
        return os.getgid()
    except AttributeError:
        return None


def _find_free_port(start: int = 5432, max_tries: int = 10) -> int:
    """Return first port in [start, start+max_tries) that is free to bind."""
    for i in range(max_tries):
        port = start + i
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("", port))
                return port
        except OSError:
            continue
    return start  # fallback


def create_env_from_example(project_root: Path) -> bool:
    """Create .env from .env.example with AIRFLOW_UID and POSTGRES_HOST_PORT. Returns True if created."""
    import re
    env = project_root / ".env"
    example = project_root / ".env.example"
    if env.exists() or not example.exists():
        return False
    content = example.read_text(encoding="utf-8")
    if "HOSTNAME" not in content:
        content = "HOSTNAME=airflow\n\n" + content
    uid = _current_uid()
    if uid is not None and "AIRFLOW_UID" in content:
        content = re.sub(r"AIRFLOW_UID=\d+", f"AIRFLOW_UID={uid}", content)
    elif uid is None and "AIRFLOW_UID" in content:
        content = re.sub(r"AIRFLOW_UID=\d+", "AIRFLOW_UID=50000", content)
    # Set Postgres host port to a free one so multiple projects can run (5432 already in use -> 5433, ...)
    free_port = _find_free_port(5432)
    if "POSTGRES_HOST_PORT" in content:
        content = re.sub(r"POSTGRES_HOST_PORT=\d+", f"POSTGRES_HOST_PORT={free_port}", content)
    else:
        content = content.replace("\nDATAWAREHOUSE_DB=", f"\nPOSTGRES_HOST_PORT={free_port}\n\nDATAWAREHOUSE_DB=", 1)
    env.write_text(content, encoding="utf-8")
    return True


def _ensure_env(project_root: Path) -> None:
    """Create .env from .env.example if .env is missing (so 'alf run' works on first use)."""
    if create_env_from_example(project_root):
        print("Created .env from .env.example. Edit .env if you need to change values.")


def _sync_airflow_uid(project_root: Path) -> None:
    """On Unix, set AIRFLOW_UID in .env to current user so container can write to mounted logs (same UID as host)."""
    uid = _current_uid()
    if uid is None:
        return
    env_file = project_root / ".env"
    if not env_file.exists():
        return
    import re
    text = env_file.read_text(encoding="utf-8")
    match = re.search(r"AIRFLOW_UID=(\d+)", text)
    if not match:
        return
    current_val = int(match.group(1))
    if current_val == uid:
        return
    new_text = re.sub(r"AIRFLOW_UID=\d+", f"AIRFLOW_UID={uid}", text)
    env_file.write_text(new_text, encoding="utf-8")
    print(f"AIRFLOW_UID set to {uid} (current user) so container can write to logs.")


# Subdirs Airflow creates under logs/ (so we pre-create them with perms the container can write to)
_AIRFLOW_LOG_SUBDIRS = ("scheduler", "webserver", "worker", "triggerer", "dag_processor")


def _ensure_logs(project_root: Path) -> None:
    """Create logs/ and Airflow subdirs with perms so the container can write (avoids PermissionError)."""
    logs_dir = project_root / "logs"
    try:
        logs_dir.mkdir(parents=True, exist_ok=True)
        for sub in _AIRFLOW_LOG_SUBDIRS:
            d = logs_dir / sub
            d.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(logs_dir, 0o777)
            for sub in _AIRFLOW_LOG_SUBDIRS:
                os.chmod(logs_dir / sub, 0o777)
        except OSError:
            pass  # e.g. Windows or no permission; dirs exist, container may still work
    except PermissionError:
        uid, gid = _current_uid(), _current_gid()
        fix = f"sudo chown -R {uid}:{gid} logs" if (uid is not None and gid is not None) else "remove the logs folder and run again"
        print(
            f"Permission denied on logs/: it was likely created by Docker as root.\n"
            f"Fix: run from the project directory:  {fix}\n"
            f"Or:  rm -rf logs  then run  alf run  again.",
            file=sys.stderr,
        )
        sys.exit(1)


def _compose(project_root: Path, *args: str, stream: bool = False) -> int:
    """Run docker compose with given args. Returns exit code. If stream=True, attach to output."""
    _ensure_compose(project_root)
    cmd = ["docker", "compose", *args]
    if stream:
        r = subprocess.run(cmd, cwd=project_root)
        return r.returncode
    r = subprocess.run(cmd, cwd=project_root)
    return r.returncode


def run_up(project_root: Path, detach: bool = True, build: bool = False) -> int:
    """Start the stack: docker compose up [-d] [--build]. Returns exit code."""
    root = Path(project_root).resolve()
    _ensure_env(root)
    _sync_airflow_uid(root)  # Unix: .env AIRFLOW_UID = current user (evita PermissionError em logs)
    _ensure_logs(root)
    args = ["up"]
    if detach:
        args.append("-d")
    if build:
        args.append("--build")
    return _compose(root, *args, stream=not detach)


def run_stop(project_root: Path) -> int:
    """Stop containers: docker compose stop. Returns exit code."""
    return _compose(Path(project_root).resolve(), "stop")


def run_restart(project_root: Path) -> int:
    """Restart: stop then up -d. Returns exit code."""
    root = Path(project_root).resolve()
    code = _compose(root, "stop")
    if code != 0:
        return code
    return _compose(root, "up", "-d")


def run_down(project_root: Path, volumes: bool = False) -> int:
    """Tear down: docker compose down [--volumes]. Returns exit code."""
    args = ["down"]
    if volumes:
        args.append("--volumes")
    return _compose(Path(project_root).resolve(), *args)


def run_logs(project_root: Path, follow: bool = False, service: str | None = None) -> int:
    """Show logs: docker compose logs [-f] [service]. Returns exit code."""
    args = ["logs"]
    if follow:
        args.append("-f")
    if service:
        args.append(service)
    return _compose(Path(project_root).resolve(), *args, stream=True)


def run_ps(project_root: Path) -> int:
    """List services: docker compose ps. Returns exit code."""
    return _compose(Path(project_root).resolve(), "ps", stream=True)
