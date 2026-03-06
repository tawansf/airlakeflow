"""Run Docker Compose commands for the project (up, stop, restart, down)."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
from pathlib import Path

import click

from airlakeflow.style import fail, secho_fail, secho_info, secho_ok, secho_warn


def _ensure_compose(project_root: Path) -> None:
    if not (project_root / "docker-compose.yaml").is_file():
        click.echo(
            fail("No docker-compose.yaml in project. Run from an AirLakeFlow project root."),
            err=True,
        )
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


def _get_postgres_port_from_env(project_root: Path) -> int:
    """Read POSTGRES_HOST_PORT from .env; return 5432 if missing or invalid."""
    import re

    env_file = project_root / ".env"
    if not env_file.exists():
        return 5432
    text = env_file.read_text(encoding="utf-8")
    m = re.search(r"POSTGRES_HOST_PORT[=\s]+(\d+)", text)
    if m:
        return int(m.group(1))
    return 5432


def _get_webserver_port_from_env(project_root: Path) -> int:
    """Read AIRFLOW_WEBSERVER_PORT from .env; return 8080 if missing or invalid."""
    import re

    env_file = project_root / ".env"
    if not env_file.exists():
        return 8080
    text = env_file.read_text(encoding="utf-8")
    m = re.search(r"AIRFLOW_WEBSERVER_PORT[=\s]+(\d+)", text)
    if m:
        return int(m.group(1))
    return 8080


def _set_ports_in_env(
    project_root: Path, postgres_port: int, webserver_port: int
) -> bool:
    """Set POSTGRES_HOST_PORT and AIRFLOW_WEBSERVER_PORT in .env. Create from .env.example if needed. Returns True if updated."""
    import re

    env_file = project_root / ".env"
    example = project_root / ".env.example"
    if not env_file.exists() and example.exists():
        create_env_from_example(project_root)
    if not env_file.exists():
        return False
    text = env_file.read_text(encoding="utf-8")
    new_text = text

    if "POSTGRES_HOST_PORT" in new_text:
        new_text = re.sub(
            r"POSTGRES_HOST_PORT\s*=\s*\d+",
            f"POSTGRES_HOST_PORT={postgres_port}",
            new_text,
        )
    else:
        # Prefer inserting before DATAWAREHOUSE_DB if present; otherwise append
        if "\nDATAWAREHOUSE_DB=" in new_text:
            new_text = new_text.replace(
                "\nDATAWAREHOUSE_DB=",
                f"\nPOSTGRES_HOST_PORT={postgres_port}\n\nDATAWAREHOUSE_DB=",
                1,
            )
        else:
            new_text = new_text.rstrip() + f"\nPOSTGRES_HOST_PORT={postgres_port}\n"

    if "AIRFLOW_WEBSERVER_PORT" in new_text:
        new_text = re.sub(
            r"AIRFLOW_WEBSERVER_PORT\s*=\s*\d+",
            f"AIRFLOW_WEBSERVER_PORT={webserver_port}",
            new_text,
        )
    else:
        if "\nPOSTGRES_HOST_PORT=" in new_text:
            new_text = new_text.replace(
                "\nPOSTGRES_HOST_PORT=",
                f"\nAIRFLOW_WEBSERVER_PORT={webserver_port}\n\nPOSTGRES_HOST_PORT=",
                1,
            )
        else:
            new_text = new_text.rstrip() + f"\nAIRFLOW_WEBSERVER_PORT={webserver_port}\n"

    if new_text == text:
        return False
    env_file.write_text(new_text, encoding="utf-8")
    return True


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
    # Set Postgres and Webserver ports to free ones so multiple projects can run
    free_pg = _find_free_port(5432)
    free_ws = _find_free_port(8080)
    if "POSTGRES_HOST_PORT" in content:
        content = re.sub(r"POSTGRES_HOST_PORT=\d+", f"POSTGRES_HOST_PORT={free_pg}", content)
    elif "\nDATAWAREHOUSE_DB=" in content:
        content = content.replace(
            "\nDATAWAREHOUSE_DB=", f"\nPOSTGRES_HOST_PORT={free_pg}\n\nDATAWAREHOUSE_DB=", 1
        )
    else:
        content = content.rstrip() + f"\nPOSTGRES_HOST_PORT={free_pg}\n"
    if "AIRFLOW_WEBSERVER_PORT" in content:
        content = re.sub(
            r"AIRFLOW_WEBSERVER_PORT=\d+", f"AIRFLOW_WEBSERVER_PORT={free_ws}", content
        )
    elif "\nPOSTGRES_HOST_PORT=" in content:
        content = content.replace(
            "\nPOSTGRES_HOST_PORT=",
            f"\nAIRFLOW_WEBSERVER_PORT={free_ws}\n\nPOSTGRES_HOST_PORT=",
            1,
        )
    else:
        content = content.rstrip() + f"\nAIRFLOW_WEBSERVER_PORT={free_ws}\n"
    env.write_text(content, encoding="utf-8")
    return True


def _ensure_env(project_root: Path) -> None:
    """Create .env from .env.example if .env is missing (so 'alf run' works on first use)."""
    if create_env_from_example(project_root):
        secho_info("Created .env from .env.example. Edit .env if you need to change values.")
    # Avoid "The HOSTNAME variable is not set" from docker compose
    env_file = project_root / ".env"
    if env_file.exists() and "HOSTNAME" not in env_file.read_text(encoding="utf-8"):
        content = env_file.read_text(encoding="utf-8")
        env_file.write_text("HOSTNAME=airflow\n\n" + content, encoding="utf-8")


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
    secho_info(f"AIRFLOW_UID set to {uid} (current user) so container can write to logs.")


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
        fix = (
            f"sudo chown -R {uid}:{gid} logs"
            if (uid is not None and gid is not None)
            else "remove the logs folder and run again"
        )
        click.echo(
            fail(
                f"Permission denied on logs/: it was likely created by Docker as root.\n"
                f"Fix: run from the project directory:  {fix}\n"
                f"Or:  rm -rf logs  then run  alf run  again."
            ),
            err=True,
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
    """Start the stack: docker compose up [-d] [--build]. On port conflict, retries with next free port."""
    root = Path(project_root).resolve()
    _ensure_env(root)
    _sync_airflow_uid(root)  # Unix: .env AIRFLOW_UID = current user (evita PermissionError em logs)
    _ensure_logs(root)
    args = ["up"]
    if detach:
        args.append("-d")
    if build:
        args.append("--build")

    max_port_retries = 3
    code = 1
    for attempt in range(max_port_retries + 1):
        code = _compose(root, *args, stream=not detach)
        if code == 0:
            return 0
        if attempt >= max_port_retries:
            return code
        current_pg = _get_postgres_port_from_env(root)
        current_ws = _get_webserver_port_from_env(root)
        next_pg = _find_free_port(start=current_pg + 1, max_tries=5)
        next_ws = _find_free_port(start=current_ws + 1, max_tries=5)
        if next_pg == current_pg and next_ws == current_ws:
            return code
        if next_pg == current_pg:
            next_pg = _find_free_port(5432, max_tries=10)
        if next_ws == current_ws:
            next_ws = _find_free_port(8080, max_tries=10)
        if not _set_ports_in_env(root, next_pg, next_ws):
            return code
        secho_warn(
            f"Run failed (exit code {code}). Retrying with POSTGRES_HOST_PORT={next_pg}, AIRFLOW_WEBSERVER_PORT={next_ws}..."
        )
        # Clean partial state so next 'up' picks up new ports
        _compose(root, "down", stream=False)
    return code


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


def run_exec(project_root: Path, service: str, cmd: list[str]) -> int:
    """Run a command inside a service container: docker compose exec SERVICE CMD. Returns exit code."""
    root = Path(project_root).resolve()
    _ensure_compose(root)
    full_cmd = ["docker", "compose", "exec", service] + cmd
    r = subprocess.run(full_cmd, cwd=root)
    return r.returncode


def run_status(project_root: Path) -> int:
    """Print a short status summary (how many services up). Returns 0 if stack up, 1 otherwise."""
    root = Path(project_root).resolve()
    compose_file = root / "docker-compose.yaml"
    if not compose_file.exists():
        secho_fail("No docker-compose.yaml in project.")
        return 1
    try:
        r = subprocess.run(
            ["docker", "compose", "ps", "--format", "json"],
            capture_output=True,
            text=True,
            timeout=15,
            cwd=root,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        secho_fail("Docker Compose not available or timed out.")
        return 1
    if r.returncode != 0:
        secho_fail("Stack not running. Run: alf run")
        return 1
    import json

    up = 0
    exited = 0
    for line in r.stdout.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            state = (obj.get("State") or obj.get("Status") or "").lower()
            if "up" in state or "running" in state:
                up += 1
            else:
                exited += 1
        except Exception:
            pass
    if up > 0:
        secho_ok(f"Stack: {up} service(s) running." + (f" {exited} exited." if exited else ""))
        return 0
    secho_fail("Stack not running. Run: alf run")
    return 1
