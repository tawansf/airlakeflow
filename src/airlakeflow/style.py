"""Terminal styling for the CLI (colors, bold) using Click. Respects NO_COLOR and TTY."""

from __future__ import annotations

import os
import sys

import click


def _color_enabled() -> bool:
    """Use color only when stdout is a TTY or FORCE_COLOR is set; never when NO_COLOR is set."""
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("FORCE_COLOR"):
        return True
    return sys.stdout.isatty()


def _style(text: str, **kwargs) -> str:
    if _color_enabled():
        return click.style(text, **kwargs)
    return text


def ok(text: str) -> str:
    """Green, for success / [OK]."""
    return _style(text, fg="green")


def fail(text: str) -> str:
    """Red, for errors / [FAIL]."""
    return _style(text, fg="red")


def warn(text: str) -> str:
    """Yellow, for warnings."""
    return _style(text, fg="yellow")


def info(text: str) -> str:
    """Cyan, for informational lines."""
    return _style(text, fg="cyan")


def dim(text: str) -> str:
    """Dim/gray for secondary text."""
    return _style(text, dim=True)


def heading(text: str) -> str:
    """Bold for section titles."""
    return _style(text, bold=True)


def prompt_line(text: str) -> str:
    """Bold cyan for suggested commands."""
    return _style(text, fg="cyan", bold=True)


def secho(text: str, **kwargs) -> None:
    """Echo styled text (pass kwargs to click.style, e.g. fg='green')."""
    click.echo(_style(text, **kwargs) if _color_enabled() else text)


def secho_ok(text: str) -> None:
    click.echo(ok(text))


def secho_fail(text: str) -> None:
    click.echo(fail(text))


def secho_warn(text: str) -> None:
    click.echo(warn(text))


def secho_info(text: str) -> None:
    click.echo(info(text))


def secho_dim(text: str) -> None:
    click.echo(dim(text))


def secho_heading(text: str) -> None:
    click.echo(heading(text))
