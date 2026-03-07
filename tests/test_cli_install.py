"""Smoke tests: CLI is installed and responds."""

from click.testing import CliRunner

from airlakeflow import __version__
from airlakeflow.cli import cli


def test_alf_version():
    runner = CliRunner()
    r = runner.invoke(cli, ["version"])
    assert r.exit_code == 0
    assert "AirLakeFlow" in r.output
    assert __version__ in r.output


def test_alf_init_help():
    runner = CliRunner()
    r = runner.invoke(cli, ["init", "--help"])
    assert r.exit_code == 0
    assert "init" in r.output and "project" in r.output.lower()
