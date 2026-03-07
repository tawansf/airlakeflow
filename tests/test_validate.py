"""Tests for validate_cmd (structure checks only, no Docker)."""

from click.testing import CliRunner

from airlakeflow.cli import cli
from airlakeflow.validate_cmd import run_validate


def test_validate_fails_without_dags(tmp_path):
    (tmp_path / "soda").mkdir()
    (tmp_path / "scripts").mkdir()
    (tmp_path / "docker-compose.yaml").write_text("services: {}")
    ok = run_validate(
        tmp_path, check_docker=False, check_structure=True, check_stack_up=False, verbose=False
    )
    assert ok is False


def test_validate_fails_without_docker_compose(tmp_path):
    (tmp_path / "dags").mkdir()
    (tmp_path / "soda").mkdir()
    (tmp_path / "scripts").mkdir()
    ok = run_validate(
        tmp_path, check_docker=False, check_structure=True, check_stack_up=False, verbose=False
    )
    assert ok is False


def test_validate_structure_ok_with_key_files(tmp_path):
    (tmp_path / "dags").mkdir()
    (tmp_path / "dags" / "setup_database.py").write_text("# setup")
    (tmp_path / "dags" / "sql").mkdir(parents=True)
    (tmp_path / "soda").mkdir()
    (tmp_path / "soda" / "configuration.yaml").write_text("name: x")
    (tmp_path / "scripts").mkdir()
    (tmp_path / "docker-compose.yaml").write_text("services: {}")
    ok = run_validate(
        tmp_path, check_docker=False, check_structure=True, check_stack_up=False, verbose=False
    )
    assert ok is True


def test_validate_cli_exit_zero(tmp_path):
    """alf validate --no-docker -q on valid project exits 0."""
    (tmp_path / "dags").mkdir()
    (tmp_path / "dags" / "setup_database.py").write_text("# setup")
    (tmp_path / "dags" / "sql").mkdir(parents=True)
    (tmp_path / "soda").mkdir()
    (tmp_path / "soda" / "configuration.yaml").write_text("name: x")
    (tmp_path / "scripts").mkdir()
    (tmp_path / "docker-compose.yaml").write_text("services: {}")
    runner = CliRunner()
    r = runner.invoke(
        cli,
        ["validate", "-r", str(tmp_path), "-N", "-S", "-q"],
    )
    assert r.exit_code == 0
