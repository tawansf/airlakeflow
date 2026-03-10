"""Tests for alf data-tests command."""

from pathlib import Path

from click.testing import CliRunner

from airlakeflow.cli import cli


def test_data_tests_creates_dag_and_config(tmp_path):
    """alf data-tests creates config/data_tests.yaml and dags/01_data_tests.py."""
    (tmp_path / "dags").mkdir(parents=True)
    (tmp_path / "config").mkdir()
    runner = CliRunner()
    r = runner.invoke(cli, ["data-tests", "-r", str(tmp_path)])
    assert r.exit_code == 0
    assert (tmp_path / "config" / "data_tests.yaml").exists()
    dag_file = tmp_path / "dags" / "01_data_tests.py"
    assert dag_file.exists()
    content = dag_file.read_text()
    assert "01_data_tests" in content
    assert "run_data_tests" in content
    assert "data_tests" in content


def test_data_tests_idempotent(tmp_path):
    """Running alf data-tests twice does not fail."""
    (tmp_path / "dags").mkdir(parents=True)
    (tmp_path / "config").mkdir()
    runner = CliRunner()
    r1 = runner.invoke(cli, ["data-tests", "-r", str(tmp_path)])
    r2 = runner.invoke(cli, ["data-tests", "-r", str(tmp_path)])
    assert r1.exit_code == 0
    assert r2.exit_code == 0
    assert (tmp_path / "dags" / "01_data_tests.py").exists()
