"""Tests for alf add alf-checks command."""

from pathlib import Path

from click.testing import CliRunner

from airlakeflow.cli import cli


def test_add_alf_checks_creates_structure_and_dag(tmp_path):
    """alf add alf-checks creates config/checks/generic.yaml, layer folders, and dags/01_alf_checks.py."""
    (tmp_path / "dags").mkdir(parents=True)
    (tmp_path / "config").mkdir()
    runner = CliRunner()
    r = runner.invoke(cli, ["add", "alf-checks", "-r", str(tmp_path)])
    assert r.exit_code == 0
    assert (tmp_path / "config" / "checks" / "generic.yaml").exists()
    assert (tmp_path / "config" / "checks" / "bronze").is_dir()
    assert (tmp_path / "config" / "checks" / "silver").is_dir()
    assert (tmp_path / "config" / "checks" / "gold").is_dir()
    dag_file = tmp_path / "dags" / "01_alf_checks.py"
    assert dag_file.exists()
    content = dag_file.read_text()
    assert "01_alf_checks" in content
    assert "run_alf_checks" in content
    assert "alf_checks" in content


def test_add_alf_checks_idempotent(tmp_path):
    """Running alf add alf-checks twice does not fail."""
    (tmp_path / "dags").mkdir(parents=True)
    (tmp_path / "config").mkdir()
    runner = CliRunner()
    r1 = runner.invoke(cli, ["add", "alf-checks", "-r", str(tmp_path)])
    r2 = runner.invoke(cli, ["add", "alf-checks", "-r", str(tmp_path)])
    assert r1.exit_code == 0
    assert r2.exit_code == 0
    assert (tmp_path / "dags" / "01_alf_checks.py").exists()


def test_load_checks_from_layer_folders(tmp_path):
    """Loader reads config/checks/bronze/*.yaml and config/checks/silver/*.yaml."""
    (tmp_path / "config" / "checks").mkdir(parents=True)
    (tmp_path / "config" / "checks" / "bronze").mkdir()
    (tmp_path / "config" / "checks" / "silver").mkdir()
    (tmp_path / "config" / "checks" / "generic.yaml").write_text("connection_id: postgres_datawarehouse\n")
    (tmp_path / "config" / "checks" / "bronze" / "example_raw.yaml").write_text(
        "checks:\n  - type: not_null\n    columns: [col1]\n"
    )
    (tmp_path / "config" / "checks" / "silver" / "example.yaml").write_text(
        "checks:\n  - type: row_count\n    min: 0\n"
    )
    from airlakeflow.data_tests import load_data_tests_config

    config = load_data_tests_config(tmp_path)
    assert config["connection_id"] == "postgres_datawarehouse"
    assert len(config["tables"]) == 2
    by_key = {(t["schema"], t["table"]): t for t in config["tables"]}
    assert ("bronze", "example_raw") in by_key
    assert by_key[("bronze", "example_raw")]["checks"][0]["type"] == "not_null"
    assert ("silver", "example") in by_key
    assert by_key[("silver", "example")]["checks"][0]["type"] == "row_count"
