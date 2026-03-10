"""Tests for alf seed command."""

from click.testing import CliRunner

from airlakeflow.cli import cli


def test_seed_creates_data_seeds_and_dag(tmp_path):
    """alf seed creates data/seeds/ and dags/00_seeds.py."""
    (tmp_path / "dags").mkdir(parents=True)
    (tmp_path / "dags" / "sql").mkdir()
    (tmp_path / "soda").mkdir()
    (tmp_path / "scripts").mkdir()
    runner = CliRunner()
    r = runner.invoke(cli, ["seed", "-r", str(tmp_path)])
    assert r.exit_code == 0
    assert (tmp_path / "data" / "seeds").is_dir()
    assert (tmp_path / "data" / "seeds" / ".gitkeep").exists()
    dag_file = tmp_path / "dags" / "00_seeds.py"
    assert dag_file.exists()
    content = dag_file.read_text()
    assert "00_seeds" in content
    assert "load_seeds" in content
    assert "data" in content and "seeds" in content


def test_seed_idempotent(tmp_path):
    """Running alf seed twice does not fail."""
    (tmp_path / "dags").mkdir(parents=True)
    runner = CliRunner()
    r1 = runner.invoke(cli, ["seed", "-r", str(tmp_path)])
    r2 = runner.invoke(cli, ["seed", "-r", str(tmp_path)])
    assert r1.exit_code == 0
    assert r2.exit_code == 0
    assert (tmp_path / "dags" / "00_seeds.py").exists()
