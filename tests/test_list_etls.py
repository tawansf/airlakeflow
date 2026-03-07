"""Tests for alf list etls."""

from click.testing import CliRunner

from airlakeflow.cli import main


def test_list_etls_empty_fails(tmp_path):
    """No dags/ -> exit 1 and message."""
    runner = CliRunner()
    r = runner.invoke(main, ["list", "etls", "-r", str(tmp_path)])
    assert r.exit_code == 1
    assert "No ETLs found" in r.output
    assert "alf new etl" in r.output


def test_list_etls_lists_etls(tmp_path):
    """With dags/crypto and dags/vendas containing pipeline.py -> lists both."""
    dags = tmp_path / "dags"
    dags.mkdir()
    (dags / "crypto").mkdir()
    (dags / "crypto" / "pipeline.py").write_text("# pipeline")
    (dags / "vendas").mkdir()
    (dags / "vendas" / "pipeline.py").write_text("# pipeline")
    (dags / "skip").mkdir()  # no pipeline.py
    runner = CliRunner()
    r = runner.invoke(main, ["list", "etls", "-r", str(tmp_path)])
    assert r.exit_code == 0
    assert "crypto" in r.output
    assert "vendas" in r.output
    assert "skip" not in r.output


def test_list_etls_project_root(tmp_path):
    """-r points to dir with one ETL."""
    proj = tmp_path / "proj"
    proj.mkdir()
    (proj / "dags").mkdir()
    (proj / "dags" / "one").mkdir()
    (proj / "dags" / "one" / "pipeline.py").write_text("# x")
    runner = CliRunner()
    r = runner.invoke(main, ["list", "etls", "-r", str(proj)])
    assert r.exit_code == 0
    assert "one" in r.output
