"""Tests for alf docs command."""

from pathlib import Path

from click.testing import CliRunner

from airlakeflow.cli import cli


def test_docs_creates_catalog_html(tmp_path):
    """alf docs creates docs/catalog.html."""
    (tmp_path / "dags").mkdir(parents=True)
    (tmp_path / "dags" / "sql").mkdir()
    (tmp_path / "config").mkdir()
    runner = CliRunner()
    r = runner.invoke(cli, ["docs", "-r", str(tmp_path)])
    assert r.exit_code == 0
    out = tmp_path / "docs" / "catalog.html"
    assert out.exists()
    content = out.read_text()
    assert "Catálogo" in content or "catalog" in content.lower()
    assert "<table>" in content


def test_docs_json_format(tmp_path):
    """alf docs --format json creates docs/catalog.json."""
    (tmp_path / "dags").mkdir(parents=True)
    (tmp_path / "config").mkdir()
    runner = CliRunner()
    r = runner.invoke(cli, ["docs", "-r", str(tmp_path), "--format", "json"])
    assert r.exit_code == 0
    assert (tmp_path / "docs" / "catalog.json").exists()
    import json
    data = json.loads((tmp_path / "docs" / "catalog.json").read_text())
    assert isinstance(data, list)


def test_docs_custom_output_dir(tmp_path):
    """alf docs -o out writes to out/catalog.html."""
    (tmp_path / "dags").mkdir(parents=True)
    (tmp_path / "config").mkdir()
    runner = CliRunner()
    r = runner.invoke(cli, ["docs", "-r", str(tmp_path), "-o", "out"])
    assert r.exit_code == 0
    assert (tmp_path / "out" / "catalog.html").exists()


def test_docs_idempotent(tmp_path):
    """Running alf docs twice does not fail."""
    (tmp_path / "dags").mkdir(parents=True)
    (tmp_path / "config").mkdir()
    runner = CliRunner()
    r1 = runner.invoke(cli, ["docs", "-r", str(tmp_path)])
    r2 = runner.invoke(cli, ["docs", "-r", str(tmp_path)])
    assert r1.exit_code == 0
    assert r2.exit_code == 0
    assert (tmp_path / "docs" / "catalog.html").exists()
