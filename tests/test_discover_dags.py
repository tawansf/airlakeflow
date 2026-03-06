"""Tests for discover_dags and migration numbering."""

from airlakeflow.new_migration import discover_dags


def test_discover_dags_empty(tmp_path):
    assert discover_dags(tmp_path) == []


def test_discover_dags_no_dags_dir(tmp_path):
    (tmp_path / "other").mkdir()
    assert discover_dags(tmp_path) == []


def test_discover_dags_finds_pipeline_folders(tmp_path):
    dags = tmp_path / "dags"
    dags.mkdir()
    (dags / "crypto").mkdir()
    (dags / "crypto" / "pipeline.py").write_text("# pipeline")
    (dags / "vendas").mkdir()
    (dags / "vendas" / "pipeline.py").write_text("# pipeline")
    (dags / "skip").mkdir()  # no pipeline.py
    (dags / "_internal").mkdir()
    (dags / "_internal" / "pipeline.py").write_text("# pipeline")
    result = discover_dags(tmp_path)
    assert set(result) == {"crypto", "vendas"}
    assert result == sorted(result)
