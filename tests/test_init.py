"""Tests for alf init (structure and config)."""

from pathlib import Path

from click.testing import CliRunner

from airlakeflow.cli import main


def test_init_creates_project_with_config(tmp_path, monkeypatch):
    """alf init with --no-demo --no-monitoring --backend pandas creates dirs and .airlakeflow.yaml."""
    monkeypatch.chdir(tmp_path)
    dest = tmp_path / "out"
    runner = CliRunner()
    r = runner.invoke(
        main,
        [
            "init",
            str(dest),
            "--no-demo",
            "--no-monitoring",
            "--backend",
            "pandas",
        ],
    )
    assert r.exit_code == 0
    assert (dest / "dags").is_dir()
    assert (dest / "soda").is_dir()
    assert (dest / "docker-compose.yaml").exists()
    assert (dest / ".airlakeflow.yaml").exists()
    content = (dest / ".airlakeflow.yaml").read_text()
    assert "silver_backend: pandas" in content
    assert "soda_data_source" in content or "contracts_dir" in content  # commented options
    # No demo: crypto should be removed (skeleton has it, init removes when --no-demo)
    assert not (dest / "dags" / "crypto").exists()


def test_init_non_interactive_defaults(tmp_path, monkeypatch):
    """Without flags and non-TTY, uses defaults (demo=True, monitoring=False, backend=pandas)."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    dest = tmp_path / "out2"
    runner = CliRunner()
    r = runner.invoke(main, ["init", str(dest)])
    assert r.exit_code == 0
    assert (dest / "dags").is_dir()
    assert (dest / ".airlakeflow.yaml").exists()
