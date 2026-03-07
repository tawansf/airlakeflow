"""Tests for alf init (structure and config)."""

from click.testing import CliRunner

from airlakeflow.cli import cli


def test_init_creates_project_with_config(tmp_path, monkeypatch):
    """alf init with -D -M -b pandas creates dirs and .airlakeflow.yaml."""
    monkeypatch.chdir(tmp_path)
    dest = tmp_path / "out"
    runner = CliRunner()
    r = runner.invoke(
        cli,
        [
            "init",
            str(dest),
            "-D",
            "-M",
            "-b",
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
    # No demo: crypto should be removed (skeleton has it, init removes when -D)
    assert not (dest / "dags" / "crypto").exists()


def test_init_non_interactive_defaults(tmp_path, monkeypatch):
    """Without flags and non-TTY, uses defaults (demo=True, monitoring=False, backend=pandas)."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    dest = tmp_path / "out2"
    runner = CliRunner()
    r = runner.invoke(cli, ["init", str(dest)])
    assert r.exit_code == 0
    assert (dest / "dags").is_dir()
    assert (dest / ".airlakeflow.yaml").exists()


def test_init_creates_default_model_when_no_models(tmp_path, monkeypatch):
    """Init creates config/models/example.py when config/models/ has no .py models."""
    monkeypatch.chdir(tmp_path)
    dest = tmp_path / "out_model"
    runner = CliRunner()
    r = runner.invoke(
        cli,
        ["init", str(dest), "-D", "-M", "-b", "pandas"],
    )
    assert r.exit_code == 0
    example = dest / "config" / "models" / "example.py"
    assert example.exists()
    content = example.read_text()
    assert "ExampleModel" in content
    assert "silver" in content.lower()
    assert "@layer" in content


def test_init_creates_venv(tmp_path, monkeypatch):
    """Init creates .venv in the project directory."""
    monkeypatch.chdir(tmp_path)
    dest = tmp_path / "out_venv"
    runner = CliRunner()
    r = runner.invoke(
        cli,
        ["init", str(dest), "-D", "-M", "-b", "pandas"],
    )
    assert r.exit_code == 0
    venv = dest / ".venv"
    assert venv.is_dir()
    # Standard venv structure
    assert (venv / "pyvenv.cfg").exists() or (venv / "bin").exists() or (venv / "Scripts").exists()


def test_init_config_has_architecture(tmp_path, monkeypatch):
    """Init writes architecture: medallion in .airlakeflow.yaml."""
    monkeypatch.chdir(tmp_path)
    dest = tmp_path / "out_arch"
    runner = CliRunner()
    r = runner.invoke(
        cli,
        ["init", str(dest), "-D", "-M", "-b", "pandas"],
    )
    assert r.exit_code == 0
    content = (dest / ".airlakeflow.yaml").read_text()
    assert "architecture" in content
    assert "medallion" in content
