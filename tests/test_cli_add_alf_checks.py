from pathlib import Path

from click.testing import CliRunner

from airlakeflow.cli import cli


def test_cli_add_alf_checks_creates_structure(tmp_path: Path):
    runner = CliRunner()
    r = runner.invoke(cli, ["add", "alf-checks", "-r", str(tmp_path)])
    assert r.exit_code == 0
    checks_dir = tmp_path / "config" / "checks"
    assert checks_dir.exists()
    assert (checks_dir / "generic.yaml").exists()
    assert (tmp_path / "dags" / "01_alf_checks.py").exists()
