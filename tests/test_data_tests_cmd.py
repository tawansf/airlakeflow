from pathlib import Path

from airlakeflow.data_tests_cmd import create_alf_check_file, run_data_tests_cmd


def test_run_data_tests_cmd_creates_structure(tmp_path: Path):
    run_data_tests_cmd(tmp_path)
    checks_dir = tmp_path / "config" / "checks"
    assert checks_dir.exists()
    assert (checks_dir / "generic.yaml").exists()
    for layer in ("bronze", "silver", "gold"):
        layer_dir = checks_dir / layer
        assert layer_dir.exists()
        assert (layer_dir / ".gitkeep").exists()
    assert (tmp_path / "dags" / "01_alf_checks.py").exists()


def test_create_alf_check_file_creates_file(tmp_path: Path):
    run_data_tests_cmd(tmp_path)
    out = create_alf_check_file(tmp_path, "bronze", "test_table")
    assert out.exists()
    content = out.read_text(encoding="utf-8")
    assert "checks:" in content
