"""Tests for alf new model: creates model file and generates migration."""

from pathlib import Path

from click.testing import CliRunner

from airlakeflow.cli import main
from airlakeflow.new_model_cmd import run_new_model


def _minimal_project(tmp_path: Path) -> Path:
    """Project with config and migrations dir (no models yet)."""
    (tmp_path / "config" / "models").mkdir(parents=True)
    (tmp_path / "dags" / "sql" / "migrations").mkdir(parents=True)
    (tmp_path / ".airlakeflow.yaml").write_text(
        "architecture: medallion\nmigration_driver: postgres\n"
    )
    return tmp_path


def test_new_model_creates_file(tmp_path):
    proj = _minimal_project(tmp_path)
    run_new_model(name="vendas", layer_name="silver", project_root=proj)
    model_file = proj / "config" / "models" / "vendas.py"
    assert model_file.exists()
    content = model_file.read_text()
    assert "VendasModel" in content
    assert '@layer("silver")' in content
    assert '__table__ = "vendas"' in content


def test_new_model_snake_case_name(tmp_path):
    proj = _minimal_project(tmp_path)
    run_new_model(name="sales-order", layer_name="gold", project_root=proj)
    # Module file should be snake_case
    assert (proj / "config" / "models" / "sales_order.py").exists()


def test_new_model_cli(tmp_path):
    proj = _minimal_project(tmp_path)
    runner = CliRunner()
    r = runner.invoke(
        main,
        ["new", "model", "produto", "-l", "silver", "-r", str(proj)],
    )
    assert r.exit_code == 0
    assert (proj / "config" / "models" / "produto.py").exists()
    # Migration should be generated (one new file)
    migrations = list((proj / "dags" / "sql" / "migrations").glob("V*.sql"))
    assert len(migrations) >= 1
    assert any("produto" in m.name or "silver" in m.name for m in migrations)


def test_new_model_generates_migration(tmp_path):
    """run_new_model creates model and triggers migration generation (migration file appears)."""
    proj = _minimal_project(tmp_path)
    run_new_model(name="item", layer_name="silver", project_root=proj)
    migrations = list((proj / "dags" / "sql" / "migrations").glob("V*.sql"))
    assert len(migrations) == 1
    assert "setup_silver_item" in migrations[0].name
    assert "CREATE TABLE" in migrations[0].read_text()
