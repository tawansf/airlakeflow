"""Tests for create_env_from_example."""

from airlakeflow.docker_cmd import create_env_from_example


def test_create_env_does_nothing_if_env_exists(tmp_path):
    (tmp_path / ".env").write_text("AIRFLOW_UID=1000")
    (tmp_path / ".env.example").write_text("AIRFLOW_UID=1000\nDATAWAREHOUSE_DB=db")
    assert create_env_from_example(tmp_path) is False
    assert (tmp_path / ".env").read_text() == "AIRFLOW_UID=1000"


def test_create_env_does_nothing_if_no_example(tmp_path):
    assert create_env_from_example(tmp_path) is False
    assert not (tmp_path / ".env").exists()


def test_create_env_creates_env_from_example(tmp_path):
    (tmp_path / ".env.example").write_text(
        "HOSTNAME=airflow\nAIRFLOW_UID=1000\nDATAWAREHOUSE_DB=datawarehouse"
    )
    assert create_env_from_example(tmp_path) is True
    assert (tmp_path / ".env").exists()
    content = (tmp_path / ".env").read_text()
    assert "AIRFLOW_UID=" in content
    assert "DATAWAREHOUSE_DB=datawarehouse" in content


def test_create_env_adds_postgres_port_if_missing(tmp_path):
    (tmp_path / ".env.example").write_text("AIRFLOW_UID=1000\nDATAWAREHOUSE_DB=db")
    assert create_env_from_example(tmp_path) is True
    content = (tmp_path / ".env").read_text()
    assert "POSTGRES_HOST_PORT=" in content
