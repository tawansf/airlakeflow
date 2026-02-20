"""Unit tests for Bronze ingestion (API and Postgres)."""
import json
from unittest.mock import MagicMock, patch

import pytest


# Patch time.sleep to avoid delaying tests
@pytest.fixture(autouse=True)
def no_sleep():
    with patch("crypto.bronze.time.sleep"):
        yield


def test_fetch_bitcoin_data_success():
    from crypto.bronze import _fetch_bitcoin_data

    payload = {"bitcoin": {"usd": 50000, "last_updated_at": 1234567890}}
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = payload

    with patch("crypto.bronze.requests.get", return_value=mock_response) as m:
        result = _fetch_bitcoin_data("http://example.com", timeout=10, max_retries=2, backoff_base=0.1)
    assert result == payload
    m.assert_called_once_with("http://example.com", timeout=10)


def test_fetch_bitcoin_data_status_not_200():
    from crypto.bronze import _fetch_bitcoin_data

    mock_response = MagicMock()
    mock_response.status_code = 500

    with patch("crypto.bronze.requests.get", return_value=mock_response):
        with pytest.raises(RuntimeError, match="status 500"):
            _fetch_bitcoin_data("http://example.com", timeout=10, max_retries=1, backoff_base=0.1)


def test_fetch_bitcoin_data_invalid_json():
    from crypto.bronze import _fetch_bitcoin_data

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.side_effect = json.JSONDecodeError("err", "doc", 0)

    with patch("crypto.bronze.requests.get", return_value=mock_response):
        with pytest.raises(RuntimeError, match="JSON válido"):
            _fetch_bitcoin_data("http://example.com", timeout=10, max_retries=1, backoff_base=0.1)


def test_bronze_ingestion_missing_url():
    import crypto.bronze as bronze_mod
    from crypto.bronze import bronze_ingestion_data_bitcoin

    with patch.object(bronze_mod, "GEEKO_URL_API", None):
        with pytest.raises(ValueError, match="GEEKO_URL_API not configured"):
            bronze_ingestion_data_bitcoin()


def test_bronze_ingestion_success():
    from crypto.bronze import bronze_ingestion_data_bitcoin

    payload = {"bitcoin": {"usd": 50000, "last_updated_at": 1234567890}}
    with patch("crypto.bronze.GEEKO_URL_API", "http://api.example.com"), patch(
        "crypto.bronze._fetch_bitcoin_data", return_value=payload
    ), patch("crypto.bronze.PostgresHook") as mock_hook:
        mock_hook.return_value.run.return_value = None
        bronze_ingestion_data_bitcoin()
    mock_hook.return_value.run.assert_called_once()
    call_args = mock_hook.return_value.run.call_args  # type: ignore[attr-defined]
    assert "INSERT INTO bronze.bitcoin_raw" in call_args[0][0]
    assert call_args[1]["parameters"]
    loaded = json.loads(call_args[1]["parameters"][0])
    assert "processed_at" in loaded
    assert loaded["bitcoin"]["usd"] == 50000
