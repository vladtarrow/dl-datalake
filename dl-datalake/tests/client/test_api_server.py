"""Tests for FastAPI server."""

from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from fastapi.testclient import TestClient

from dl_datalake.client.api_server import app


@pytest.fixture
def test_client():
    """Create FastAPI test client."""
    return TestClient(app)


@pytest.fixture
def mock_client():
    """Mock DataLakeClient."""
    with patch("dl_datalake.client.api_server.client") as mock:
        yield mock


def test_health_endpoint(test_client):
    """Test /health endpoint."""
    response = test_client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_list_entries(test_client, mock_client):
    """Test /list endpoint."""
    # Mock manifest entries with proper structure
    mock_entry1 = MagicMock()
    mock_entry1.id = 1
    mock_entry1.symbol = "BTCUSDT"
    mock_entry1.exchange = "BINANCE"
    mock_entry1.path = "/data/test.parquet"
    mock_entry1.type = "raw"

    mock_entry2 = MagicMock()
    mock_entry2.id = 2
    mock_entry2.symbol = "ETHUSDT"
    mock_entry2.exchange = "BYBIT"
    mock_entry2.path = "/data/eth.parquet"
    mock_entry2.type = "raw"

    mock_client.list.return_value = [mock_entry1, mock_entry2]

    response = test_client.get("/list")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert data[0]["symbol"] == "BTCUSDT"
    mock_client.list.assert_called_once()


def test_read_data_success(test_client, mock_client):
    """Test /read endpoint with valid parameters."""
    mock_df = pl.DataFrame(
        {
            "ts": [1000, 2000],
            "open": [100.0, 105.0],
            "close": [105.0, 110.0],
        },
    )
    mock_client.read_ohlc.return_value = mock_df

    response = test_client.get(
        "/read",
        params={
            "exchange": "BINANCE",
            "symbol": "BTCUSDT",
            "start": "2024-01-01T00:00:00",
            "end": "2024-01-02T00:00:00",
            "data_type": "raw",
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert data[0]["open"] == 100.0
    # Verify correct parameter names (start_date, end_date)
    mock_client.read_ohlc.assert_called_once_with(
        "BINANCE",
        "BTCUSDT",
        "2024-01-01T00:00:00",
        "2024-01-02T00:00:00",
        "raw",
    )


def test_read_data_missing_params(test_client):
    """Test /read endpoint with missing required parameters."""
    response = test_client.get("/read")

    assert response.status_code == 422  # Validation error


def test_read_data_exception_handling(test_client, mock_client):
    """Test /read endpoint error handling."""
    mock_client.read_ohlc.side_effect = Exception("Database error")

    response = test_client.get(
        "/read",
        params={
            "exchange": "BINANCE",
            "symbol": "BTCUSDT",
            "start": "2024-01-01T00:00:00",
            "end": "2024-01-02T00:00:00",
            "data_type": "raw",
        },
    )

    assert response.status_code == 500
    assert "Database error" in response.json()["detail"]
