"""Tests for DataLakeClient."""

from unittest.mock import MagicMock

import pytest

from dl_datalake.client.dl_client import DataLakeClient


@pytest.fixture
def client(tmp_path):
    """Create DataLakeClient with temp paths."""
    return DataLakeClient(
        data_root=str(tmp_path / "data"),
        db_path=str(tmp_path / "manifest.db"),
    )


def test_init(tmp_path):
    """Test DataLakeClient initialization."""
    client = DataLakeClient(
        data_root=str(tmp_path / "data"),
        db_path=str(tmp_path / "manifest.db"),
    )

    assert client.reader is not None
    assert client.manifest is not None
    assert client.features is not None


def test_read_ohlc(client):
    """Test reading OHLC via facade."""
    # Mock reader
    client.reader.read_range = MagicMock()

    client.read_ohlc(
        exchange="BINANCE",
        symbol="BTCUSDT",
        start_date="2024-01-01T00:00:00",
        end_date="2024-01-02T00:00:00",
        data_type="raw",
    )

    client.reader.read_range.assert_called_once_with(
        "BINANCE",
        "BTCUSDT",
        "raw",
        "2024-01-01T00:00:00",
        "2024-01-02T00:00:00",
    )


def test_list_manifest(client):
    """Test listing via facade."""
    client.manifest.list_entries = MagicMock(return_value=[{"id": 1}])

    entries = client.list(symbol="BTCUSDT")

    assert entries == [{"id": 1}]
    client.manifest.list_entries.assert_called_once_with(
        symbol="BTCUSDT",
        data_type=None,
        exchange=None,
        market=None,
    )


def test_upload_features(client, tmp_path):
    """Test uploading features via facade."""
    # Create dummy feature file
    feature_file = tmp_path / "features.parquet"
    feature_file.write_text("dummy")

    client.features.upload_feature = MagicMock(return_value="1.0.0")

    client.upload_features(
        file_path=str(feature_file),
        exchange="BINANCE",
        market="SPOT",
        symbol="BTCUSDT",
        feature_set="my_features",
        version="1.0.0",
    )

    client.features.upload_feature.assert_called_once_with(
        str(feature_file),
        "BINANCE",
        "SPOT",
        "BTCUSDT",
        "my_features",
        "1.0.0",
    )
