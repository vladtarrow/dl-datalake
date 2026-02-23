"""Tests for FeatureStore."""

from unittest.mock import MagicMock

import pytest

from dl_datalake.features.manager import FeatureStore


@pytest.fixture
def feature_store(tmp_path):
    """Create FeatureStore with temp paths."""
    return FeatureStore(
        base_path=str(tmp_path / "data"),
        db_path=str(tmp_path / "manifest.db"),
    )


def test_upload_feature(feature_store, tmp_path):
    """Test uploading a feature file."""
    # Create source feature file
    source_file = tmp_path / "source_features.parquet"
    source_file.write_text("feature data")

    feature_store.manifest.add_entry = MagicMock(return_value=1)

    # Don't mock copy2 - let it actually copy the file
    # so checksum calculation works
    result = feature_store.upload_feature(
        src_path=str(source_file),
        exchange="BINANCE",
        market="SPOT",
        symbol="BTCUSDT",
        feature_set="my_features",
        version="1.0.0",
    )

    # Returns version
    assert result == "1.0.0"
    # Verify manifest entry was added
    assert feature_store.manifest.add_entry.called


def test_upload_calculates_checksum(feature_store, tmp_path):
    """Test that upload calculates checksum."""
    source_file = tmp_path / "features.parquet"
    source_file.write_bytes(b"test content")

    feature_store.manifest.add_entry = MagicMock(return_value=1)

    # Need to create actual destination directory for checksum calculation
    dest_dir = feature_store.base_path / "features" / "test_features" / "1.0.0"
    dest_dir.mkdir(parents=True, exist_ok=True)

    feature_store.upload_feature(
        src_path=str(source_file),
        exchange="BINANCE",
        market="SPOT",
        symbol="BTCUSDT",
        feature_set="test_features",
        version="1.0.0",
    )

    # Verify checksum was calculated and passed to manifest
    assert feature_store.manifest.add_entry.called
    call_kwargs = feature_store.manifest.add_entry.call_args[1]
    assert "checksum" in call_kwargs
    assert call_kwargs["checksum"] is not None


def test_get_feature_path(feature_store):
    """Test feature path generation."""
    path = feature_store._get_feature_path("my_features", "1.0.0")

    assert path.exists()
    assert "features" in str(path)
    assert "my_features" in str(path)
    assert "1.0.0" in str(path)


def test_upload_nonexistent_file(feature_store):
    """Test uploading non-existent file raises error."""
    with pytest.raises(FileNotFoundError):
        feature_store.upload_feature(
            src_path="/nonexistent/file.parquet",
            exchange="BINANCE",
            market="SPOT",
            symbol="BTCUSDT",
            feature_set="test",
            version="1.0.0",
        )
