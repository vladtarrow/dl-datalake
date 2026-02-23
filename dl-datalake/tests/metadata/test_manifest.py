"""Tests for ManifestManager."""

import pytest

from dl_datalake.metadata.manifest import ManifestManager


@pytest.fixture
def manifest(tmp_path):
    """Create ManifestManager with temp database."""
    db_path = tmp_path / "test_manifest.db"
    mgr = ManifestManager(db_path=str(db_path))
    mgr.db_path = str(db_path)  # Store for later use
    return mgr


def test_init_creates_database(tmp_path):
    """Test that database and tables are created."""
    db_path = tmp_path / "test.db"
    manager = ManifestManager(db_path=str(db_path))

    assert db_path.exists()
    # Verify we can query the table
    entries = manager.list_entries()
    assert entries == []


def test_add_entry_success(manifest):
    """Test adding a manifest entry."""
    entry_id = manifest.add_entry(
        exchange="BINANCE",
        market="FUTURES",
        symbol="BTCUSDT",
        path="/data/test.parquet",
        type="raw",
        time_from=1000000,
        time_to=2000000,
    )

    assert isinstance(entry_id, int)
    assert entry_id > 0


def test_add_entry_with_checksum(manifest):
    """Test adding entry with checksum and metadata."""
    manifest.add_entry(
        exchange="BINANCE",
        market="FUTURES",
        symbol="BTCUSDT",
        path="/data/test.parquet",
        type="raw",
        time_from=1000000,
        time_to=2000000,
        checksum="abc123",
        metadata_json='{"key": "value"}',
    )

    entries = manifest.list_entries()
    assert len(entries) == 1
    assert entries[0].checksum == "abc123"
    assert entries[0].metadata_json == '{"key": "value"}'


def test_add_entry_returns_id(manifest):
    """Test that add_entry returns the generated ID."""
    id1 = manifest.add_entry(
        exchange="BINANCE",
        market="FUTURES",
        symbol="BTCUSDT",
        path="/data/test1.parquet",
        type="raw",
        time_from=1000000,
        time_to=2000000,
    )

    id2 = manifest.add_entry(
        exchange="BINANCE",
        market="FUTURES",
        symbol="ETHUSDT",
        path="/data/test2.parquet",
        type="raw",
        time_from=1000000,
        time_to=2000000,
    )

    assert id1 != id2
    assert id2 > id1


def test_list_entries(manifest):
    """Test listing all entries."""
    # Add multiple entries
    manifest.add_entry(
        exchange="BINANCE",
        market="FUTURES",
        symbol="BTCUSDT",
        path="/data/btc.parquet",
        type="raw",
        time_from=1000000,
        time_to=2000000,
    )

    manifest.add_entry(
        exchange="BYBIT",
        market="SPOT",
        symbol="ETHUSDT",
        path="/data/eth.parquet",
        type="agg",
        time_from=3000000,
        time_to=4000000,
    )

    entries = manifest.list_entries()

    assert len(entries) == 2
    assert entries[0].exchange == "BINANCE"
    assert entries[1].exchange == "BYBIT"


def test_list_entries_empty(manifest):
    """Test listing entries when database is empty."""
    entries = manifest.list_entries()
    assert entries == []


def test_delete_entry_by_path(manifest):
    """Test deleting entries by symbol."""
    manifest.add_entry(
        exchange="BINANCE",
        market="FUTURES",
        symbol="BTCUSDT",
        path="/data/test.parquet",
        type="raw",
        time_from=1000000,
        time_to=2000000,
    )

    # Verify entry exists
    assert len(manifest.list_entries()) == 1

    # Delete entry using delete_entries method
    deleted_paths = manifest.delete_entries("BTCUSDT")

    assert len(deleted_paths) == 1
    assert len(manifest.list_entries()) == 0


def test_delete_entry_not_found(manifest):
    """Test deleting a non-existent entry."""
    deleted_paths = manifest.delete_entries("NONEXISTENT")
    assert len(deleted_paths) == 0


def test_session_management(manifest):
    """Test that sessions are properly managed (commit/rollback)."""
    # Add entry in first session
    entry_id = manifest.add_entry(
        exchange="BINANCE",
        market="FUTURES",
        symbol="BTCUSDT",
        path="/data/test.parquet",
        type="raw",
        time_from=1000000,
        time_to=2000000,
    )

    # Create new instance pointing to same DB
    manifest2 = ManifestManager(db_path=manifest.db_path)

    # Should see the entry (session was committed)
    entries = manifest2.list_entries()
    assert len(entries) == 1
    assert entries[0].id == entry_id


def test_list_entries_preserves_order(manifest):
    """Test that entries are listed in insertion order (id order)."""
    id1 = manifest.add_entry(
        exchange="BINANCE",
        market="FUTURES",
        symbol="BTCUSDT",
        path="/data/1.parquet",
        type="raw",
        time_from=1000000,
        time_to=2000000,
    )

    id2 = manifest.add_entry(
        exchange="BINANCE",
        market="FUTURES",
        symbol="ETHUSDT",
        path="/data/2.parquet",
        type="raw",
        time_from=1000000,
        time_to=2000000,
    )

    entries = manifest.list_entries()

    assert entries[0].id == id1
    assert entries[1].id == id2
