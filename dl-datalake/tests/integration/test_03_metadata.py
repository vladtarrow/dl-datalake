
from dl_datalake.metadata.manifest import ManifestManager


def test_manifest_crud_and_cold_start(temp_datalake):
    """
    Use Case 3: Metadata Interaction & Use Case 5: Cold Start
    Verifies DB creation, persistence, and CRUD operations.
    """
    manifest = temp_datalake["manifest"]

    # 1. Create
    entry_id = manifest.add_entry(
        exchange="BINANCE",
        market="SPOT",
        symbol="BTCUSDT",
        path="/tmp/data/file.parquet",  # noqa: S108
        type="raw",
        version="1.0.0",
        checksum="abc123hash",
    )
    assert entry_id is not None

    # 2. Read
    results = manifest.list_entries(symbol="BTCUSDT")
    assert len(results) == 1
    assert results[0].checksum == "abc123hash"

    # 3. Cold Start Simulation
    # Re-instantiate ManifestManager pointing to SAME db_path
    db_path = temp_datalake["db_path"]

    new_manifest = ManifestManager(db_path=str(db_path))

    # Verify data persists
    results_after_restart = new_manifest.list_entries(symbol="BTCUSDT")
    assert len(results_after_restart) == 1
    assert results_after_restart[0].id == entry_id

    # 4. Delete
    deleted_paths = new_manifest.delete_entries(symbol="BTCUSDT")
    assert len(deleted_paths) == 1
    assert deleted_paths[0] == "/tmp/data/file.parquet"  # noqa: S108

    # Verify empty
    assert len(new_manifest.list_entries(symbol="BTCUSDT")) == 0
