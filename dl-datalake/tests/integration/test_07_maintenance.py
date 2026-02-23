from datetime import date
from pathlib import Path

import polars as pl
import pytest


def test_schema_evolution(temp_datalake, sample_ohlc_generator):
    """
    Use Case 7.1: Schema Evolution
    Verifies that we can read data even if schema changes (new columns added).
    """

    writer = temp_datalake["writer"]
    exchange = "EVO"
    market = "SPOT"
    symbol = "BTC"

    # Batch 1: Standard OHLCV
    df1 = sample_ohlc_generator(length=10)
    writer.write_table(
        df1, exchange, market, symbol, "raw", "1min", date=date(2023, 1, 1),
    )

    # Batch 2: Added 'vwap' column
    df2 = sample_ohlc_generator(length=10)
    df2 = df2.with_columns(pl.lit(100.0).alias("vwap"))
    writer.write_table(
        df2, exchange, market, symbol, "raw", "1min", date=date(2023, 1, 2),
    )

    # Try the Manifest listing to find files
    # (Since we manually used writer, manifest isn't updated, but we can list paths directly from disk for this test)
    data_root = temp_datalake["data_root"]
    files = list(data_root.rglob("*.parquet"))
    assert len(files) == 2

    # Read both with Polars scanning
    # Polars scan_parquet handles schema evolution (diagonal concatenation) if configured or compatible
    # By default read_parquet on list might fail if schemas differ, but let's see.
    # We want to emulate a query that reads a date range covering both.

    # Read both with Polars scanning
    # To properly handle schema evolution (files with different schemas), we need to scan them individually
    # and use diagonal concatenation.
    lfs = [pl.scan_parquet(str(f)) for f in files]

    try:
        # Use diagonal concat to merge schemas (union of columns, nulls for missing)
        df_combined = pl.concat(lfs, how="diagonal").collect()

        # Verify both columns exist (filled with nulls where missing)
        assert "vwap" in df_combined.columns
        assert df_combined.height == 20

        # Verify nulls in first batch (first 10 rows)
        # Note: Order is not guaranteed without sort, but we can check null count
        assert df_combined["vwap"].null_count() == 10

    except Exception as e:  # noqa: BLE001
        pytest.fail(f"Schema evolution read failed: {e}")




def test_cleanup_policy(temp_datalake, sample_csv_path):
    """
    Use Case 7.3: Cleanup
    Verifies process of deleting entries and files.
    """
    pipeline = temp_datalake["pipeline"]
    manifest = temp_datalake["manifest"]

    # Ingest data
    path, _ = sample_csv_path()
    pipeline.ingest_csv(path, "DEL", "SPOT", "SYM")

    entries = manifest.list_entries(symbol="SYM")
    assert len(entries) > 0
    file_path = Path(entries[0].path)
    assert file_path.exists()

    # Perform cleanup:
    # 1. Get paths to delete from manifest
    paths_to_delete = manifest.delete_entries(symbol="SYM")

    # 2. Physical deletion (simulate what a cleanup script would do)
    for p in paths_to_delete:
        Path(p).unlink(missing_ok=True)

    # Verify
    assert len(manifest.list_entries(symbol="SYM")) == 0
    assert not file_path.exists()
