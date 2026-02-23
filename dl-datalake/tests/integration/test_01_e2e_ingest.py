from pathlib import Path

import polars as pl


def test_csv_ingestion_end_to_end(temp_datalake, sample_csv_path):
    """
    Use Case 1: End-to-End Ingestion
    Verifies that a CSV file is correctly ingested, stored as Parquet, and registered in the manifest.
    """
    # 1. Prepare source data
    csv_path, source_df = sample_csv_path(
        filename="binance_btc_usdt.csv",
        length=100,
        start_time=1672531200000,  # 2023-01-01 00:00:00 UTC
    )

    exchange = "binance"
    market = "spot"
    symbol = "BTCUSDT"

    # 2. Run Ingestion
    pipeline = temp_datalake["pipeline"]
    success = pipeline.ingest_csv(csv_path, exchange, market, symbol)

    assert success is True

    # 3. Verify manifest entry
    manifest = temp_datalake["manifest"]
    entries = manifest.list_entries(symbol=symbol, exchange=exchange)
    assert (
        len(entries) >= 1
    )  # Might be split into multiple files if crossing days, but valid for 100 mins

    entry = entries[0]
    assert entry.symbol == symbol
    assert entry.exchange == exchange.upper()
    assert entry.path.endswith(".parquet")
    assert entry.type == "raw"

    # 4. Verify physical file
    parquet_path = Path(entry.path)
    assert parquet_path.exists()

    # 5. Verify Content Parity
    # Read back the parquet file
    result_df = pl.read_parquet(parquet_path)

    # Compare schemas (ts, open, high, low, close, volume)
    # Note: Source DF has 'ts', 'open', 'high', 'low', 'close', 'volume'
    # Result DF should have the same

    # Sort both to ensure order
    source_df_sorted = source_df.sort("ts")
    result_df_sorted = result_df.sort("ts")

    # Check simple equality for key columns
    assert result_df_sorted.height == source_df_sorted.height
    assert result_df_sorted["ts"].equals(source_df_sorted["ts"])
    assert result_df_sorted["close"].equals(source_df_sorted["close"])
