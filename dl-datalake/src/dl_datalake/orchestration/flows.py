"""Prefect flows for data ingestion and aggregation."""

from datetime import UTC, datetime, timedelta

import polars as pl
from prefect import flow, task

from dl_datalake.ingest.pipeline import IngestPipeline
from dl_datalake.metadata.manifest import ManifestManager
from dl_datalake.storage.reader import ParquetReader
from dl_datalake.storage.writer import ParquetWriter


@task(retries=3)
def ingest_csv_task(file_path: str, exchange: str, market: str, symbol: str) -> bool:
    """Task to ingest a CSV file.

    Args:
        file_path: Path to the CSV file.
        exchange: Exchange name.
        market: Market type.
        symbol: Trading symbol.

    Returns:
        True if successful.
    """
    pipeline = IngestPipeline()
    return pipeline.ingest_csv(file_path, exchange, market, symbol)


@task
def aggregate_ohlc_task(
    exchange: str,
    symbol: str,
    source_tf: str,
    target_tf: str,
) -> None:
    """Polars-based resample aggregation.

    Args:
        exchange: Exchange name.
        symbol: Trading symbol.
        source_tf: Source timeframe (e.g. 1min). Unused logic-wise but kept for flow.
        target_tf: Target timeframe (e.g. 15min).
    """
    _ = source_tf  # Mark as used to satisfy linter
    reader = ParquetReader()
    writer = ParquetWriter()
    manifest = ManifestManager()

    # For now, let's take a range (e.g., last day)
    # Simple logic to demonstrate the pipeline
    end_date = datetime.now(UTC)
    start_date = end_date - timedelta(days=7)

    df = reader.read_range(
        exchange,
        symbol,
        "raw",
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    )

    if df.is_empty():
        return

    # Polars Upsampling/Grouping is extremely fast
    # Convert 'ts' to datetime for grouping
    df = df.with_columns(  # pyright: ignore
        pl.from_epoch("ts", time_unit="ms").alias("datetime"),
    )

    # Map target timeframe to polars duration (e.g., 15min -> 15m)
    duration = target_tf.replace("min", "m").replace("h", "h")

    agg_df = (
        df.group_by_dynamic("datetime", every=duration)  # pyright: ignore
        .agg(  # pyright: ignore
            [
                pl.col("open").first(),
                pl.col("high").max(),
                pl.col("low").min(),
                pl.col("close").last(),
                pl.col("volume").sum(),
                pl.col("ts").first(),
            ],
        )
        .drop("datetime")
        .drop_nulls()
    )

    written_paths = list(writer.write_ohlc(agg_df, exchange, "agg", symbol, target_tf))

    for path in written_paths:
        manifest.add_entry(
            exchange=exchange,
            market="agg",
            symbol=symbol,
            path=path,
            type="agg",
            time_from=int(agg_df["ts"].min()),  # pyright: ignore
            time_to=int(agg_df["ts"].max()),  # pyright: ignore
            metadata_json=f'{{"timeframe": "{target_tf}"}}',
        )


@flow(name="Ingest and Aggregate")
def ingest_pipeline_flow(
    file_path: str,
    exchange: str,
    market: str,
    symbol: str,
) -> None:
    """Main flow to ingest and aggregate data.

    Args:
        file_path: Path to CSV.
        exchange: Exchange name.
        market: Market name.
        symbol: Symbol name.
    """
    _ = ingest_csv_task(file_path, exchange, market, symbol)
    aggregate_ohlc_task(exchange, symbol, "1min", "15min")
    aggregate_ohlc_task(exchange, symbol, "1min", "1h")


@flow(name="Daily Maintenance")
def daily_offload_flow() -> None:
    """Run daily maintenance tasks."""
    # Placeholder for offload/compaction logic
