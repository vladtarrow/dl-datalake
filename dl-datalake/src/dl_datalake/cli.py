"""Data Lake CLI."""

from pathlib import Path

import typer

from .ingest.pipeline import IngestPipeline
from .metadata.manifest import ManifestManager

app = typer.Typer(help="Data Lake CLI")


@app.command()
def init(db_path: str = "manifest.db") -> None:
    """Initialize the manifest database.

    Args:
        db_path: Path to manifest DB.
    """
    _ = ManifestManager(db_path=db_path)
    typer.echo(f"Initialized manifest at {db_path}")


@app.command()
def download_symbols(
    exchange: str = "binance",
    futures: bool = True,
) -> None:
    """Download and list all active symbols from an exchange.

    Args:
        exchange: Exchange name.
        futures: Use futures market.
    """
    from .ingest.exchange_connector import ExchangeConnector

    connector = ExchangeConnector(
        exchange_id=exchange,
        market_type="future" if futures else "spot",
    )
    symbols = connector.get_all_symbols()
    for s in symbols:
        typer.echo(s)
    typer.echo(f"Total symbols on {exchange.upper()}: {len(symbols)}")


@app.command()
def download_history(
    symbol: str,
    exchange: str = "binance",
    days: int = 7,
    futures: bool = True,
    timeframe: str = "1m",
    funding: bool = False,
) -> None:
    """Download history from an exchange and save to lake.

    Args:
        symbol: Symbol to download.
        exchange: Exchange name.
        days: Number of days.
        futures: Use futures market.
        timeframe: Candle timeframe.
        funding: Download funding rates too.
    """
    from .ingest.exchange_connector import ExchangeConnector

    connector = ExchangeConnector(
        exchange_id=exchange,
        market_type="future" if futures else "spot",
    )

    typer.echo(f"Downloading {symbol} from {exchange.upper()} OHLCV...")
    count = connector.download_ohlcv(symbol, timeframe, days)
    typer.echo(f"Saved {count} candles.")

    if funding and futures:
        typer.echo(f"Downloading {symbol} funding rates...")
        f_count = connector.download_funding_rates(symbol, days)
        typer.echo(f"Saved {f_count} funding records.")


@app.command()
def delete(
    symbol: str,
    data_type: str | None = None,
    db_path: str = "manifest.db",
) -> None:
    """Delete data for a symbol from disk and manifest.

    Args:
        symbol: Symbol to delete.
        data_type: Optional data type.
        db_path: Path to manifest DB.
    """
    manager = ManifestManager(db_path=db_path)
    typer.echo(f"Searching for {symbol} data to delete...")
    paths = manager.delete_entries(symbol=symbol, data_type=data_type)

    if not paths:
        typer.echo("No entries found.")
        return

    count = 0
    for path in paths:
        path_obj = Path(path)
        if path_obj.exists():
            path_obj.unlink()
            count += 1
            # Try to remove empty parent directories
            try:
                # remove empty dirs up to data root
                path_obj.parent.rmdir()
            except OSError as e:
                # Ignore "Directory not empty" or "Directory doesn't exist"
                # errno.ENOTEMPTY and winerror 145 (ERROR_DIR_NOT_EMPTY)
                # Check both errno and winerror for cross-platform compatibility
                import errno

                error_dir_not_empty_win = 145  # Windows-specific error code

                is_not_empty = (
                    e.errno == errno.ENOTEMPTY if hasattr(errno, "ENOTEMPTY") else False
                ) or (getattr(e, "winerror", None) == error_dir_not_empty_win)
                is_not_exist = e.errno == errno.ENOENT

                if is_not_empty or is_not_exist:
                    pass
                else:
                    # Log warning for other errors (e.g. permission denied)
                    typer.echo(
                        f"Warning: Could not remove directory {path_obj.parent}: {e}",
                    )

    typer.echo(f"Deleted {count} files and cleared manifest entries.")


@app.command()
def ingest(
    source: str,
    exchange: str,
    market: str = "futures",
    symbol: str = "BTCUSDT",
    data_root: str = "data",
    db_path: str = "manifest.db",
) -> None:
    """Ingest a CSV file into the Data Lake.

    Args:
        source: Source CSV file.
        exchange: Exchange name.
        market: Market name.
        symbol: Symbol name.
        data_root: Root data dir.
        db_path: Path to manifest DB.
    """
    pipeline = IngestPipeline(data_root=data_root, db_path=db_path)
    typer.echo(f"Ingesting {source}...")
    _ = pipeline.ingest_csv(source, exchange, market, symbol)
    typer.echo("Ingestion complete.")


@app.command()
def upload_feature(
    path: str,
    exchange: str,
    market: str,
    symbol: str,
    feature_set: str,
    version: str = "1.0.0",
    data_root: str = "data",
    db_path: str = "manifest.db",
) -> None:
    """Register a feature file.

    Args:
        path: Path to feature file.
        exchange: Exchange name.
        market: Market type (spot, futures, etc.).
        symbol: Symbol.
        feature_set: Feature set name.
        version: Version string.
        data_root: Data root dir.
        db_path: Path to manifest DB.
    """
    from .features.manager import FeatureStore

    store = FeatureStore(base_path=data_root, db_path=db_path)
    typer.echo(f"Uploading {path} to feature store...")
    dest = store.upload_feature(path, exchange, market, symbol, feature_set, version)
    typer.echo(f"Registered at {dest}")


@app.command()
def read(
    exchange: str,
    symbol: str,
    start: str,
    end: str,
    data_type: str = "raw",
    data_root: str = "data",
) -> None:
    """Read data and show head.

    Args:
        exchange: Exchange name.
        symbol: Symbol.
        start: Start date.
        end: End date.
        data_type: Data type.
        data_root: Data root dir.
    """
    from .storage.reader import ParquetReader

    reader = ParquetReader(base_path=data_root)
    df = reader.read_range(exchange, symbol, data_type, start, end)
    typer.echo(df.head())
    typer.echo(f"Total rows: {len(df)}")


if __name__ == "__main__":
    app()
