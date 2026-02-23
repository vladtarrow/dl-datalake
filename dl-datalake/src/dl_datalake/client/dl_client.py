"""Data Lake Client SDK."""

from typing import Any

import polars as pl

from dl_datalake.features.manager import FeatureStore
from dl_datalake.metadata.manifest import ManifestManager
from dl_datalake.storage.reader import ParquetReader


class DataLakeClient:
    """Client for interacting with the Data Lake."""

    def __init__(self, data_root: str = "data", db_path: str = "manifest.db") -> None:
        """Initialize DataLakeClient.

        Args:
            data_root: Root directory.
            db_path: Path to manifest DB.
        """
        self.reader = ParquetReader(base_path=data_root)
        self.manifest = ManifestManager(db_path=db_path)
        self.features = FeatureStore(base_path=data_root, db_path=db_path)

    def list(
        self,
        symbol: str | None = None,
        data_type: str | None = None,
        exchange: str | None = None,
        market: str | None = None,
    ) -> list[Any]:
        """List entries in the manifest.

        Args:
            symbol: Filter by symbol.
            data_type: Filter by data type.
            exchange: Filter by exchange.
            market: Filter by market type.

        Returns:
            List of manifest entries.
        """
        return self.manifest.list_entries(
            symbol=symbol,
            data_type=data_type,
            exchange=exchange,
            market=market,
        )

    def read_ohlc(
        self,
        exchange: str,
        symbol: str,
        start_date: str,
        end_date: str,
        data_type: str = "raw",
    ) -> pl.DataFrame:
        """Read OHLC data.

        Args:
            exchange: Exchange name.
            symbol: Symbol.
            start_date: Start date (ISO).
            end_date: End date (ISO).
            data_type: Data type.

        Returns:
            Polars DataFrame.
        """
        return self.reader.read_range(exchange, symbol, data_type, start_date, end_date)

    def upload_features(  # noqa: PLR0913
        self,
        file_path: str,
        exchange: str,
        market: str,
        symbol: str,
        feature_set: str,
        version: str = "1.0.0",
    ) -> str:
        """Upload a feature file.

        Args:
            file_path: Path to feature file.
            exchange: Exchange name.
            market: Market type.
            symbol: Symbol.
            feature_set: Name of feature set.
            version: Version string.

        Returns:
            Destination path.
        """
        return self.features.upload_feature(
            file_path,
            exchange,
            market,
            symbol,
            feature_set,
            version,
        )
