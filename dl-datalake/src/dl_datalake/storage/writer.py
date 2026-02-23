"""Parquet writer module."""

import datetime
from collections.abc import Generator
from pathlib import Path

import polars as pl


class ParquetWriter:
    """Writes data to Parquet files partitioned by date."""

    def __init__(self, base_path: str = "data") -> None:
        """Initialize ParquetWriter.

        Args:
            base_path: Root directory for data storage.
        """
        self.base_path = Path(base_path)

    def _get_partition_path(
        self,
        exchange: str,
        market: str,
        symbol: str,
        data_type: str,
        period: str,
        date: datetime.date,
    ) -> Path:
        # Sanitize symbol for file system (especially for Windows ':' and '/')
        safe_symbol = (
            symbol.upper().replace("/", "_").replace(":", "_").replace(" ", "_")
        )

        path = (
            self.base_path
            / exchange.upper()
            / market.upper()
            / safe_symbol
            / data_type.lower()
            / period.lower()
            / date.strftime("%Y")
            / date.strftime("%m")
        )
        path.mkdir(parents=True, exist_ok=True)
        return path


    def write_table(
        self,
        df: pl.DataFrame,
        exchange: str,
        market: str,
        symbol: str,
        data_type: str,
        period: str,
        date: datetime.date,
    ) -> tuple[Path, int, int]:
        """Generic table writer using Polars with UPSERT logic.

        If the file already exists, it merges new data with existing data,
        removes duplicates, and sorts by timestamp.

        Returns:
            Tuple of (Path, time_min, time_max).
        """
        path = self._get_partition_path(
            exchange,
            market,
            symbol,
            data_type,
            period,
            date,
        )
        safe_symbol = (
            symbol.upper().replace("/", "_").replace(":", "_").replace(" ", "_")
        )
        # Use YYYYMM format for monthly files
        filename = f"{safe_symbol}_{period}_{date.strftime('%Y%m')}.parquet"
        full_path = path / filename

        # --- Upsert/Merge Logic ---
        if full_path.exists():
            existing_df = pl.read_parquet(str(full_path))
            # Merge and deduplicate
            df = pl.concat([existing_df, df])
            
            # Determine timestamp column
            ts_col = "ts" if "ts" in df.columns else "timestamp"
            
            # Deduplicate (keep latest) and sort
            df = df.unique(subset=[ts_col], keep="last").sort(ts_col)

        # Calculate time range from FINAL merged dataframe
        t_min, t_max = 0, 0
        if "ts" in df.columns:
            t_min = int(df["ts"].min())  # pyright: ignore
            t_max = int(df["ts"].max())  # pyright: ignore
        elif "timestamp" in df.columns:
            t_min = int(df["timestamp"].min())  # pyright: ignore
            t_max = int(df["timestamp"].max())  # pyright: ignore

        # Use atomic write: write to temp file, then rename
        temp_path = full_path.with_suffix(".parquet.tmp")
        df.write_parquet(temp_path, compression="snappy")
        temp_path.replace(full_path)

        # --- Data Integrity Validation ---
        def raise_integrity_error(msg: str) -> None:
            raise RuntimeError(msg)

        try:
            # 1. Row count verification
            verify_df = pl.read_parquet(str(full_path))
            if len(verify_df) != len(df):
                msg = (
                    f"Integrity check failed: original df has {len(df)} rows, "
                    f"but written file {full_path.name} has {len(verify_df)} rows."
                )
                raise_integrity_error(msg)

            # 2. Sequential integrity (no backward-jumps in time)
            ts_col = "ts" if "ts" in verify_df.columns else "timestamp"
            if not verify_df[ts_col].is_sorted():
                msg = f"Integrity check failed: timestamps in {full_path.name} are not sorted."
                raise_integrity_error(msg)

        except Exception as e:
            if isinstance(e, RuntimeError):
                raise
            msg = f"Failed to verify written file {full_path}: {e}"
            raise RuntimeError(msg) from e

        return full_path, t_min, t_max


    def write_ohlc(
        self,
        df: pl.DataFrame,
        exchange: str,
        market: str,
        symbol: str,
        period: str = "1min",
    ) -> Generator[tuple[Path, int, int], None, None]:
        """Write OHLC data partitioned by day.

        Expects Polars DataFrame with 'ts' column (timestamp in ms).

        Args:
            df: DataFrame with OHLC data.
            exchange: Exchange name.
            market: Market type.
            symbol: Trading symbol.
            period: Candle period.

        Yields:
             Paths to written files.

        Raises:
            ValueError: If 'ts' column is missing.
        """
        if "ts" not in df.columns:
            msg = "DataFrame must have 'ts' column"
            raise ValueError(msg)

        # Add a month column for partitioning
        df_with_month = df.with_columns(  # pyright: ignore
            pl.from_epoch("ts", time_unit="ms").dt.truncate("1mo").dt.date().alias("month"),
        )

        # Partition by month
        for (month,), month_df in df_with_month.partition_by(
            "month",
            as_dict=True,
            include_key=False,
        ).items():
            if not isinstance(month, datetime.date):
                continue
            yield self.write_table(
                month_df,
                exchange,
                market,
                symbol,
                "raw",
                period,
                month,
            )


    def write_ticks(
        self,
        df: pl.DataFrame,
        exchange: str,
        market: str,
        symbol: str,
    ) -> Generator[tuple[Path, int, int], None, None]:
        """Write tick data partitioned by day.

        Args:
            df: DataFrame with tick data.
            exchange: Exchange name.
            market: Market type.
            symbol: Trading symbol.

        Yields:
            Paths to written files.
        """
        # Partition by month
        df_with_month = df.with_columns(  # pyright: ignore
            pl.from_epoch("ts", time_unit="ms").dt.truncate("1mo").dt.date().alias("month"),
        )

        # Partition by month
        partitions = df_with_month.partition_by(  # pyright: ignore
            "month",
            include_key=False,
            as_dict=True,
        )

        for (month_val,), month_df in partitions.items():  # pyright: ignore
            if not isinstance(month_val, datetime.date):
                continue

            yield self.write_table(
                month_df,
                exchange,
                market,
                symbol,
                "ticks",
                "tick",
                month_val,  # pyright: ignore
            )

