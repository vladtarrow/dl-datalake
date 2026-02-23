"""Parquet reader module."""

import datetime
from pathlib import Path

import duckdb
import polars as pl


class ParquetReader:
    """Reads Parquet data using DuckDB and Polars."""

    def __init__(self, base_path: str = "data") -> None:
        """Initialize ParquetReader.

        Args:
            base_path: Root directory for data.
        """
        self.base_path = Path(base_path)

    def read_range(
        self,
        exchange: str,
        symbol: str,
        data_type: str,
        start_date: str,
        end_date: str,
    ) -> pl.DataFrame:
        """Read data using DuckDB and return a Polars DataFrame.

        Args:
            exchange: Exchange name.
            symbol: Trading symbol.
            data_type: Data type (e.g. raw, agg).
            start_date: Start date (ISO format).
            end_date: End date (ISO format).

        Returns:
            Polars DataFrame.
        """
        path_pattern = (
            self.base_path
            / exchange.upper()
            / "*"
            / symbol.upper()
            / data_type.lower()
            / "**/*.parquet"
        )

        # Use DuckDB to query parquet and export to Polars (zero-copy if possible)
        # Note: start/end dates are converted to ms

        ts_start = int(datetime.datetime.fromisoformat(start_date).timestamp() * 1000)
        ts_end = int(datetime.datetime.fromisoformat(end_date).timestamp() * 1000)

        # Use parameterized query to prevent SQL injection
        query = """
            SELECT * FROM read_parquet(?)
            WHERE ts >= ?
            AND ts <= ?
            ORDER BY ts
        """

        # DuckDB can export to arrow, which Polars reads instantly
        arrow_table = duckdb.execute(
            query,
            [str(path_pattern.as_posix()), ts_start, ts_end],
        ).to_arrow_table()  # pyright: ignore
        df = pl.from_arrow(arrow_table)  # pyright: ignore
        if isinstance(df, pl.Series):
            return df.to_frame()
        return df

    def list_symbols(self) -> list[str]:
        """List all unique symbols across exchanges and markets.

        Returns:
            List of symbol strings.
        """
        # Use sets to avoid duplicates
        symbols: set[str] = set()
        if not self.base_path.exists():
            return []

        for ex_path in self.base_path.iterdir():
            if not ex_path.is_dir():
                continue
            for mk_path in ex_path.iterdir():
                if not mk_path.is_dir():
                    continue
                for sym_path in mk_path.iterdir():
                    # Symbol is the directory name
                    symbols.add(sym_path.name)
        return list(symbols)
