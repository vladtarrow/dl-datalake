"""Data ingestion pipeline."""

import hashlib
import json
from pathlib import Path

import polars as pl

from dl_datalake.metadata.manifest import ManifestManager
from dl_datalake.storage.writer import ParquetWriter


class IngestPipeline:
    """Orchestrates data ingestion."""

    def __init__(self, data_root: str = "data", db_path: str = "manifest.db") -> None:
        """Initialize pipeline.

        Args:
            data_root: Root directory for data.
            db_path: Path to manifest database.
        """
        self.writer = ParquetWriter(base_path=data_root)
        self.manifest = ManifestManager(db_path=db_path)

    def _calculate_checksum(self, file_path: Path | str) -> str:
        sha256_hash = hashlib.sha256()
        with Path(file_path).open("rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def ingest_csv(
        self,
        file_path: str,
        exchange: str,
        market: str,
        symbol: str,
    ) -> bool:
        """Ingest CSV using Polars' fast multi-threaded reader.

        Args:
            file_path: Source CSV path.
            exchange: Exchange name.
            market: Market name.
            symbol: Symbol.

        Returns:
            True if successful.
        """
        # Polars scan_csv/read_csv is significantly faster than pandas
        df = pl.read_csv(file_path)

        # Simple column mapping if needed
        if "ts" not in df.columns:
            # Fallback for standard Binance files if headers are missing
            expected_cols = ["ts", "open", "high", "low", "close", "volume"]
            if len(df.columns) == len(expected_cols):
                df = df.rename(dict(zip(df.columns, expected_cols, strict=False)))
            else:
                msg = (
                    f"CSV missing 'ts' column and column count {len(df.columns)} "
                    f"does not match expected default format {expected_cols}"
                )
                raise ValueError(msg)

        written_data = list(self.writer.write_ohlc(df, exchange, market, symbol))

        for path, t_min, t_max in written_data:
            self.manifest.add_entry(
                exchange=exchange,
                market=market,
                symbol=symbol,
                path=path,
                type="raw",
                time_from=t_min,
                time_to=t_max,
                checksum=self._calculate_checksum(path),
                metadata_json=json.dumps(
                    {"timeframe": "1m"},
                ),  # timeframe should be passed or detected
            )
        return True

    def ingest_ticks_csv(
        self,
        file_path: str,
        exchange: str,
        market: str,
        symbol: str,
    ) -> bool:
        """Ingest tick CSV using Polars.

        Args:
            file_path: Source CSV.
            exchange: Exchange name.
            market: Market Name.
            symbol: Symbol.

        Returns:
            True if successful.
        """
        df = pl.read_csv(file_path)
        written_data = list(self.writer.write_ticks(df, exchange, market, symbol))

        for path, t_min, t_max in written_data:
            self.manifest.add_entry(
                exchange=exchange,
                market=market,
                symbol=symbol,
                path=path,
                type="ticks",
                time_from=t_min,
                time_to=t_max,
                checksum=self._calculate_checksum(path),
                metadata_json=json.dumps({"timeframe": "tick"}),
            )
        return True

    def verify_integrity(
        self,
        exchange: str,
        symbol: str,
        market: str | None = None,
        timeframe: str = "1m",
    ) -> dict:
        """Verify integrity of OHLCV data in the data lake.

        Checks for gaps and duplicates.

        Args:
            exchange: Exchange name.
            symbol: Sanitized symbol.
            market: Market name.
            timeframe: Timeframe to check.

        Returns:
            Dictionary with results.
        """
        entries = self.manifest.list_entries(
            exchange=exchange,
            symbol=symbol,
            market=market,
            data_type="raw",
        )

        # Filter by timeframe in metadata
        valid_paths = []
        for entry in entries:
            if not entry.path:
                continue
            meta = {}
            if entry.metadata_json:
                try:
                    meta = json.loads(entry.metadata_json)
                except:  # noqa: E722
                    pass
            if meta.get("timeframe") == timeframe:
                p = Path(entry.path)
                if not p.is_absolute():
                    p = Path(self.writer.base_path) / p
                if p.exists():
                    valid_paths.append(str(p))

        if not valid_paths:
            return {"status": "error", "message": "No files found to verify"}

        try:
            # Read all parquet files
            df = pl.read_parquet(valid_paths).sort("ts")
            row_count = len(df)

            if row_count < 2:
                return {
                    "status": "success",
                    "row_count": row_count,
                    "message": "Not enough data for verification",
                }

            # Calculate diffs
            df = df.with_columns(
                (pl.col("ts") - pl.col("ts").shift(1)).alias("diff"),
            )

            # Determine mode diff (most frequent interval)
            mode_diff = df["diff"].mode().item(0)
            if mode_diff is None:
                return {"status": "error", "message": "Could not determine timeframe"}

            # Detect gaps
            gaps = df.filter(pl.col("diff") > mode_diff)
            gap_count = len(gaps)

            # Detect duplicates or out-of-order
            overlaps = df.filter(pl.col("diff") <= 0)
            overlap_count = len(overlaps)

            result = {
                "status": "success" if gap_count == 0 and overlap_count == 0 else "warning",
                "row_count": row_count,
                "gap_count": gap_count,
                "overlap_count": overlap_count,
                "interval_ms": mode_diff,
            }

            if gap_count > 0:
                result["message"] = f"Found {gap_count} gaps"
            elif overlap_count > 0:
                result["message"] = f"Found {overlap_count} duplicates/overlaps"
            else:
                result["message"] = "Data is continuous and valid"

            return result

        except Exception as e:
            return {"status": "error", "message": f"Verification failed: {e}"}
