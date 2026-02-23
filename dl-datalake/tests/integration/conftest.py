from datetime import UTC, datetime

import numpy as np
import polars as pl
import pytest

from dl_datalake.ingest.pipeline import IngestPipeline
from dl_datalake.metadata.manifest import ManifestManager
from dl_datalake.storage.writer import ParquetWriter


@pytest.fixture
def temp_datalake(tmp_path):
    """
    Sets up a temporary datalake environment.
    Returns a dictionary with paths and initialized components.
    """
    data_root = tmp_path / "data"
    db_path = tmp_path / "manifest.db"

    # Create distinct directories to ensure no overlap if not handled correctly
    data_root.mkdir()

    pipeline = IngestPipeline(data_root=str(data_root), db_path=str(db_path))
    manifest = ManifestManager(db_path=str(db_path))
    writer = ParquetWriter(base_path=str(data_root))

    return {
        "root": tmp_path,
        "data_root": data_root,
        "db_path": db_path,
        "pipeline": pipeline,
        "manifest": manifest,
        "writer": writer,
    }


@pytest.fixture
def sample_ohlc_generator():
    """Generates sample OHLC data."""

    def _generate(length=100, start_time=None, period_ms=60000, seed=42):
        if start_time is None:
            start_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC).timestamp() * 1000

        rng = np.random.default_rng(seed)

        # Generate timestamps
        timestamps = np.array(
            [start_time + i * period_ms for i in range(length)], dtype=np.int64,
        )

        # Generate random walk for prices
        base_price = 1000.0
        changes = rng.standard_normal(length) * 2.0
        close = base_price + np.cumsum(changes)

        # Derive OHLC from close (synthetic)
        high = close + np.abs(rng.standard_normal(length))
        low = close - np.abs(rng.standard_normal(length))
        open_ = np.roll(close, 1)
        open_[0] = base_price

        volume = np.abs(rng.standard_normal(length) * 100) + 10

        return pl.DataFrame(
            {
                "ts": timestamps,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            },
        )

    return _generate


@pytest.fixture
def sample_csv_path(tmp_path, sample_ohlc_generator):
    """Creates a physical CSV file with sample data."""

    def _create(filename="test_data.csv", **kwargs):
        df = sample_ohlc_generator(**kwargs)
        path = tmp_path / filename
        df.write_csv(path)
        return str(path), df

    return _create
