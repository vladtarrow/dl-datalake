from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from dl_datalake.ingest.pipeline import IngestPipeline


@pytest.fixture
def pipeline(tmp_path):
    return IngestPipeline(
        data_root=str(tmp_path),
        db_path=str(tmp_path / "manifest.db"),
    )


def test_calculate_checksum(pipeline, tmp_path):
    f = tmp_path / "test.txt"
    f.write_text("hello")
    # echo -n "hello" | sha256sum
    expected = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    assert pipeline._calculate_checksum(f) == expected


@patch("dl_datalake.ingest.pipeline.pl.read_csv")
def test_ingest_csv_valid(mock_read_csv, pipeline):
    # Mock DF with ts
    mock_df = pl.DataFrame(
        {
            "ts": [1000, 2000],
            "open": [10, 11],
        },
    )
    mock_read_csv.return_value = mock_df

    # Mock writer and manifest
    pipeline.writer = MagicMock()
    pipeline.writer.write_ohlc.return_value = [(Path("mock_path.parquet"), 1000, 2000)]

    pipeline.manifest = MagicMock()
    pipeline._calculate_checksum = MagicMock(return_value="hash")

    result = pipeline.ingest_csv("dummy.csv", "binance", "spot", "BTCUSDT")

    assert result is True
    pipeline.writer.write_ohlc.assert_called_once()
    pipeline.manifest.add_entry.assert_called_once()


@patch("dl_datalake.ingest.pipeline.pl.read_csv")
def test_ingest_csv_fallback(mock_read_csv, pipeline):
    # Mock DF without header but correct column count (6)
    mock_df = pl.DataFrame(
        {
            "col1": [1000],
            "col2": [10],
            "col3": [10],
            "col4": [10],
            "col5": [10],
            "col6": [100],
        },
    )
    mock_read_csv.return_value = mock_df

    pipeline.writer = MagicMock()
    pipeline.writer.write_ohlc.return_value = []

    pipeline.ingest_csv("dummy.csv", "binance", "spot", "BTCUSDT")

    # Check that rename happened effectively (by checking what was passed to writer)
    # The writer receives the transformed df
    args, _ = pipeline.writer.write_ohlc.call_args
    df_arg = args[0]
    assert "ts" in df_arg.columns
    assert "open" in df_arg.columns


@patch("dl_datalake.ingest.pipeline.pl.read_csv")
def test_ingest_csv_invalid_columns(mock_read_csv, pipeline):
    # Mock DF with wrong column count and no ts
    mock_df = pl.DataFrame(
        {
            "col1": [1000],
            "col2": [10],
        },
    )
    mock_read_csv.return_value = mock_df

    with pytest.raises(ValueError, match="does not match expected default format"):
        pipeline.ingest_csv("dummy.csv", "binance", "spot", "BTCUSDT")
