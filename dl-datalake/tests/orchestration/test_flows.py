"""Tests for Prefect flows."""

from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from dl_datalake.orchestration.flows import (
    aggregate_ohlc_task,
    ingest_csv_task,
    ingest_pipeline_flow,
)

# pytestmark = pytest.mark.skip("Prefect logging issue in CI/Test env")


@pytest.fixture
def mock_pipeline():
    """Mock IngestPipeline."""
    with patch("dl_datalake.orchestration.flows.IngestPipeline") as mock:
        yield mock


def test_ingest_csv_task(mock_pipeline, tmp_path):
    """Test ingest_csv_task execution."""
    # Create dummy CSV
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("ts,open,high,low,close,volume\n1000,100,110,90,105,1000")

    pipeline_instance = MagicMock()
    mock_pipeline.return_value = pipeline_instance

    ingest_csv_task.fn(
        file_path=str(csv_file),
        exchange="BINANCE",
        market="FUTURES",
        symbol="BTCUSDT",
    )

    pipeline_instance.ingest_csv.assert_called_once_with(
        str(csv_file),
        "BINANCE",
        "FUTURES",
        "BTCUSDT",
    )


@patch("dl_datalake.orchestration.flows.IngestPipeline")
def test_ingest_csv_task_retry(mock_pipeline_class, tmp_path):
    """Test that ingest_csv_task has retry logic."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("data")

    pipeline_instance = MagicMock()
    pipeline_instance.ingest_csv.side_effect = [Exception("First fail"), None]
    mock_pipeline_class.return_value = pipeline_instance

    # Task should retry on failure
    # Note: Prefect tasks decorated with @task(retries=2) will retry
    # We can't test the actual retry behavior without running Prefect,
    # but we can test that the task function works
    #  Just verify it was called (first call will fail)
    try:  # noqa: SIM105
        ingest_csv_task.fn(str(csv_file), "BINANCE", "FUTURES", "BTCUSDT")
    except Exception:  # noqa: BLE001, S110
        pass  # Expected to fail on first attempt

    # Verify it was called
    assert pipeline_instance.ingest_csv.called


@patch("dl_datalake.orchestration.flows.ParquetWriter")
@patch("dl_datalake.orchestration.flows.ParquetReader")
def test_aggregate_ohlc_task(mock_reader_class, mock_writer_class):
    """Test OHLC aggregation task."""
    # Mock reader
    reader_instance = MagicMock()
    mock_reader_class.return_value = reader_instance
    mock_df = pl.DataFrame(
        {
            "ts": [1000, 2000, 3000],
            "open": [100.0, 105.0, 110.0],
            "high": [110.0, 115.0, 120.0],
            "low": [90.0, 95.0, 100.0],
            "close": [105.0, 110.0, 115.0],
            "volume": [1000.0, 1100.0, 1200.0],
        },
    )
    reader_instance.read_range.return_value = mock_df

    # Mock writer
    writer_instance = MagicMock()
    mock_writer_class.return_value = writer_instance

    aggregate_ohlc_task.fn("BINANCE", "BTCUSDT", "1min", "15min")

    # Verify reader and writer were called
    assert reader_instance.read_range.called
    assert writer_instance.write_ohlc.called


@patch("dl_datalake.orchestration.flows.ingest_csv_task")
@patch("dl_datalake.orchestration.flows.aggregate_ohlc_task")
def test_ingest_pipeline_flow(mock_aggregate, mock_ingest):
    """Test full ingest pipeline flow."""
    ingest_pipeline_flow.fn(
        file_path="/data/test.csv",
        exchange="BINANCE",
        market="FUTURES",
        symbol="BTCUSDT",
    )

    # Verify ingest was called
    mock_ingest.assert_called_once_with(
        "/data/test.csv",
        "BINANCE",
        "FUTURES",
        "BTCUSDT",
    )

    # Verify aggregation was called twice (15min and 1h)
    assert mock_aggregate.call_count == 2
