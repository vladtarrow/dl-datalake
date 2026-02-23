"""Tests for ParquetReader."""

from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from dl_datalake.storage.reader import ParquetReader


@pytest.fixture
def reader(tmp_path):
    """Create ParquetReader with temp directory."""
    return ParquetReader(base_path=str(tmp_path))


def test_init(tmp_path):
    """Test ParquetReader initialization."""
    reader = ParquetReader(base_path=str(tmp_path))
    assert reader.base_path == tmp_path


@patch("dl_datalake.storage.reader.duckdb")
def test_read_range_basic(mock_duckdb, reader):
    """Test successful read with date range."""
    # Setup mock data
    mock_arrow_table = MagicMock()
    mock_query_result = MagicMock()
    mock_query_result.to_arrow_table.return_value = mock_arrow_table
    mock_duckdb.execute.return_value = mock_query_result

    # Create mock DataFrame
    mock_df = pl.DataFrame({"ts": [1000, 2000], "value": [10, 20]})

    with patch("dl_datalake.storage.reader.pl.from_arrow", return_value=mock_df):
        result = reader.read_range(
            exchange="BINANCE",
            symbol="BTCUSDT",
            data_type="raw",
            start_date="2024-01-01T00:00:00",
            end_date="2024-01-02T00:00:00",
        )

    # Verify parameterized query was used
    assert mock_duckdb.execute.called
    call_args = mock_duckdb.execute.call_args
    assert "?" in call_args[0][0]  # Query has placeholders
    assert len(call_args[0][1]) == 3  # Three parameters

    assert isinstance(result, pl.DataFrame)
    assert len(result) == 2


@patch("dl_datalake.storage.reader.duckdb")
def test_read_range_parameterized_query(mock_duckdb, reader):
    """Test that query uses parameters (SQL injection prevention)."""
    mock_arrow_table = MagicMock()
    mock_query_result = MagicMock()
    mock_query_result.to_arrow_table.return_value = mock_arrow_table
    mock_duckdb.execute.return_value = mock_query_result

    mock_df = pl.DataFrame({"ts": []})

    with patch("dl_datalake.storage.reader.pl.from_arrow", return_value=mock_df):
        reader.read_range(
            exchange="TEST",
            symbol="TEST'; DROP TABLE--",  # SQL injection attempt
            data_type="raw",
            start_date="2024-01-01T00:00:00",
            end_date="2024-01-02T00:00:00",
        )

    # Verify parametrized query (not f-string)
    call_args = mock_duckdb.execute.call_args
    query = call_args[0][0]
    params = call_args[0][1]

    # Query should have placeholders
    assert query.count("?") == 3
    # Parameters should be in list
    assert isinstance(params, list)
    assert len(params) == 3


@patch("dl_datalake.storage.reader.duckdb")
def test_read_range_empty_result(mock_duckdb, reader):
    """Test reading with empty result."""
    mock_arrow_table = MagicMock()
    mock_query_result = MagicMock()
    mock_query_result.to_arrow_table.return_value = mock_arrow_table
    mock_duckdb.execute.return_value = mock_query_result

    empty_df = pl.DataFrame({"ts": []})

    with patch("dl_datalake.storage.reader.pl.from_arrow", return_value=empty_df):
        result = reader.read_range(
            exchange="BINANCE",
            symbol="BTCUSDT",
            data_type="raw",
            start_date="2024-01-01T00:00:00",
            end_date="2024-01-01T00:00:01",
        )

    assert isinstance(result, pl.DataFrame)
    assert len(result) == 0


@patch("dl_datalake.storage.reader.duckdb")
def test_read_range_returns_series_converted_to_frame(mock_duckdb, reader):
    """Test that if pl.from_arrow returns Series, it's converted to DataFrame."""
    mock_arrow_table = MagicMock()
    mock_query_result = MagicMock()
    mock_query_result.to_arrow_table.return_value = mock_arrow_table
    mock_duckdb.execute.return_value = mock_query_result

    # Mock pl.from_arrow to return a Series
    mock_series = pl.Series("ts", [1000, 2000])

    with patch("dl_datalake.storage.reader.pl.from_arrow", return_value=mock_series):
        result = reader.read_range(
            exchange="BINANCE",
            symbol="BTCUSDT",
            data_type="raw",
            start_date="2024-01-01T00:00:00",
            end_date="2024-01-02T00:00:00",
        )

    # Should be converted to DataFrame
    assert isinstance(result, pl.DataFrame)


def test_list_symbols(tmp_path):
    """Test listing symbols from directory structure."""
    # Create fake directory structure
    (tmp_path / "BINANCE" / "FUTURES" / "BTCUSDT").mkdir(parents=True)
    (tmp_path / "BINANCE" / "FUTURES" / "ETHUSDT").mkdir(parents=True)
    (tmp_path / "BYBIT" / "SPOT" / "BTCUSDT").mkdir(parents=True)

    reader = ParquetReader(base_path=str(tmp_path))
    symbols = reader.list_symbols()

    # Should return unique symbols
    assert set(symbols) == {"BTCUSDT", "ETHUSDT"}


def test_list_symbols_no_data_dir(tmp_path):
    """Test list_symbols when data directory doesn't exist."""
    nonexistent = tmp_path / "nonexistent"
    reader = ParquetReader(base_path=str(nonexistent))
    symbols = reader.list_symbols()

    assert symbols == []
