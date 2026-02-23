from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dl_datalake.ingest.exchange_connector import ExchangeConnector


@pytest.fixture
def mock_ccxt():
    """Mock CCXT exchange."""
    with patch("dl_datalake.ingest.exchange_connector.ccxt") as mock:
        yield mock


@pytest.fixture
def connector(mock_ccxt):
    """Create ExchangeConnector with mocked CCXT."""
    mock_exchange = MagicMock()
    mock_ccxt.binance.return_value = mock_exchange

    with patch("dl_datalake.ingest.exchange_connector.IngestPipeline") as mock_pipeline:
        mock_pipeline_instance = MagicMock()
        mock_pipeline.return_value = mock_pipeline_instance

        conn = ExchangeConnector(exchange_id="binance", market_type="futures")
        yield conn


def test_init_binance_futures(mock_ccxt):
    """Test initialization with Binance futures."""
    mock_exchange = MagicMock()
    mock_ccxt.binance.return_value = mock_exchange

    connector = ExchangeConnector(exchange_id="binance", market_type="future")

    assert connector.exchange_id == "binance"
    assert connector.market_type == "future"
    mock_ccxt.binance.assert_called_once()
    # Verify options passed
    call_kwargs = mock_ccxt.binance.call_args[0][0]
    assert call_kwargs["options"]["defaultType"] == "future"


def test_init_binance_spot(mock_ccxt):
    """Test initialization with Binance spot."""
    mock_exchange = MagicMock()
    mock_ccxt.binance.return_value = mock_exchange

    connector = ExchangeConnector(exchange_id="binance", market_type="spot")

    assert connector.market_type == "spot"
    call_kwargs = mock_ccxt.binance.call_args[0][0]
    assert call_kwargs["options"]["defaultType"] == "spot"


def test_init_bybit_futures(mock_ccxt):
    """Test initialization with Bybit futures (linear)."""
    mock_exchange = MagicMock()
    mock_ccxt.bybit.return_value = mock_exchange

    connector = ExchangeConnector(exchange_id="bybit", market_type="linear")

    assert connector.exchange_id == "bybit"
    assert connector.market_type == "linear"
    call_kwargs = mock_ccxt.bybit.call_args[0][0]
    assert call_kwargs["options"]["defaultType"] == "linear"


def test_init_bybit_spot(mock_ccxt):
    """Test initialization with Bybit spot."""
    mock_exchange = MagicMock()
    mock_ccxt.bybit.return_value = mock_exchange

    connector = ExchangeConnector(exchange_id="bybit", market_type="spot")

    assert connector.market_type == "spot"
    call_kwargs = mock_ccxt.bybit.call_args[0][0]
    assert call_kwargs["options"]["defaultType"] == "spot"


def test_get_all_symbols(connector):
    """Test fetching all active symbols."""
    connector.exchange.load_markets.return_value = {
        "BTC/USDT": {"active": True},
        "ETH/USDT": {"active": True},
        "OLD/PAIR": {"active": False},
    }

    symbols = connector.get_all_symbols()

    assert symbols == ["BTC/USDT", "ETH/USDT"]
    connector.exchange.load_markets.assert_called_once()


def test_download_ohlcv_success(connector):
    """Test successful OHLCV download."""
    # Mock CCXT response
    connector.exchange.milliseconds.return_value = 2000000000
    # Return data once, then empty list to break the loop
    connector.exchange.fetch_ohlcv.side_effect = [
        [[1000, 100.0, 110.0, 90.0, 105.0, 1000.0]],  # Probe response
        [
            [1000, 100.0, 110.0, 90.0, 105.0, 1000.0],  # ts, o, h, l, c, v
            [2000, 105.0, 115.0, 95.0, 110.0, 1100.0],
        ],
        [],  # Empty list breaks the while loop
    ]

    # Mocking markets for symbol check
    connector.exchange.markets = {"BTC/USDT": {"id": "BTCUSDT"}}
    
    # Pipeline is already mocked via fixture
    connector.pipeline.writer.write_ohlc.return_value = [("/path/to/file.parquet", 1000, 2000)]

    count = connector.download_ohlcv("BTC/USDT", timeframe="1m", since_days=1)

    assert count == 2
    assert connector.pipeline.writer.write_ohlc.called
    assert connector.pipeline.manifest.add_entry.called


def test_download_ohlcv_empty_response(connector):
    """Test handling empty OHLCV response."""
    connector.exchange.milliseconds.return_value = 2000000000
    connector.exchange.fetch_ohlcv.return_value = []

    count = connector.download_ohlcv("BTC/USDT", timeframe="1m", since_days=1)

    assert count == 0


@patch("dl_datalake.ingest.exchange_connector.logger")
def test_download_ohlcv_exception_logged(mock_logger, connector):
    """Test that exceptions are logged with loguru."""
    # Mocking markets for symbol check
    connector.exchange.markets = {"BTC/USDT": {"id": "BTCUSDT"}}
    connector.exchange.milliseconds.return_value = 2000000000
    connector.exchange.fetch_ohlcv.side_effect = Exception("Network error")

    count = connector.download_ohlcv("BTC/USDT", timeframe="1m", since_days=1)

    assert count == 0
    # Verify logger.error was called
    assert mock_logger.error.called
    assert mock_logger.error.call_args[1]["symbol"] == "BTC/USDT"


def test_download_funding_rates_success(connector):
    """Test successful funding rates download."""
    connector.exchange.milliseconds.return_value = 2000000000
    connector.exchange.fetch_funding_rate_history.return_value = [
        {"timestamp": 1000, "fundingRate": 0.0001},
        {"timestamp": 2000, "fundingRate": 0.0002},
    ]

    connector.pipeline.writer = MagicMock()
    connector.pipeline.writer.write_table.return_value = (Path("/path"), 1000, 2000)
    connector.pipeline.manifest = MagicMock()

    count = connector.download_funding_rates("BTC/USDT", _since_days=1)

    assert count == 2
    assert connector.pipeline.writer.write_table.called
    assert connector.pipeline.manifest.add_entry.called


def test_download_funding_rates_spot_skipped(mock_ccxt):
    """Test that funding rates are skipped for spot market."""
    mock_exchange = MagicMock()
    mock_ccxt.binance.return_value = mock_exchange

    connector = ExchangeConnector(exchange_id="binance", market_type="spot")

    count = connector.download_funding_rates("BTC/USDT", _since_days=1)

    assert count == 0
    # Should not call API for spot
    assert not connector.exchange.fetch_funding_rate_history.called


@patch("dl_datalake.ingest.exchange_connector.logger")
def test_download_funding_rates_exception_logged(mock_logger, connector):
    """Test that funding rate exceptions are logged."""
    connector.exchange.milliseconds.return_value = 2000000000
    connector.exchange.fetch_funding_rate_history.side_effect = Exception("API error")

    count = connector.download_funding_rates("BTC/USDT", _since_days=1)

    assert count == 0
    assert mock_logger.error.called
    assert mock_logger.error.call_args[1]["symbol"] == "BTC/USDT"
