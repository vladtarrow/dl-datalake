import pytest
from unittest.mock import MagicMock, patch
import ccxt
import time
from dl_datalake.ingest.exchange_connector import ExchangeConnector

@pytest.fixture
def mock_ccxt():
    with patch("dl_datalake.ingest.exchange_connector.ccxt") as mock:
        yield mock

@pytest.fixture
def connector(mock_ccxt):
    mock_exchange = MagicMock()
    mock_ccxt.binance.return_value = mock_exchange
    # Mock DDoSProtection class on ccxt
    mock_ccxt.DDoSProtection = ccxt.DDoSProtection
    
    with patch("dl_datalake.ingest.exchange_connector.IngestPipeline") as mock_pipeline:
        mock_pipeline_instance = MagicMock()
        mock_pipeline.return_value = mock_pipeline_instance
        conn = ExchangeConnector(exchange_id="binance", market_type="futures")
        yield conn

@patch("time.sleep")
def test_download_ohlcv_rate_limit_retry(mock_sleep, connector):
    """Test that download_ohlcv handles 429 and sleeps for 30s."""
    connector.exchange.milliseconds.return_value = 2000000000
    connector.exchange.markets = {"BTC/USDT": {"id": "BTCUSDT"}}
    
    # Side effect: 
    # 1. Success on probe
    # 2. 429 on first fetch
    # 3. Success on second fetch
    # 4. Empty on third fetch
    connector.exchange.fetch_ohlcv.side_effect = [
        [[1000, 100.0, 110.0, 90.0, 105.0, 1000.0]], # Probe
        ccxt.DDoSProtection("Rate limit"),           # First fetch (429)
        [[2000, 106.0, 116.0, 96.0, 111.0, 1100.0]], # Second fetch (Success)
        []                                           # End loop
    ]
    
    connector.pipeline.writer.write_ohlc.return_value = [("/path", 2000, 2000)]
    
    count = connector.download_ohlcv("BTC/USDT", timeframe="1m")
    
    assert count == 1
    # Check that sleep was called with 30 seconds
    mock_sleep.assert_any_call(30)
    assert connector.exchange.fetch_ohlcv.call_count == 4

@patch("time.sleep")
def test_probe_rate_limit_retry(mock_sleep, connector):
    """Test that listing date probe handles 429 and retries."""
    connector.exchange.milliseconds.return_value = 2000000000
    connector.exchange.markets = {"BTC/USDT": {"id": "BTCUSDT"}}
    
    # Side effect for fetch_ohlcv:
    # 1. 429 on first probe
    # 2. Success on second probe
    # 3. Empty on first fetch (end loop)
    # Note: probe uses fetch_ohlcv(symbol, timeframe, since=0, limit=1)
    # while loop uses fetch_ohlcv(symbol, timeframe, since, limit=1000)
    connector.exchange.fetch_ohlcv.side_effect = [
        ccxt.DDoSProtection("Rate limit"),           # First probe (429)
        [[1000, 100.0, 110.0, 90.0, 105.0, 1000.0]], # Second probe (Success)
        []                                           # First real fetch
    ]
    
    count = connector.download_ohlcv("BTC/USDT", timeframe="1m")
    
    # We didn't save any candles in this mock scenario after the probe
    assert count == 0
    mock_sleep.assert_called_with(30)
    # 2 probes + 1 fetch = 3 calls
    assert connector.exchange.fetch_ohlcv.call_count == 3
