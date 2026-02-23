import datetime

import polars as pl
import pytest

from dl_datalake.storage.writer import ParquetWriter


@pytest.fixture
def writer(tmp_path):
    return ParquetWriter(base_path=str(tmp_path))


def test_get_partition_path(writer):
    path = writer._get_partition_path(
        exchange="binance",
        market="futures",
        symbol="BTCUSDT",
        data_type="raw",
        period="1m",
        date=datetime.date(2023, 1, 1),
    )
    # Check structure: base/EXCHANGE/MARKET/SYMBOL/type/period/Y/M
    expected_parts = [
        "BINANCE",
        "FUTURES",
        "BTCUSDT",
        "raw",
        "1m",
        "2023",
        "01",
    ]
    assert path.is_dir()
    for part in expected_parts:
        assert part in str(path)
    # Day folder should NOT be there
    assert "2023/01/01" not in str(path).replace("\\", "/")



def test_write_table(writer):
    df = pl.DataFrame({"ts": [1672531200000], "a": [1]})
    date = datetime.date(2023, 1, 1)

    path, t_min, t_max = writer.write_table(
        df=df,
        exchange="binance",
        market="futures",
        symbol="BTCUSDT",
        data_type="raw",
        period="1m",
        date=date,
    )

    assert path.exists()
    assert str(path).endswith("BTCUSDT_1m_202301.parquet")

    read_df = pl.read_parquet(path)
    assert read_df["a"][0] == 1
    assert t_min == 1672531200000


def test_write_table_upsert(writer):
    date = datetime.date(2023, 1, 1)
    # 1. Write first chunk
    df1 = pl.DataFrame({"ts": [1000, 2000], "val": [1, 2]})
    writer.write_table(df1, "binance", "spot", "BTCUSDT", "raw", "1m", date)
    
    # 2. Write second chunk with overlap and new data
    df2 = pl.DataFrame({"ts": [2000, 3000], "val": [99, 3]}) # 2000 is update
    path, _, _ = writer.write_table(df2, "binance", "spot", "BTCUSDT", "raw", "1m", date)
    
    # 3. Verify
    result = pl.read_parquet(path)
    assert len(result) == 3
    assert result.filter(pl.col("ts") == 2000)["val"][0] == 99
    assert result["ts"].to_list() == [1000, 2000, 3000]



def test_write_ohlc_partitions(writer):
    # Create data spanning two days
    base_ts = 1672531200000  # 2023-01-01 00:00:00 UTC
    day_ms = 86400000

    df = pl.DataFrame(
        {
            "ts": [base_ts, base_ts + day_ms],  # Jan 1 and Jan 2
            "open": [100.0, 101.0],
        },
    )

    paths = list(
        writer.write_ohlc(
            df=df,
            exchange="binance",
            market="futures",
            symbol="BTCUSDT",
            period="1m",
        ),
    )

    # Both dates Jan 1 and Jan 2 are in the same month
    assert len(paths) == 1
    assert "202301.parquet" in str(paths[0][0])

    # Verify content
    df_read = pl.read_parquet(paths[0][0])
    assert len(df_read) == 2
    assert df_read["ts"].to_list() == [base_ts, base_ts + day_ms]



def test_write_ohlc_missing_ts(writer):
    df = pl.DataFrame({"open": [100.0]})
    with pytest.raises(ValueError, match="must have 'ts' column"):
        list(writer.write_ohlc(df, "binance", "futures", "BTCUSDT"))


def test_write_ticks(writer):
    base_ts = 1672531200000
    df = pl.DataFrame(
        {
            "ts": [base_ts],
            "price": [100.0],
            "qty": [1.0],
        },
    )

    paths = list(
        writer.write_ticks(
            df=df,
            exchange="binance",
            market="futures",
            symbol="BTCUSDT",
        ),
    )

    assert len(paths) == 1
    path, t_min, t_max = paths[0]
    assert "ticks" in str(path)
    assert "tick" in str(path)  # period name
