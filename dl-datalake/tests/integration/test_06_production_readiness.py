from concurrent.futures import ThreadPoolExecutor

import pytest


def test_concurrent_writes(temp_datalake, sample_ohlc_generator):
    """
    Use Case 6.1: Concurrency
    Verifies that multiple threads writing to different symbols (or same) don't crash.
    """
    pipeline = temp_datalake["pipeline"]
    manifest = temp_datalake["manifest"]

    # Create 2 source files
    df1 = sample_ohlc_generator(length=50)
    df2 = sample_ohlc_generator(length=50)

    p1 = temp_datalake["root"] / "data1.csv"
    p2 = temp_datalake["root"] / "data2.csv"

    df1.write_csv(p1)
    df2.write_csv(p2)

    def ingest_task(path, sym):
        return pipeline.ingest_csv(str(path), "BINANCE", "SPOT", sym)

    # Run in parallel
    with ThreadPoolExecutor(max_workers=2) as executor:
        f1 = executor.submit(ingest_task, p1, "SYM1")
        f2 = executor.submit(ingest_task, p2, "SYM2")

        assert f1.result() is True
        assert f2.result() is True

    # Verify both exist
    assert len(manifest.list_entries(symbol="SYM1")) >= 1
    assert len(manifest.list_entries(symbol="SYM2")) >= 1


def test_error_handling_bad_data(temp_datalake):
    """
    Use Case 6.2: Error Handling (Bad Data)
    """
    pipeline = temp_datalake["pipeline"]

    # Create invalid CSV (missing ts)
    bad_csv = temp_datalake["root"] / "bad.csv"
    bad_csv.write_text("open,high,low,close,volume\n1,2,3,4,5")  # No 'ts'

    with pytest.raises(Exception):  # noqa: B017, PT011
        pipeline.ingest_csv(str(bad_csv), "BINANCE", "SPOT", "BADSYM")

    # Verify no partial write in manifest
    entries = temp_datalake["manifest"].list_entries(symbol="BADSYM")
    assert len(entries) == 0
