import pytest
from sqlalchemy.exc import IntegrityError


def test_ingest_idempotency(temp_datalake, sample_csv_path):
    """
    Use Case 8: Idempotency
    Verifies that running ingestion twice for the same data is safe.
    """
    csv_path, _ = sample_csv_path(filename="idem.csv", length=100)
    pipeline = temp_datalake["pipeline"]
    manifest = temp_datalake["manifest"]
    exchange = "IDEM"
    symbol = "BTC"
    market = "SPOT"

    # Run 1
    pipeline.ingest_csv(csv_path, exchange, market, symbol)
    entries_1 = manifest.list_entries(symbol=symbol)
    assert len(entries_1) > 0

    # Run 2 (Same file)
    # Ideally this should Just Work (Update or Ignore).
    # If the current implementation raises IntegrityError, we mark it as xfail or
    # we catch it to confirm the behavior is at least specific.
    # The Use Case says: "System ne dolzhna upast".

    try:
        pipeline.ingest_csv(csv_path, exchange, market, symbol)
    except IntegrityError:
        # Known issue: ManifestManager doesn't handle UPSERT yet.
        pytest.fail(
            "Ingestion is NOT idempotent: raised IntegrityError on duplicate entry.",
        )
    except Exception as e:  # noqa: BLE001
        pytest.fail(f"Ingestion failed with {e}")

    entries_2 = manifest.list_entries(symbol=symbol)
    assert len(entries_2) == len(entries_1)  # Should not duplicate
