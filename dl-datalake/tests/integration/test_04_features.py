
import pytest

from dl_datalake.features.manager import FeatureStore


def test_feature_store_upload_and_versioning(temp_datalake):
    """
    Use Case 4: Feature Store Workflow
    Verifies that feature files can be registered and versioned.
    """
    # Initialize FeatureStore with temp paths
    data_root = temp_datalake["data_root"]
    db_path = temp_datalake["db_path"]

    fs = FeatureStore(base_path=str(data_root), db_path=str(db_path))

    # 1. Create a dummy feature file
    src_feature_file = temp_datalake["root"] / "my_features.parquet"
    src_feature_file.write_text(
        "DUMMY PARQUET CONTENT",
    )  # In real life this would be a valid parquet

    # 2. Upload Feature
    exchange = "BINANCE"
    symbol = "BTCUSDT"
    feature_set = "talib_indicators"
    version = "1.0.0"

    returned_version = fs.upload_feature(
        src_path=str(src_feature_file),
        exchange=exchange,
        market="SPOT",
        symbol=symbol,
        feature_set=feature_set,
        version=version,
    )

    assert returned_version == version

    # 3. Verify File Location
    expected_path = (
        data_root / "features" / feature_set / version / "my_features.parquet"
    )
    assert expected_path.exists()
    assert expected_path.read_text() == "DUMMY PARQUET CONTENT"

    # 4. Verify Manifest Entry
    manifest = temp_datalake["manifest"]
    entries = manifest.list_entries(symbol=symbol, data_type=feature_set)
    assert len(entries) == 1
    assert entries[0].version == version
    assert entries[0].path == str(expected_path)
    assert entries[0].type == feature_set


def test_feature_store_missing_file(temp_datalake):
    data_root = temp_datalake["data_root"]
    db_path = temp_datalake["db_path"]
    fs = FeatureStore(base_path=str(data_root), db_path=str(db_path))

    with pytest.raises(FileNotFoundError):
        fs.upload_feature(
            src_path="/non/existent/file",
            exchange="BINANCE",
            market="SPOT",
            symbol="BTC",
            feature_set="f",
        )
