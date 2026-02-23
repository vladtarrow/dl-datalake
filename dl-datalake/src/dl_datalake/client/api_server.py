"""Data Lake REST API for managing and accessing data and features."""

import contextlib
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse

from dl_datalake.client.dl_client import DataLakeClient
from dl_datalake.features.manager import FeatureStore
from dl_datalake.metadata.manifest import ManifestEntry, ManifestManager

app = FastAPI(title="Data Lake REST API")
client = DataLakeClient()


@app.get("/list")
def list_entries(
    symbol: str | None = None,
    data_type: str | None = None,
    exchange: str | None = None,
    market: str | None = None,
) -> list[dict[str, Any]]:
    """List manifest entries.

    Args:
        symbol: Filter by symbol.
        data_type: Filter by type.
        exchange: Filter by exchange.
        market: Filter by market type.

    Returns:
        List of entries.
    """
    entries = client.list(
        symbol=symbol,
        data_type=data_type,
        exchange=exchange,
        market=market,
    )
    return [
        {
            "id": e.id,
            "symbol": e.symbol,
            "exchange": e.exchange,
            "market": e.market,
            "path": e.path,
            "type": e.type,
        }
        for e in entries
    ]


@app.get("/read")
def read_data(
    exchange: str,
    symbol: str,
    start: str,
    end: str,
    data_type: str = "raw",
) -> list[dict[str, Any]]:
    """Read data.

    Args:
        exchange: Exchange name.
        symbol: Symbol.
        start: Start date.
        end: End date.
        data_type: Data type.

    Returns:
        List of rows.
    """
    try:
        df = client.read_ohlc(exchange, symbol, start, end, data_type)
        return df.to_dicts()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/health")
def health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "ok"}


# ========== Feature Store Endpoints ==========


@app.get("/features")
def list_features(
    exchange: str | None = None,
    symbol: str | None = None,
    market: str | None = None,
    feature_set: str | None = None,
    version: str | None = None,
) -> list[dict[str, Any]]:
    """List all features with versions.

    Args:
        exchange: Filter by exchange
        symbol: Filter by symbol
        market: Filter by market type
        feature_set: Filter by feature set name
        version: Filter by version

    Returns:
        List of feature entries with metadata
    """
    manifest = ManifestManager()
    all_entries = manifest.list_entries(
        exchange=exchange,
        symbol=symbol,
        market=market,
        data_type=None,
    )

    # Filter to only features (exclude standard data types)
    standard_types = {"raw", "ticks", "agg", "alt"}
    feature_entries = []

    for entry in all_entries:
        if entry.type not in standard_types:
            if feature_set and entry.type != feature_set:
                continue
            if version and entry.version != version:
                continue
            feature_entries.append(entry)

    return [
        {
            "id": e.id,
            "exchange": e.exchange,
            "symbol": e.symbol,
            "market": e.market,
            "feature_set": e.type,
            "version": e.version,
            "path": e.path,
            "checksum": e.checksum,
            "created_at": e.created_at.isoformat() if e.created_at else None,
        }
        for e in feature_entries
    ]


@app.get("/features/sets")
def list_feature_sets(
    exchange: str | None = None,
    symbol: str | None = None,
    market: str | None = None,
) -> dict[str, Any]:
    """List all unique feature sets with their versions.

    Args:
        exchange: Filter by exchange
        symbol: Filter by symbol
        market: Filter by market type

    Returns:
        Grouped view of feature sets with available versions
    """
    manifest = ManifestManager()
    all_entries = manifest.list_entries(
        exchange=exchange,
        symbol=symbol,
        market=market,
        data_type=None,
    )

    standard_types = {"raw", "ticks", "agg", "alt"}
    feature_sets = {}

    for entry in all_entries:
        if entry.type not in standard_types:
            feature_set_name = entry.type
            if feature_set_name not in feature_sets:
                feature_sets[feature_set_name] = {
                    "name": feature_set_name,
                    "exchange": entry.exchange,
                    "symbol": entry.symbol,
                    "market": entry.market,
                    "versions": [],
                }

            if entry.version not in feature_sets[feature_set_name]["versions"]:
                feature_sets[feature_set_name]["versions"].append(entry.version)

    # Sort versions
    for fs in feature_sets.values():
        with contextlib.suppress(Exception):
            fs["versions"] = sorted(
                fs["versions"],
                key=lambda v: (
                    float(str(v))
                    if (
                        isinstance(v, str)
                        and str(v).replace(".", "").replace("-", "").isdigit()
                    )
                    else 0
                ),
                reverse=True,
            )

    return {"feature_sets": list(feature_sets.values())}


@app.post("/features/upload")
def upload_feature(  # noqa: PLR0913
    file: Annotated[UploadFile, File(...)],
    exchange: Annotated[str, Query(...)],
    market: Annotated[str, Query(...)],
    symbol: Annotated[str, Query(...)],
    feature_set: Annotated[str, Query(...)],
    version: Annotated[str, Query(description="Version string")] = "1.0.0",
) -> dict[str, Any]:
    """Upload a feature file.

    Args:
        file: The feature file to upload
        exchange: Exchange name
        market: Market type (SPOT, FUTURES, etc.)
        symbol: Trading symbol
        feature_set: Feature set name
        version: Version string

    Returns:
        Upload result with version
    """
    try:
        # Save uploaded file temporarily
        temp_dir = Path("temp")
        temp_dir.mkdir(exist_ok=True)
        temp_path = temp_dir / (file.filename or "uploaded_feature")

        with Path(temp_path).open("wb") as f:
            shutil.copyfileobj(file.file, f)

        # Upload to feature store
        store = FeatureStore()
        # Pass parameters as keyword arguments to avoid positional issues if any,
        # and use a helper or dict if PLR0913 persists, but here we just call it.
        result_version = store.upload_feature(
            src_path=str(temp_path),
            exchange=exchange,
            market=market,
            symbol=symbol,
            feature_set=feature_set,
            version=version,
        )

        # Clean up temp file
        temp_path.unlink()
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to upload feature: {e!s}",
        ) from e
    else:
        return {
            "status": "success",
            "version": result_version,
            "message": f"Feature {feature_set} v{result_version} uploaded successfully",
        }


@app.get("/features/{feature_id}")
def get_feature(feature_id: int) -> dict[str, Any]:
    """Get feature metadata by ID.

    Args:
        feature_id: ID of the feature entry in manifest

    Returns:
        Feature metadata
    """
    manifest = ManifestManager()
    with manifest.Session() as session:
        entry = session.query(ManifestEntry).filter_by(id=int(feature_id)).first()
        if not entry:
            raise HTTPException(status_code=404, detail="Feature not found")

        file_path = Path(entry.path)
        file_size = 0
        last_modified = entry.created_at

        if file_path.exists():
            stat = file_path.stat()
            file_size = stat.st_size
            last_modified = datetime.fromtimestamp(stat.st_mtime, tz=UTC)

        metadata = {}
        if entry.metadata_json:
            with contextlib.suppress(Exception):
                metadata = json.loads(entry.metadata_json)

        return {
            "id": entry.id,
            "exchange": entry.exchange,
            "symbol": entry.symbol,
            "market": entry.market,
            "feature_set": entry.type,
            "version": entry.version,
            "file_path": str(file_path),
            "file_size_bytes": file_size,
            "checksum": entry.checksum,
            "last_modified": last_modified.isoformat() if last_modified else None,
            "created_at": entry.created_at.isoformat() if entry.created_at else None,
            "metadata": metadata,
        }


@app.get("/features/{feature_id}/download")
def download_feature(feature_id: int) -> FileResponse:
    """Download a feature file by ID.

    Args:
        feature_id: ID of the feature entry in manifest

    Returns:
        File download response
    """
    manifest = ManifestManager()
    with manifest.Session() as session:
        entry = session.query(ManifestEntry).filter_by(id=int(feature_id)).first()
        if not entry:
            raise HTTPException(status_code=404, detail="Feature not found")

        file_path = Path(entry.path)
        if not file_path.exists():
            raise HTTPException(
                status_code=404,
                detail=f"Feature file not found: {file_path}",
            )

        return FileResponse(
            path=str(file_path),
            filename=file_path.name,
            media_type="application/octet-stream",
        )


@app.delete("/features/{feature_id}")
def delete_feature(feature_id: int) -> dict[str, str]:
    """Delete a feature by ID.

    Args:
        feature_id: ID of the feature entry in manifest

    Returns:
        Deletion status
    """
    manifest = ManifestManager()
    with manifest.Session() as session:
        entry = session.query(ManifestEntry).filter_by(id=int(feature_id)).first()
        if not entry:
            raise HTTPException(status_code=404, detail="Feature not found")

        file_path = Path(entry.path)

        # Delete file if exists
        if file_path.exists():
            file_path.unlink()

        # Delete entry from manifest
        session.delete(entry)
        session.commit()

        return {
            "status": "success",
            "message": f"Feature {entry.type} v{entry.version} deleted",
        }
