import os
import shutil
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, File, HTTPException, UploadFile, Query
from fastapi.responses import FileResponse
from dl_datalake.features.manager import FeatureStore
from dl_datalake.metadata.manifest import ManifestManager, ManifestEntry

from schemas import Dataset, DatasetList

router = APIRouter(prefix="/api/v1/features", tags=["features"])

# Assuming we are running from src/dl-datalake-ui/backend
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent / "dl-datalake"
MANIFEST_PATH = str(BASE_DIR / "manifest.db")
DATA_ROOT = str(BASE_DIR / "data")


@router.get("", response_model=DatasetList)
def list_features(
    exchange: Optional[str] = None,
    symbol: Optional[str] = None,
    market: Optional[str] = None,
    feature_set: Optional[str] = None,
    version: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
):
    """List all features with versions.
    
    Args:
        exchange: Filter by exchange
        symbol: Filter by symbol
        market: Filter by market type
        feature_set: Filter by feature set name
        version: Filter by version
        limit: Maximum number of results
        offset: Offset for pagination
    """
    if not os.path.exists(MANIFEST_PATH):
        return DatasetList(datasets=[], total=0)
    
    manager = ManifestManager(db_path=MANIFEST_PATH)
    
    # Get all entries that are features (not raw, ticks, agg)
    all_entries = manager.list_entries(
        exchange=exchange,
        symbol=symbol,
        market=market,
        data_type=None  # We'll filter manually
    )
    
    # Filter to only features (exclude standard data types)
    feature_entries = []
    standard_types = {"raw", "ticks", "agg", "alt"}
    
    for entry in all_entries:
        # If type is not in standard types, it's a feature
        if entry.type not in standard_types:
            # Apply feature_set filter if provided
            if feature_set and entry.type != feature_set:
                continue
            # Apply version filter if provided
            if version and entry.version != version:
                continue
            feature_entries.append(entry)
    
    total = len(feature_entries)
    
    # Apply pagination
    paged_entries = feature_entries[offset : offset + limit]
    
    datasets = []
    for entry in paged_entries:
        file_path = Path(entry.path)
        if not file_path.is_absolute():
            file_path = BASE_DIR / file_path
            
        file_size = 0
        from datetime import datetime
        last_modified = entry.created_at
        
        if file_path.exists():
            stat = file_path.stat()
            file_size = stat.st_size
            last_modified = datetime.fromtimestamp(stat.st_mtime)
        
        import json
        metadata = {}
        if entry.metadata_json:
            try:
                metadata = json.loads(entry.metadata_json)
            except:
                pass
        
        datasets.append(Dataset(
            id=str(entry.id),
            exchange=entry.exchange,
            symbol=entry.symbol,
            market=entry.market,
            timeframe=entry.version,  # Use version as timeframe for features
            data_type=entry.type,  # This is the feature_set name
            file_path=str(file_path),
            file_size_bytes=file_size,
            last_modified=last_modified,
            time_from=datetime.fromtimestamp(entry.time_from / 1000) if entry.time_from else None,
            time_to=datetime.fromtimestamp(entry.time_to / 1000) if entry.time_to else None
        ))
    
    return DatasetList(datasets=datasets, total=total)


@router.get("/sets")
def list_feature_sets(
    exchange: Optional[str] = None,
    symbol: Optional[str] = None,
    market: Optional[str] = None
):
    """List all unique feature sets with their versions.
    
    Args:
        exchange: Filter by exchange
        symbol: Filter by symbol
        market: Filter by market type
    
    Returns:
        Grouped view of feature sets with available versions
    """
    if not os.path.exists(MANIFEST_PATH):
        return {"feature_sets": []}
    
    manager = ManifestManager(db_path=MANIFEST_PATH)
    all_entries = manager.list_entries(
        exchange=exchange,
        symbol=symbol,
        market=market,
        data_type=None
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
                    "versions": []
                }
            
            # Add version if not already present
            if entry.version not in feature_sets[feature_set_name]["versions"]:
                feature_sets[feature_set_name]["versions"].append(entry.version)
    
    # Sort versions (try to sort as numbers if possible)
    for fs in feature_sets.values():
        try:
            fs["versions"] = sorted(
                fs["versions"],
                key=lambda v: float(v) if v.replace(".", "").isdigit() else 0,
                reverse=True
            )
        except:
            fs["versions"] = sorted(fs["versions"], reverse=True)
    
    return {"feature_sets": list(feature_sets.values())}


@router.post("/upload")
def upload_feature(
    file: UploadFile = File(...),
    exchange: str = Query(...),
    market: str = Query(...),
    symbol: str = Query(...),
    feature_set: str = Query(...),
    version: str = Query("1.0.0")
):
    """Upload a feature file.
    
    Args:
        file: The feature file to upload
        exchange: Exchange name
        market: Market type (SPOT, FUTURES, etc.)
        symbol: Trading symbol
        feature_set: Feature set name
        version: Version string
    """
    try:
        # Save uploaded file temporarily
        temp_path = BASE_DIR / "temp" / file.filename
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(temp_path, "wb") as f:
            shutil.copyfileobj(file.file, f)
        
        # Upload to feature store
        store = FeatureStore(base_path=DATA_ROOT, db_path=MANIFEST_PATH)
        result_version = store.upload_feature(
            src_path=str(temp_path),
            exchange=exchange,
            market=market,
            symbol=symbol,
            feature_set=feature_set,
            version=version
        )
        
        # Clean up temp file
        temp_path.unlink()
        
        return {
            "status": "success",
            "version": result_version,
            "message": f"Feature {feature_set} v{result_version} uploaded successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload feature: {str(e)}")


@router.get("/{feature_id}/download")
def download_feature(feature_id: int):
    """Download a feature file by ID.
    
    Args:
        feature_id: ID of the feature entry in manifest
    """
    manager = ManifestManager(db_path=MANIFEST_PATH)
    with manager.Session() as session:
        entry = session.query(ManifestEntry).filter_by(id=int(feature_id)).first()
        if not entry:
            raise HTTPException(status_code=404, detail="Feature not found")
        
        file_path = Path(entry.path)
        if not file_path.is_absolute():
            file_path = BASE_DIR / file_path
        
        if not file_path.exists():
            raise HTTPException(status_code=404, detail=f"Feature file not found: {file_path}")
        
        return FileResponse(
            path=str(file_path),
            filename=file_path.name,
            media_type="application/octet-stream"
        )


@router.get("/{feature_id}")
def get_feature(feature_id: int):
    """Get feature metadata by ID.
    
    Args:
        feature_id: ID of the feature entry in manifest
    """
    manager = ManifestManager(db_path=MANIFEST_PATH)
    with manager.Session() as session:
        entry = session.query(ManifestEntry).filter_by(id=int(feature_id)).first()
        if not entry:
            raise HTTPException(status_code=404, detail="Feature not found")
        
        file_path = Path(entry.path)
        if not file_path.is_absolute():
            file_path = BASE_DIR / file_path
        
        file_size = 0
        from datetime import datetime
        last_modified = entry.created_at
        
        if file_path.exists():
            stat = file_path.stat()
            file_size = stat.st_size
            last_modified = datetime.fromtimestamp(stat.st_mtime)
        
        import json
        metadata = {}
        if entry.metadata_json:
            try:
                metadata = json.loads(entry.metadata_json)
            except:
                pass
        
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
            "metadata": metadata
        }


@router.delete("/{feature_id}")
def delete_feature(feature_id: int):
    """Delete a feature by ID.
    
    Args:
        feature_id: ID of the feature entry in manifest
    """
    manager = ManifestManager(db_path=MANIFEST_PATH)
    with manager.Session() as session:
        entry = session.query(ManifestEntry).filter_by(id=int(feature_id)).first()
        if not entry:
            raise HTTPException(status_code=404, detail="Feature not found")
        
        file_path = Path(entry.path)
        if not file_path.is_absolute():
            file_path = BASE_DIR / file_path
        
        # Delete file if exists
        if file_path.exists():
            file_path.unlink()
        
        # Delete entry from manifest
        session.delete(entry)
        session.commit()
        
        return {"status": "success", "message": f"Feature {entry.type} v{entry.version} deleted"}
