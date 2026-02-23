import os
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException
from dl_datalake.metadata.manifest import ManifestManager

from schemas import DataPreview, Dataset, DatasetList

router = APIRouter(prefix="/api/v1", tags=["data"])

# Assuming we are running from src/dl-datalake-ui/backend
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent / "dl-datalake"
MANIFEST_PATH = str(BASE_DIR / "manifest.db")

@router.get("/datasets", response_model=DatasetList)
def get_datasets(
    exchange: str = None, 
    symbol: str = None,
    market: str = None,
    data_type: str = None,
    limit: int = 20,
    offset: int = 0
):
    print(f"DEBUG: Using MANIFEST_PATH={MANIFEST_PATH}")
    if not os.path.exists(MANIFEST_PATH):
        print(f"DEBUG: Manifest file NOT FOUND at {MANIFEST_PATH}")
        return DatasetList(datasets=[], total=0)
        
    manager = ManifestManager(db_path=MANIFEST_PATH)
    entries = manager.list_entries(exchange=exchange, symbol=symbol, market=market, data_type=data_type)
    total = len(entries)
    
    # Apply pagination
    paged_entries = entries[offset : offset + limit]
    print(f"DEBUG: Found {total} entries, returning {len(paged_entries)}")
    
    datasets = []
    for entry in paged_entries:
        file_path = Path(entry.path)
        if not file_path.is_absolute():
            file_path = BASE_DIR / file_path
            
        file_size = 0
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
        
        # Fallback: extract timeframe from path if missing in metadata
        timeframe = metadata.get('timeframe')
        if not timeframe:
            try:
                # Search for 'raw' or 'ticks' folder and take the NEXT one as timeframe
                path_parts = entry.path.replace('\\', '/').split('/')
                for i, part in enumerate(path_parts):
                    if part.lower() in ['raw', 'ticks', 'agg', 'feature'] and i + 1 < len(path_parts):
                        timeframe = path_parts[i+1]
                        break
            except:
                pass
                
        datasets.append(Dataset(
            id=str(entry.id),
            exchange=entry.exchange,
            symbol=entry.symbol,
            market=entry.market,
            timeframe=timeframe, 
            data_type=entry.type,
            file_path=str(file_path),
            file_size_bytes=file_size,
            last_modified=last_modified,
            time_from=datetime.fromtimestamp(entry.time_from / 1000) if entry.time_from else None,
            time_to=datetime.fromtimestamp(entry.time_to / 1000) if entry.time_to else None
        ))
        
    return DatasetList(datasets=datasets, total=total)


@router.get("/datasets/{dataset_id}/preview", response_model=DataPreview)
def get_dataset_preview(dataset_id: int, limit: int = 100, offset: int = 0):
    import polars as pl
    import json
    manager = ManifestManager(db_path=MANIFEST_PATH)
    with manager.Session() as session:
        from dl_datalake.metadata.manifest import ManifestEntry
        entry = session.query(ManifestEntry).filter_by(id=int(dataset_id)).first()
        if not entry:
            raise HTTPException(status_code=404, detail="Dataset not found in manifest")
            
        file_path = Path(entry.path)
        if not file_path.is_absolute():
            file_path = BASE_DIR / file_path
            
        if not file_path.exists():
            raise HTTPException(status_code=404, detail=f"File not found: {file_path}")
            
        try:
            # Parse metadata
            metadata = {}
            if entry.metadata_json:
                try:
                    metadata = json.loads(entry.metadata_json)
                except:
                    pass
            
            # Fallback: extract timeframe from path if missing in metadata
            timeframe = metadata.get('timeframe')
            if not timeframe:
                try:
                    path_parts = entry.path.replace('\\', '/').split('/')
                    for i, part in enumerate(path_parts):
                        if part.lower() in ['raw', 'ticks', 'agg', 'feature'] and i + 1 < len(path_parts):
                            timeframe = path_parts[i+1]
                            break
                except:
                    pass
            
            if not timeframe:
                timeframe = 'N/A'
                
            metadata['timeframe'] = timeframe

            # Polars is VERY fast at slicing Parquet without loading everything
            df = pl.scan_parquet(str(file_path)) 
            total_rows = df.select(pl.count()).collect().item()
            
            # Slice and add 'timeframe' and 'symbol' as columns so they are visible "inside" the data rows
            preview_df = df.slice(offset, limit).with_columns([
                pl.lit(timeframe).alias("timeframe"),
                pl.lit(entry.symbol).alias("symbol")
            ]).collect()
            
            return DataPreview(
                columns=preview_df.columns,
                rows=preview_df.to_dicts(),
                total_rows=total_rows,
                metadata=metadata
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to read file: {str(e)}")


@router.get("/datasets/{dataset_id}/export")
def export_dataset(dataset_id: int):
    import polars as pl
    import json
    manager = ManifestManager(db_path=MANIFEST_PATH)
    with manager.Session() as session:
        from dl_datalake.metadata.manifest import ManifestEntry
        entry = session.query(ManifestEntry).filter_by(id=int(dataset_id)).first()
        if not entry:
            raise HTTPException(status_code=404, detail="Dataset not found in manifest")
            
        file_path = Path(entry.path)
        if not file_path.is_absolute():
            file_path = BASE_DIR / file_path
            
        if not file_path.exists():
            raise HTTPException(status_code=404, detail=f"File not found: {file_path}")
            
        try:
            # Parse metadata to get timeframe
            metadata = {}
            if entry.metadata_json:
                try:
                    metadata = json.loads(entry.metadata_json)
                except:
                    pass
            
            timeframe = metadata.get('timeframe', '1')
            if timeframe == '1m': timeframe = '1' # Normalize for the requested format
            
            # Load the data
            df = pl.read_parquet(str(file_path))
            
            # Transform columns: <TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>,...
            # The input columns likely are: timestamp, open, high, low, close, volume
            # We need to map them to the requested format.
            
            # Ensure timestamp exists and is datetime
            if "timestamp" in df.columns:
                df = df.with_columns([
                    pl.from_epoch("timestamp", time_unit="ms").alias("dt")
                ])
            elif "time" in df.columns:
                df = df.with_columns([
                    pl.from_epoch("time", time_unit="ms").alias("dt")
                ])
            else:
                # Fallback if no obvious timestamp column
                raise HTTPException(status_code=400, detail="Could not find timestamp column in Parquet")

            # Create the requested columns
            export_df = df.select([
                pl.lit(entry.symbol.replace("_", "")).alias("<TICKER>"),
                pl.lit(timeframe).alias("<PER>"),
                pl.col("dt").dt.strftime("%Y%m%d").alias("<DATE>"),
                pl.col("dt").dt.strftime("%H%M%S").alias("<TIME>"),
                pl.col("open").alias("<OPEN>"),
                pl.col("high").alias("<HIGH>"),
                pl.col("low").alias("<LOW>"),
                pl.col("close").alias("<CLOSE>"),
                pl.col("volume").alias("<VOL>")
            ])
            
            # Prepare export directory (trading-research/export/[exchange]/[market])
            exchange_name = entry.exchange.upper()
            market_name = entry.market.upper()
            export_dir = BASE_DIR.parent.parent / "export" / exchange_name / market_name
            export_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate filename: dl_[symbol]_[exchange]_[market]_[timeframe].csv.txt
            # Using requested format: [name]_[exchangeName]_[marketName]
            clean_symbol = entry.symbol.replace("_", "")
            filename = f"dl_{clean_symbol}_{exchange_name}_{market_name}.csv.txt"
            export_path = export_dir / filename
            
            # Write to CSV
            export_df.write_csv(str(export_path), include_header=True)
            
            return {
                "status": "success",
                "filename": filename,
                "path": str(export_path)
            }
            
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")



def _export_single_ticker(exchange: str, symbol: str, market: str | None, manager: ManifestManager):
    """
    Helper function to export a single ticker.
    Returns a dict with status and details, or raises exception on failure.
    """
    import polars as pl
    import json
    
    # List all entries for this ticker
    # Try with exact symbol and also symbol with underscore
    entries = manager.list_entries(exchange=exchange, symbol=symbol, market=market)
    if not entries:
        # Try alternate symbol format if underscores are present
        alt_symbol = symbol.replace("_", "") if "_" in symbol else symbol
        if alt_symbol != symbol:
             entries = manager.list_entries(exchange=exchange, symbol=alt_symbol, market=market)
             
    # Filter for 1m timeframe and 'raw' data
    valid_entries = []
    for entry in entries:
        if entry.type != 'raw':
            continue
            
        # Extract timeframe
        timeframe = None
        if entry.metadata_json:
            try:
                metadata = json.loads(entry.metadata_json)
                timeframe = metadata.get('timeframe')
            except:
                pass
        
        if not timeframe:
            try:
                path_parts = entry.path.replace('\\', '/').split('/')
                for i, part in enumerate(path_parts):
                    if part.lower() in ['raw', 'ticks', 'agg', 'feature'] and i + 1 < len(path_parts):
                        timeframe = path_parts[i+1]
                        break
            except:
                pass
        
        if timeframe in ['1m', '1']:
            valid_entries.append(entry)
            
    if not valid_entries:
        # Instead of raising HTTP exception immediately, we return None/False so batch process can skip
        # For single export, the caller should handle empty result
        return None
        
    try:
        # Load and concat all segments
        dfs = []
        for entry in valid_entries:
            file_path = Path(entry.path)
            if not file_path.is_absolute():
                file_path = BASE_DIR / file_path
            
            if file_path.exists():
                df = pl.read_parquet(str(file_path))
                
                # Normalize timestamp to 'dt'
                if "ts" in df.columns:
                    df = df.with_columns(pl.from_epoch("ts", time_unit="ms").alias("dt"))
                elif "timestamp" in df.columns:
                    df = df.with_columns(pl.from_epoch("timestamp", time_unit="ms").alias("dt"))
                elif "time" in df.columns:
                    df = df.with_columns(pl.from_epoch("time", time_unit="ms").alias("dt"))
                else:
                    continue # Skip if no timestamp
                    
                dfs.append(df)
                
        if not dfs:
             raise ValueError("No valid data found in parquet files")
             
        # Combine and sort
        full_df = pl.concat(dfs).sort("dt")
        
        # Transform to requested format
        # Use the symbol from the first valid entry to ensure consistency
        final_symbol = valid_entries[0].symbol
        export_df = full_df.select([
            pl.lit(final_symbol.replace("_", "")).alias("<TICKER>"),
            pl.lit("1").alias("<PER>"),
            pl.col("dt").dt.strftime("%Y%m%d").alias("<DATE>"),
            pl.col("dt").dt.strftime("%H%M%S").alias("<TIME>"),
            pl.col("open").alias("<OPEN>"),
            pl.col("high").alias("<HIGH>"),
            pl.col("low").alias("<LOW>"),
            pl.col("close").alias("<CLOSE>"),
            pl.col("volume").alias("<VOL>")
        ])
        
        # Prepare export directory (trading-research/export/[exchange]/[market])
        exchange_name = exchange.upper()
        market_name = (market or "AGGREGATED").upper()
        export_dir = BASE_DIR.parent.parent / "export" / exchange_name / market_name
        export_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename: dl_[symbol]_[exchange]_[market].csv.txt
        clean_symbol = final_symbol.replace("_", "")
        filename = f"dl_{clean_symbol}_{exchange_name}_{market_name}.csv.txt"
        export_path = export_dir / filename
        
        # Write to CSV
        export_df.write_csv(str(export_path), include_header=True)
        
        return {
            "status": "success",
            "filename": filename,
            "path": str(export_path),
            "rows_exported": len(export_df)
        }
        
    except Exception as e:
        raise e


@router.get("/export/{exchange}/{symbol}")
def export_ticker(exchange: str, symbol: str, market: str = None):
    manager = ManifestManager(db_path=MANIFEST_PATH)
    try:
        result = _export_single_ticker(exchange, symbol, market, manager)
        if not result:
            available_symbols = [e.symbol for e in manager.list_entries(exchange=exchange)[:5]]
            raise HTTPException(status_code=404, detail=f"No 1m raw data found for {symbol} on {exchange}. Found symbols: {available_symbols}")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")


@router.post("/export/batch")
def export_batch(exchange: str, market: str = None):
    """
    Batch export all tickers for a given exchange (and optionally market).
    """
    manager = ManifestManager(db_path=MANIFEST_PATH)
    
    # Get all unique symbols for this exchange
    # We can just list all entries and extract symbols
    entries = manager.list_entries(exchange=exchange, market=market)
    
    # Filter for those that likely have raw data
    raw_entries = [e for e in entries if e.type == 'raw']
    unique_symbols = sorted(list(set(e.symbol for e in raw_entries)))
    
    if not unique_symbols:
        return {
            "status": "warning",
            "message": "No symbols found with raw data",
            "exported_count": 0,
            "symbols_processed": []
        }
        
    exported = []
    failed = []
    skipped = []
    
    for sym in unique_symbols:
        try:
            # We pass the market from the entry if market was not specified globally, 
            # but _export_single_ticker handles market logic.
            # If market is None, we might export across markets for same symbol? 
            # Better to be specific if we can, but user request implies "all tickers".
            # For simplicity, if market is None, we let the helper find via list_entries, 
            # which might mix markets if symbol exists in multiple. 
            # Let's try to be precise if we have the info.
            
            # Use specific market for the symbol if available from our list
            sym_market = market
            if not sym_market:
                # Find the market for this symbol from our raw_entries
                # Just take the first one found
                for e in raw_entries:
                    if e.symbol == sym:
                        sym_market = e.market
                        break
            
            res = _export_single_ticker(exchange, sym, sym_market, manager)
            if res:
                exported.append(f"{sym} ({res['rows_exported']} rows)")
            else:
                skipped.append(sym)
                
        except Exception as e:
            failed.append(f"{sym}: {str(e)}")
            
    return {
        "status": "success",
        "total_symbols": len(unique_symbols),
        "exported_count": len(exported),
        "failed_count": len(failed),
        "skipped_count": len(skipped),
        "exported_details": exported,
        "failed_details": failed,
        "export_dir": str(BASE_DIR.parent.parent / "export" / exchange.upper())
    }



@router.delete("/datasets/{dataset_id}")
def delete_dataset(dataset_id: int):
    manager = ManifestManager(db_path=MANIFEST_PATH)
    with manager.Session() as session:
        from dl_datalake.metadata.manifest import ManifestEntry
        entry = session.query(ManifestEntry).get(dataset_id)
        if not entry:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        path = Path(entry.path)
        if not path.is_absolute():
            path = BASE_DIR / path
            
        if path.exists():
            path.unlink()
            
        session.delete(entry)
        session.commit()
        
    return {"status": "success"}
