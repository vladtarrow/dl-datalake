from fastapi import APIRouter, BackgroundTasks, HTTPException
from pathlib import Path
from datetime import UTC, datetime

from dl_datalake.ingest.exchange_connector import ExchangeConnector
from dl_datalake.ingest.pipeline import IngestPipeline
from dl_datalake.metadata.manifest import ManifestManager

from schemas import (
    DownloadRequest, 
    BulkDownloadRequest,
    IngestRequest, 
    TaskStatus, 
    ExchangeInfo, 
    ExchangeList, 
    MarketInfo, 
    MarketList, 
    SymbolList
)

router = APIRouter(prefix="/api/v1/ingest", tags=["ingest"])

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent / "dl-datalake"
MANIFEST_PATH = str(BASE_DIR / "manifest.db")
DATA_ROOT = str(BASE_DIR / "data")

import threading
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from loguru import logger
from typing import Dict, List, Any

class DownloadManager:
    """Manages concurrent downloads with per-exchange limits and tracking."""
    
    def __init__(self, max_concurrent_per_exchange: int = 3, max_total_workers: int = 10):
        self.semaphores = defaultdict(lambda: threading.Semaphore(max_concurrent_per_exchange))
        # Use a global thread pool for true parallel execution
        self.executor = ThreadPoolExecutor(max_workers=max_total_workers)
        # symbol -> {status: str, message: str, start_time: datetime}
        self.active_tasks: Dict[str, Dict[str, Any]] = {}
        # exchange_id -> ccxt_instance
        self.exchange_cache: Dict[str, Any] = {}
        # Shared manifest manager
        self.manifest = ManifestManager(db_path=MANIFEST_PATH)
        self.lock = threading.Lock()

    def submit_task(self, req: DownloadRequest):
        """Submit a task to the executor if not already running."""
        symbol = req.symbol
        exchange_id = req.exchange.lower()
        market_type = req.market.lower()
        # Include data type in key to allow parallel/separate status for raw vs funding
        task_key = f"{exchange_id}:{market_type}:{symbol}:{req.data_type}"
        
        with self.lock:
            if task_key in self.active_tasks:
                current_status = self.active_tasks[task_key].get("status")
                if current_status in ["pending", "running"]:
                    logger.info(f"Task for {task_key} already active ({current_status}), skipping.")
                    return
            
            # Record the task immediately so it shows up in the UI
            self.active_tasks[task_key] = {
                "status": "pending",
                "exchange": req.exchange,
                "market": req.market,
                "symbol": symbol,
                "data_type": req.data_type,
                "message": "Queued (ThreadPool)...",
                "start_time": datetime.now(UTC).isoformat()
            }
        
        self.executor.submit(self.run_safe_download, req)

    def run_safe_download(self, req: DownloadRequest):
        symbol = req.symbol
        exchange_id = req.exchange.lower()
        market_type = req.market.lower()
        task_key = f"{exchange_id}:{market_type}:{symbol}:{req.data_type}"
        
        with self.lock:
            # Update status to indicate it's now waiting for an exchange-specific slot
            if task_key in self.active_tasks:
                self.active_tasks[task_key]["message"] = "Waiting for exchange slot..."
            logger.info(f"Task processing started: {task_key}")

        try:
            with self.semaphores[exchange_id]:
                with self.lock:
                    self.active_tasks[task_key]["status"] = "running"
                    self.active_tasks[task_key]["message"] = "Fetching data..."
                    logger.info(f"Task started: {task_key} (Status: running)")
                
                # Reuse or create exchange instance for pooling
                import ccxt
                market_type = req.market.lower()
                cache_key = f"{exchange_id}_{market_type}"
                
                with self.lock:
                    if cache_key not in self.exchange_cache:
                        ex_class = getattr(ccxt, exchange_id)
                        ex = ex_class({
                            "enableRateLimit": True,
                            "options": {"defaultType": market_type}
                        })
                        # Explicitly load markets once for this instance to map symbols correctly
                        try:
                            ex.load_markets()
                        except Exception as e:
                            logger.error(f"Failed to load markets for {cache_key}: {e}")
                        self.exchange_cache[cache_key] = ex
                    exchange_instance = self.exchange_cache[cache_key]

                connector = ExchangeConnector(
                    exchange_id=req.exchange, 
                    market_type=req.market,
                    exchange_instance=exchange_instance
                )
                # Share the already initialized manifest/pipeline
                connector.pipeline = IngestPipeline(data_root=DATA_ROOT, db_path=MANIFEST_PATH)
                connector.pipeline.manifest = self.manifest
                
                def update_progress(count: int):
                    with self.lock:
                        if task_key in self.active_tasks:
                            self.active_tasks[task_key]["message"] = f"Fetched {count:,} candles..."
                
                # Execute requested download types
                if req.data_type in ["raw", "both"]:
                    # If full_history is requested, we ignore start_date and pass None 
                    # to trigger the 'probe' logic in ExchangeConnector
                    start_val = None if req.full_history else req.start_date
                    
                    connector.download_ohlcv(
                        symbol, 
                        req.timeframe, 
                        progress_callback=update_progress,
                        start_date=start_val
                    )
                
                is_derivative = any(der in req.market.lower() for der in ['future', 'swap', 'linear', 'derivative'])
                if (req.funding or req.data_type in ["funding", "both"]) and is_derivative:
                    # Update message for funding
                    with self.lock:
                        if task_key in self.active_tasks:
                            self.active_tasks[task_key]["message"] = "Fetching funding rates..."
                    connector.download_funding_rates(symbol)
                
                # Verification Step
                with self.lock:
                    if task_key in self.active_tasks:
                        self.active_tasks[task_key]["message"] = "Verifying integrity..."
                
                sanitized_symbol = symbol.replace("/", "_").replace(":", "_").upper()
                verify_result = connector.pipeline.verify_integrity(
                    exchange=req.exchange.lower(),
                    symbol=sanitized_symbol,
                    market=req.market.lower(),
                    timeframe=req.timeframe
                )
                
                with self.lock:
                    if task_key in self.active_tasks:
                        if verify_result["status"] == "success":
                            self.active_tasks[task_key]["status"] = "completed"
                            self.active_tasks[task_key]["message"] = "Finished (Verified)"
                            logger.info(f"Task completed: {task_key} - {verify_result['message']}")
                        elif verify_result["status"] == "warning":
                            self.active_tasks[task_key]["status"] = "completed"
                            self.active_tasks[task_key]["message"] = f"Finished. {verify_result['message']}"
                            # This will go to datalake.log because level is WARNING
                            logger.warning(f"Data integrity warning for {task_key}: {verify_result['message']}")
                        else:
                            self.active_tasks[task_key]["status"] = "completed"
                            self.active_tasks[task_key]["message"] = f"Finished. Verify error: {verify_result['message']}"
                            
                            # If no files found, it might be expected (e.g. empty response from exchange)
                            if verify_result.get("message") == "No files found to verify":
                                logger.warning(f"Verification: No data files found for {task_key}. Possibly no data was downloaded.")
                            else:
                                # This will go to datalake.log because level is ERROR
                                logger.error(f"Verification system error for {task_key}: {verify_result['message']}")
                    
        except Exception as e:
            logger.error(f"Error during download for {task_key}: {e}")
            with self.lock:
                if task_key in self.active_tasks:
                    self.active_tasks[task_key]["status"] = "failed"
                    self.active_tasks[task_key]["message"] = f"Error: {str(e)}"
        finally:
            # Final safety check: if we somehow exited without updating status
            with self.lock:
                if task_key in self.active_tasks and self.active_tasks[task_key]["status"] == "running":
                    self.active_tasks[task_key]["status"] = "failed"
                    self.active_tasks[task_key]["message"] = "Terminated unexpectedly"

    def get_status(self) -> Dict[str, Any]:
        with self.lock:
            return {k: v for k, v in self.active_tasks.items()}

download_manager = DownloadManager(max_concurrent_per_exchange=5, max_total_workers=20)

@router.get("/status")
def get_download_status():
    return download_manager.get_status()

@router.post("/download", response_model=TaskStatus)
def download_data(req: DownloadRequest):
    download_manager.submit_task(req)
    return TaskStatus(
        task_id=f"dl_{req.symbol}", 
        status="pending", 
        message=f"Queued download for {req.symbol} from {req.exchange}"
    )

@router.post("/bulk-download", response_model=TaskStatus)
def bulk_download_data(req: BulkDownloadRequest):
    for symbol in req.symbols:
        single_req = DownloadRequest(
            exchange=req.exchange,
            symbol=symbol,
            market=req.market,
            timeframe=req.timeframe,
            futures=req.futures,
            funding=req.funding,
            start_date=req.start_date,
            data_type=req.data_type
        )
        download_manager.submit_task(single_req)
        
    return TaskStatus(
        task_id="bulk_dl", 
        status="pending", 
        message=f"Queued {len(req.symbols)} downloads from {req.exchange}"
    )

@router.get("/exchanges", response_model=ExchangeList)
def list_exchanges():
    import ccxt
    # Return all exchanges supported by CCXT
    return ExchangeList(exchanges=[
        ExchangeInfo(id=ex, name=ex.capitalize()) for ex in ccxt.exchanges
    ])

@router.get("/exchanges/{exchange_id}/markets", response_model=MarketList)
def list_markets(exchange_id: str):
    import ccxt
    try:
        ex_class = getattr(ccxt, exchange_id.lower())
        ex = ex_class()
        markets_data = ex.load_markets()
        
        # Extract unique market types (spot, future, swap, etc.)
        types = set()
        for m in markets_data.values():
            m_type = m.get('type')
            if m_type:
                types.add(m_type)
        
        return MarketList(markets=[
            MarketInfo(id=t, name=t.capitalize()) for t in sorted(list(types))
        ])
    except Exception as e:
        # Fallback for unexpected errors or if exchange class not found
        return MarketList(markets=[
            MarketInfo(id="spot", name="Spot"),
            MarketInfo(id="future", name="Future")
        ])

@router.get("/exchanges/{exchange_id}/symbols", response_model=SymbolList)
def list_symbols(exchange_id: str, market: str = "spot"):
    import ccxt
    try:
        ex_class = getattr(ccxt, exchange_id.lower())
        # Use the specific market type in options if possible, 
        # but load_markets + filter is more reliable for discovery
        ex = ex_class()
        markets = ex.load_markets()
        
        symbols = [
            s for s, m in markets.items() 
            if m.get("active", True) and m.get("type") == market
        ]
        return SymbolList(symbols=sorted(symbols))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch symbols: {str(e)}")

@router.post("/file", response_model=TaskStatus)
def ingest_file(req: IngestRequest):
    if not Path(req.source_path).exists():
        raise HTTPException(status_code=400, detail="Source file not found")
        
    pipeline = IngestPipeline(data_root=DATA_ROOT, db_path=MANIFEST_PATH)
    pipeline.ingest_csv(req.source_path, req.exchange, req.market, req.symbol)
    
    # Verification
    # Note: ingest_csv currently defaults to '1m' in the manifest entry
    sanitized_symbol = req.symbol.replace("/", "_").replace(":", "_").upper()
    verify_result = pipeline.verify_integrity(
        exchange=req.exchange.lower(),
        symbol=sanitized_symbol,
        market=req.market.lower(),
        timeframe="1m" 
    )
    
    msg = f"Successfully ingested {req.symbol}."
    if verify_result["status"] == "success":
        msg += " Data verified."
        logger.info(f"File ingestion completed: {req.symbol} - {verify_result['message']}")
    elif verify_result["status"] == "warning":
        msg += f" {verify_result['message']}"
        logger.warning(f"File ingestion completed with warnings: {req.symbol} - {verify_result['message']}")
    else:
        msg += f" {verify_result['message']}"
        logger.error(f"File ingestion failed verification: {req.symbol} - {verify_result['message']}")
    
    return TaskStatus(
        task_id="ingest", 
        status="completed", 
        message=msg
    )

@router.delete("/exchanges/{exchange_id}/markets/{market_id}/history")
def delete_symbol_history(
    exchange_id: str,
    market_id: str, 
    symbol: str, 
    data_type: str | None = None
):
    """Delete history for a symbol. Use query param for symbol to handle slashes correctly."""
    pipeline = IngestPipeline(data_root=DATA_ROOT, db_path=MANIFEST_PATH)
    
    # Sanitize symbol to match Manifest storage format (underscores, upper)
    # Note: ExchangeConnector sanitizes before saving
    sanitized_symbol = symbol.replace("/", "_").replace(":", "_").upper()
    
    deleted_paths = pipeline.manifest.delete_entries(
        symbol=sanitized_symbol,
        exchange=exchange_id,
        market=market_id,
        data_type=data_type
    )
    
    # Optionally delete files? 
    # The user asked to "clear history", which usually implies data gone.
    # Safe option: delete the files if they exist.
    count = 0
    for p in deleted_paths:
        try:
            path = Path(p)
            if path.exists():
                path.unlink()
                count += 1
        except Exception as e:
            logger.error(f"Failed to delete file {p}: {e}")
            
    return {
        "status": "success", 
        "deleted_entries": len(deleted_paths),
        "deleted_files": count,
        "message": f"Deleted {len(deleted_paths)} entries and {count} files for {symbol}"
    }
