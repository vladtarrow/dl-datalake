from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


class Symbol(BaseModel):
    exchange: str
    symbol: str
    market_type: str = "futures"


class Dataset(BaseModel):
    id: str  # e.g. "binance/BTCUSDT/1m"
    exchange: str
    symbol: str
    market: str
    timeframe: Optional[str] = None
    data_type: str
    file_path: str
    file_size_bytes: int
    last_modified: datetime
    time_from: Optional[datetime] = None
    time_to: Optional[datetime] = None


class DatasetList(BaseModel):
    datasets: List[Dataset]
    total: int


class DownloadRequest(BaseModel):
    exchange: str = "binance"
    symbol: str
    market: str = "spot"
    days: Optional[int] = None
    timeframe: str = "1m"
    futures: bool = True
    funding: bool = False
    start_date: Optional[str] = None
    full_history: bool = True
    data_type: str = "raw"  # "raw", "funding", "both"


class BulkDownloadRequest(BaseModel):
    exchange: str = "binance"
    symbols: List[str]
    market: str = "spot"
    timeframe: str = "1m"
    futures: bool = True
    funding: bool = False
    start_date: Optional[str] = None
    full_history: bool = True
    data_type: str = "raw"


class IngestRequest(BaseModel):
    source_path: str
    exchange: str
    symbol: str
    market: str = "futures"


class TaskStatus(BaseModel):
    task_id: str
    status: str
    message: Optional[str] = None


class DataPreview(BaseModel):
    columns: List[str]
    rows: List[dict]
    total_rows: int
    metadata: Optional[dict] = None

class ExchangeInfo(BaseModel):
    id: str
    name: str

class ExchangeList(BaseModel):
    exchanges: List[ExchangeInfo]

class MarketInfo(BaseModel):
    id: str
    name: str

class MarketList(BaseModel):
    markets: List[MarketInfo]

class SymbolList(BaseModel):
    symbols: List[str]
