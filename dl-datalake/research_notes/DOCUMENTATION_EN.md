# Technical Documentation: Local Data Lake (dl-datalake)

## 1. Introduction
**dl-datalake** is a high-efficiency system for storing, managing, and accessing market data on local hardware. Designed to operate under resource-limited conditions (2–4 GB RAM), it supports terabyte-scale volumes through efficient partitioning.

---

## 2. System Architecture

### 2.1 Storage Layer
- **Format**: Apache Parquet with `snappy` compression.
- **Partitioning**: Data is organized by directory structure:
  `data/{EXCHANGE}/{MARKET}/{SYMBOL}/{TYPE}/{PERIOD}/{YYYY}/{MM}/{DD}/`
  This enables rapid data retrieval for specific days without scanning the entire database.
- **Query Engine**: Reading is performed via **DuckDB** (or Polars), ensuring SQL-like performance without loading data entirely into RAM.

### 2.2 Metadata Registry (Manifest)
- **Backend**: SQLite.
- **Role**: Registers every file, storing time ranges (`time_from`, `time_to`), data versions, and checksums (SHA-256). This guarantees data integrity and traceability (data lineage).

### 2.3 Orchestration
- **Engine**: Prefect v2.
- **Tasks**:
  - `ingest`: Loading raw CSV data in chunks.
  - `aggregate`: Creating higher timeframes (15m, 1h) from 1m data.
  - `maintenance`: Compacting small files and cleanup.

---

## 3. Installation and Setup

### Requirements
- Python 3.10+
- Operating System: Windows / Linux / macOS

### Quick Start
```bash
# Clone and install dependencies
cd src/dl-datalake
pip install -r requirements.txt

# Initialize the metadata database
python -m dl_datalake.cli init
```

---

## 4. Usage Instructions

### 4.1 CLI (Command Line Interface)

**Download symbols:**
```bash
# List all Binance Futures (default)
python -m dl_datalake.cli download-symbols --futures

# List all Bybit Futures
python -m dl_datalake.cli download-symbols --exchange bybit --futures
```

**Download history:**
```bash
# History from Bybit
python -m dl_datalake.cli download-history --exchange bybit --symbol BTC/USDT:USDT --days 7 --futures

# Minutes and funding for BTCUSDT on Binance
python -m dl_datalake.cli download-history --exchange binance --symbol BTCUSDT --days 30 --futures --funding
```

**Ingest local files:**
```bash
python -m dl_datalake.cli ingest --source btc_1m.csv --exchange BINANCE --symbol BTCUSDT
```

**Delete data:**
```bash
# Delete ALL data for BTCUSDT
python -m dl_datalake.cli delete --symbol BTCUSDT

# Delete only aggregated data (15m, 1h)
python -m dl_datalake.cli delete --symbol BTCUSDT --data-type agg
```

**Read data (view):**
```bash
python -m dl_datalake.cli read --exchange BINANCE --symbol BTCUSDT --start 2026-01-01 --end 2026-01-07
```

**Register features:**
```bash
python -m dl_datalake.cli upload-feature --path my_features.parquet --exchange BINANCE --symbol BTCUSDT --feature-set alpha_v1 --version 1.0.1
```

### 4.2 Python SDK (DataLakeClient)
A convenient interface for integration into algorithmic strategies and ML pipelines.

```python
from dl_datalake.client.dl_client import DataLakeClient

client = DataLakeClient()

# Get data as a DataFrame
df = client.read_ohlc(
    exchange="BINANCE",
    symbol="BTCUSDT",
    start_date="2026-01-01",
    end_date="2026-01-10"
)

# List all features for a symbol
features = client.list(symbol="BTCUSDT", data_type="feature")
```

### 4.3 REST API
For usage in distributed systems or from other programming languages (JS, C++).

```bash
# Start server
uvicorn dl_datalake.client.api_server:app --reload

# Usage examples:
# 1. All records for BTCUSDT from all exchanges
curl "http://localhost:8000/list?symbol=BTCUSDT"

# 2. Only BTCUSDT from Bybit
curl "http://localhost:8000/list?symbol=BTCUSDT&exchange=bybit"

# 3. Read data
curl "http://localhost:8000/read?exchange=binance&symbol=BTCUSDT&start=2026-01-01&end=2026-01-31"
```

---

## 5. Feature Store and Versioning
The system supports storing result sets (indicators, signals, embeddings).
- All features are stored separately from market data in `data/features/`.
- Every feature upload requires a version (`version`).
- Version metadata is stored in the manifest, allowing quick switching between different strategy versions during backtesting.

---

## 6. Operational Recommendations
1. **RAM**: For large data volumes (>10 GB CSV), use the built-in chunking mechanism in `IngestPipeline`.
2. **Safety**: The `manifest.db` file is mission-critical. Regular backups are highly recommended.
3. **Performance**: For maximum read speed, use SQL queries via `ParquetReader` directly if the `DataLakeClient` capabilities are insufficient.

---

## 7. Development and Extension
- **New Data Types**: Add the corresponding method to `ParquetWriter` and register the type in `DataType` (metadata).
- **New Aggregations**: Define a new Prefect Task in `orchestration/flows.py`.
