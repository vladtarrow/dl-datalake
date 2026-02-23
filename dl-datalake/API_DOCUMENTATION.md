# DL-Datalake REST API Documentation

Full REST API reference for dl-datalake.

## Table of Contents

1. [Overview](#overview)
2. [Technical Details](#technical-details)
3. [Core API](#core-api)
4. [UI API](#ui-api)
5. [Usage Examples](#usage-examples)
6. [Error Codes](#error-codes)

---

## Overview

The project exposes two API servers:

1. **Core API** (`api_server.py`) — base REST API
   - Base URL: `http://localhost:8000`
   - Start: `uvicorn dl_datalake.client.api_server:app --reload`

2. **UI API** (`dl-datalake-ui/backend`) — extended API with additional capabilities
   - Base URL: `http://localhost:8000`
   - Prefix: `/api/v1`
   - Start: `uvicorn main:app --reload --port 8000`

Interactive documentation is always available at:
- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`

---

## Technical Details

### Rate Limiting

The system automatically handles exchange rate limits:
- **429 (Too Many Requests)**: On `ccxt.DDoSProtection`, the system pauses (30 s) and retries.
- **Retry logic**: Up to 3 retries for listing-date probing and up to 5 retries for the main download loop.
- **IP Bans (418)**: Continuously ignoring Retry-After can result in an IP ban. The system respects exchange recommendations.

### Data Format: Parquet

All market data is stored in **Apache Parquet** format — a binary columnar format that provides:
- **Compression**: 5–10× smaller than CSV.
- **Speed**: Read only required columns (e.g., `close` for indicators) without loading the whole file.
- **Metadata**: Min/Max column statistics stored per file for predicate pushdown.

**Internal structure:**
- **Header**: Magic bytes `PAR1`.
- **Row Groups**: Data split into groups for parallel processing.
- **Column Chunks**: Columns stored physically separate within each row group.
- **Footer**: Schema and statistics for query optimization.

---

## Core API

Base endpoints for data access.

### 1. Health Check

**GET** `/health`

Check API health.

**Example:**
```bash
curl "http://localhost:8000/health"
```

**Response:**
```json
{ "status": "ok" }
```

---

### 2. List Manifest Entries

**GET** `/list`

List all entries in the manifest with optional filters.

**Query parameters:**
- `symbol` (optional): Filter by symbol (e.g. `BTCUSDT`)
- `data_type` (optional): Filter by type (`raw`, `ticks`, `agg`, `alt`, or feature set name)
- `exchange` (optional): Filter by exchange (e.g. `BINANCE`)
- `market` (optional): Filter by market type (`SPOT`, `FUTURES`, etc.)

**Examples:**
```bash
# All entries for BTCUSDT
curl "http://localhost:8000/list?symbol=BTCUSDT"

# Raw SPOT data only
curl "http://localhost:8000/list?symbol=BTCUSDT&data_type=raw&market=SPOT"
```

**Response:**
```json
[
  {
    "id": 1,
    "symbol": "BTCUSDT",
    "exchange": "BINANCE",
    "market": "SPOT",
    "path": "data/BINANCE/SPOT/BTCUSDT/raw/1m/2024/01/15/BTCUSDT_1m_20240115.parquet",
    "type": "raw"
  }
]
```

---

### 3. Read Data

**GET** `/read`

Read market data for a given time range.

**Query parameters:**
- `exchange` (required): Exchange (e.g. `BINANCE`)
- `symbol` (required): Symbol (e.g. `BTCUSDT`)
- `start` (required): Start date `YYYY-MM-DD`
- `end` (required): End date `YYYY-MM-DD`
- `data_type` (optional): Data type (`raw`, `agg`, `ticks`, `alt`). Default: `"raw"`

**Example:**
```bash
curl "http://localhost:8000/read?exchange=BINANCE&symbol=BTCUSDT&start=2024-01-01&end=2024-01-31"
```

**Response:**
```json
[
  { "ts": 1704067200000, "open": 42000.0, "high": 42500.0, "low": 41800.0, "close": 42300.0, "volume": 1234.56 },
  { "ts": 1704067260000, "open": 42300.0, "high": 42400.0, "low": 42200.0, "close": 42350.0, "volume": 987.65 }
]
```

---

## Feature Store API (Core API)

### 4. List All Features

**GET** `/features`

List all registered features with version info.

**Query parameters:**
- `exchange` (optional)
- `symbol` (optional)
- `market` (optional)
- `feature_set` (optional)
- `version` (optional)

**Example:**
```bash
curl "http://localhost:8000/features?symbol=BTCUSDT"
```

**Response:**
```json
[
  {
    "id": 123,
    "exchange": "BINANCE",
    "symbol": "BTCUSDT",
    "market": "SPOT",
    "feature_set": "rsi_indicators",
    "version": "1.0.0",
    "path": "data/features/rsi_indicators/1.0.0/features.parquet",
    "checksum": "abc123...",
    "created_at": "2024-01-15T10:30:00"
  }
]
```

---

### 5. List Feature Sets with Versions

**GET** `/features/sets`

Get a grouped list of all feature sets and their available versions.

**Query parameters:** `exchange`, `symbol`, `market` (all optional)

**Example:**
```bash
curl "http://localhost:8000/features/sets?symbol=BTCUSDT"
```

**Response:**
```json
{
  "feature_sets": [
    {
      "name": "rsi_indicators",
      "exchange": "BINANCE",
      "symbol": "BTCUSDT",
      "market": "SPOT",
      "versions": ["2.0.0", "1.0.0"]
    }
  ]
}
```

---

### 6. Upload Feature

**POST** `/features/upload`

Upload a feature file to the Feature Store.

**Body (multipart/form-data):**
- `file` (required): File to upload (any format)
- `exchange` (required)
- `market` (required)
- `symbol` (required)
- `feature_set` (required): Feature set name
- `version` (optional): Default `"1.0.0"`

**Example (curl):**
```bash
curl -X POST "http://localhost:8000/features/upload" \
  -F "file=@./my_features.parquet" \
  -F "exchange=BINANCE" \
  -F "market=SPOT" \
  -F "symbol=BTCUSDT" \
  -F "feature_set=rsi_indicators" \
  -F "version=1.0.0"
```

**Example (Python):**
```python
import requests

response = requests.post(
    "http://localhost:8000/features/upload",
    files={"file": open("my_features.parquet", "rb")},
    data={"exchange": "BINANCE", "market": "SPOT", "symbol": "BTCUSDT",
          "feature_set": "rsi_indicators", "version": "1.0.0"},
)
print(response.json())
```

**Response:**
```json
{ "status": "success", "version": "1.0.0", "message": "Feature rsi_indicators v1.0.0 uploaded successfully" }
```

---

### 7. Get Feature Metadata

**GET** `/features/{feature_id}`

Get metadata for a specific feature by its manifest ID.

**Example:**
```bash
curl "http://localhost:8000/features/123"
```

**Response:**
```json
{
  "id": 123,
  "exchange": "BINANCE",
  "symbol": "BTCUSDT",
  "market": "SPOT",
  "feature_set": "rsi_indicators",
  "version": "1.0.0",
  "file_path": "data/features/rsi_indicators/1.0.0/features.parquet",
  "file_size_bytes": 1024000,
  "checksum": "abc123...",
  "created_at": "2024-01-15T10:30:00"
}
```

---

### 8. Download Feature

**GET** `/features/{feature_id}/download`

Download a feature file by ID.

```bash
curl "http://localhost:8000/features/123/download" -o downloaded_feature.parquet
```

Response: binary file stream with download headers.

---

### 9. Delete Feature

**DELETE** `/features/{feature_id}`

Delete a feature by ID (removes both the file and the manifest entry).

```bash
curl -X DELETE "http://localhost:8000/features/123"
```

**Response:**
```json
{ "status": "success", "message": "Feature rsi_indicators v1.0.0 deleted" }
```

---

## UI API

Extended API for the web interface. All endpoints are prefixed with `/api/v1`.

### 10. List Datasets

**GET** `/api/v1/datasets`

List all datasets (market data + features) with pagination and filtering.

**Query parameters:**
- `exchange` (optional): e.g. `BINANCE` — **case-sensitive, use UPPER CASE**
- `symbol` (optional): e.g. `BTCUSDT`
- `market` (optional): `SPOT`, `FUTURES`, `LINEAR`, `SWAP`
- `data_type` (optional): `raw`, `ticks`, `agg`, `alt`, or feature set name
- `limit` (optional): Records per page. Default `20`
- `offset` (optional): Pagination offset. Default `0`

> [!TIP]
> Values for `exchange`, `symbol`, and `market` are stored in UPPER CASE in the database. If a query returns no results, try `exchange=BINANCE` instead of `exchange=binance`.

**Examples:**
```bash
# All raw BTC data on Binance
curl "http://localhost:8000/api/v1/datasets?exchange=BINANCE&symbol=BTCUSDT&data_type=raw"

# Next page (records 21–40)
curl "http://localhost:8000/api/v1/datasets?exchange=BINANCE&offset=20&limit=20"
```

**Response:**
```json
{
  "datasets": [
    {
      "id": "1",
      "exchange": "BINANCE",
      "symbol": "BTCUSDT",
      "market": "SPOT",
      "timeframe": "1m",
      "data_type": "raw",
      "file_path": "/path/to/file.parquet",
      "file_size_bytes": 1024000,
      "last_modified": "2024-01-15T10:30:00",
      "time_from": "2024-01-01T00:00:00",
      "time_to": "2024-01-31T23:59:59"
    }
  ],
  "total": 1
}
```

---

### 11. Preview Dataset

**GET** `/api/v1/datasets/{dataset_id}/preview`

Get a preview of a dataset (first N rows).

**Query parameters:**
- `limit` (optional): Number of rows. Default `100`
- `offset` (optional): Row offset. Default `0`

**Example:**
```bash
curl "http://localhost:8000/api/v1/datasets/1/preview?limit=50"
```

**Response:**
```json
{
  "columns": ["ts", "open", "high", "low", "close", "volume"],
  "rows": [
    { "ts": 1704067200000, "open": 42000.0, "high": 42500.0, "low": 41800.0, "close": 42300.0, "volume": 1234.56 }
  ],
  "total_rows": 44640,
  "metadata": { "timeframe": "1m" }
}
```

---

### 12. Export Dataset

**GET** `/api/v1/datasets/{dataset_id}/export`

Export a dataset segment as a CSV file (compatible with trading terminals).

```bash
curl "http://localhost:8000/api/v1/datasets/1/export"
```

**Response:**
```json
{
  "status": "success",
  "filename": "dl_BTCUSDT_BINANCE_SPOT.csv.txt",
  "path": "/path/to/export/BINANCE/SPOT/dl_BTCUSDT_BINANCE_SPOT.csv.txt"
}
```

---

### 13. Delete Dataset

**DELETE** `/api/v1/datasets/{dataset_id}`

Delete a dataset by ID.

```bash
curl -X DELETE "http://localhost:8000/api/v1/datasets/1"
```

**Response:** `{ "status": "success" }`

---

## Ingestion API (UI API)

### 14. Ingestion Status

**GET** `/api/v1/ingest/status`

Get status of all active downloads.

**Response:**
```json
{
  "binance:spot:BTCUSDT:raw": {
    "status": "running",
    "exchange": "binance",
    "market": "spot",
    "symbol": "BTCUSDT",
    "data_type": "raw",
    "message": "Fetched 5,000 candles...",
    "start_time": "2024-01-15T10:30:00Z"
  }
}
```

---

### 15. Download Historical Data

**POST** `/api/v1/ingest/download`

Start a historical data download from an exchange.

**Body (JSON):**
```json
{
  "exchange": "binance",
  "symbol": "BTCUSDT",
  "market": "spot",
  "timeframe": "1m",
  "data_type": "raw",
  "start_date": "2024-01-01",
  "full_history": false
}
```

**Parameters:**
- `exchange` (required): Exchange ID (binance, bybit, etc.)
- `symbol` (required): Trading symbol
- `market` (optional): Market type (spot, future, etc.)
- `timeframe` (optional): Candle timeframe. Default `"1m"`
- `data_type` (optional): `raw`, `funding`, or `both`
- `start_date` (optional): `YYYY-MM-DD`
- `full_history` (optional): If `true`, ignores `start_date` and downloads from listing date

**Example:**
```bash
curl -X POST "http://localhost:8000/api/v1/ingest/download" \
  -H "Content-Type: application/json" \
  -d '{"exchange": "binance", "symbol": "BTCUSDT", "market": "spot", "start_date": "2024-01-01"}'
```

**Response:**
```json
{ "task_id": "dl_BTCUSDT", "status": "pending", "message": "Queued download for BTCUSDT from binance" }
```

---

### 16. Bulk Download

**POST** `/api/v1/ingest/bulk-download`

Start downloads for multiple symbols simultaneously.

**Body:**
```json
{
  "exchange": "binance",
  "symbols": ["BTCUSDT", "ETHUSDT"],
  "market": "spot",
  "timeframe": "1m",
  "data_type": "raw",
  "start_date": "2024-01-01"
}
```

**Response:**
```json
{ "task_id": "bulk_dl", "status": "pending", "message": "Queued 2 downloads from binance" }
```

---

### 17. List Exchanges

**GET** `/api/v1/ingest/exchanges`

List all available exchanges (from CCXT).

**Response:**
```json
{ "exchanges": [{ "id": "binance", "name": "Binance" }, { "id": "bybit", "name": "Bybit" }] }
```

---

### 18. List Markets for Exchange

**GET** `/api/v1/ingest/exchanges/{exchange_id}/markets`

List available market types for an exchange.

---

### 19. List Symbols for Exchange

**GET** `/api/v1/ingest/exchanges/{exchange_id}/symbols`

List all active symbols on an exchange.

**Query parameters:**
- `market` (optional): Market type. Default `"spot"`

---

### 20. Upload Local File

**POST** `/api/v1/ingest/file`

Import data from a local CSV file into the lake.

---

### 21. Aggregated Export (full ticker)

**GET** `/api/v1/export/{exchange}/{symbol}`

Find all data fragments (1m raw) for a ticker, merge them in order, and export as one large CSV file.

**Query parameters:**
- `market` (optional): Market type

**Example:**
```bash
curl "http://localhost:8000/api/v1/export/binance/BTCUSDT?market=spot"
```

**Response:**
```json
{
  "status": "success",
  "filename": "dl_BTCUSDT_BINANCE_SPOT.csv.txt",
  "path": "/path/to/export/BINANCE/SPOT/dl_BTCUSDT_BINANCE_SPOT.csv.txt",
  "rows_exported": 525600
}
```

---

### 22. Delete History

**DELETE** `/api/v1/ingest/exchanges/{exchange_id}/markets/{market_id}/history`

Fully delete data and manifest entries for a specific symbol.

**Query parameters:**
- `symbol` (required)
- `data_type` (optional)

**Example:**
```bash
curl -X DELETE "http://localhost:8000/api/v1/ingest/exchanges/binance/markets/spot/history?symbol=BTCUSDT"
```

**Response:**
```json
{ "status": "success", "deleted_entries": 12, "deleted_files": 12, "message": "Deleted 12 entries and 12 files for BTCUSDT" }
```

---

## Feature Store API (UI API)

All feature endpoints are also available in the UI API under `/api/v1/features`:

- `GET /api/v1/features` — list all features (with pagination)
- `GET /api/v1/features/sets` — list feature sets with versions
- `POST /api/v1/features/upload` — upload a feature file
- `GET /api/v1/features/{id}` — get feature metadata
- `GET /api/v1/features/{id}/download` — download a feature file
- `DELETE /api/v1/features/{id}` — delete a feature

See [Feature Store API (Core API)](#feature-store-api-core-api) for details.

---

## Usage Examples

### Full Data Workflow

```python
import requests
import polars as pl

BASE_URL = "http://localhost:8000/api/v1"

# 1. List exchanges
exchanges = requests.get(f"{BASE_URL}/ingest/exchanges").json()
print(f"Available exchanges: {len(exchanges['exchanges'])}")

# 2. List symbols
symbols = requests.get(
    f"{BASE_URL}/ingest/exchanges/binance/symbols",
    params={"market": "spot"}
).json()
print(f"Symbols on Binance: {len(symbols['symbols'])}")

# 3. Start download
download_req = {
    "exchange": "binance",
    "symbol": "BTCUSDT",
    "market": "spot",
    "timeframe": "1m",
    "start_date": "2024-01-01"
}
response = requests.post(f"{BASE_URL}/ingest/download", json=download_req)
print(response.json())

# 4. Check download status
status = requests.get(f"{BASE_URL}/ingest/status").json()
print(status)

# 5. List downloaded datasets
datasets = requests.get(
    f"{BASE_URL}/datasets",
    params={"symbol": "BTCUSDT", "limit": 100}
).json()
print(f"Datasets found: {datasets['total']}")

# 6. Preview data
if datasets['datasets']:
    preview = requests.get(
        f"{BASE_URL}/datasets/{datasets['datasets'][0]['id']}/preview",
        params={"limit": 10}
    ).json()
    print(f"Columns: {preview['columns']}")
    print(f"Total rows: {preview['total_rows']}")

# 7. Upload features
with open("my_features.parquet", "rb") as f:
    response = requests.post(
        f"{BASE_URL}/features/upload",
        files={"file": f},
        data={"exchange": "BINANCE", "market": "SPOT", "symbol": "BTCUSDT",
              "feature_set": "rsi_indicators", "version": "1.0.0"},
    )
    print(response.json())

# 8. List features
features = requests.get(f"{BASE_URL}/features", params={"symbol": "BTCUSDT"}).json()
print(f"Features found: {features['total']}")

# 9. List feature sets with versions
sets = requests.get(f"{BASE_URL}/features/sets", params={"symbol": "BTCUSDT"}).json()
for fs in sets["feature_sets"]:
    print(f"{fs['name']}: versions {fs['versions']}")

# 10. Download a feature
if features.get('datasets'):
    feature_id = features['datasets'][0]['id']
    response = requests.get(f"{BASE_URL}/features/{feature_id}/download")
    with open("downloaded.parquet", "wb") as f:
        f.write(response.content)
    print("Feature downloaded")
```

---

## Error Codes

### Success

| Code | Meaning |
|---|---|
| `200 OK` | Request succeeded |
| `201 Created` | Resource created |

### Client Errors

| Code | Meaning |
|---|---|
| `400 Bad Request` | Invalid request parameters |
| `404 Not Found` | Resource not found |
| `422 Unprocessable Entity` | Validation error |

### Server Errors

| Code | Meaning |
|---|---|
| `500 Internal Server Error` | Unexpected server error |

**Error response format:**
```json
{ "detail": "Feature not found" }
```

---

## Notes

1. **Date formats**: All dates use ISO 8601 (`YYYY-MM-DD` or `YYYY-MM-DDTHH:MM:SS`)
2. **Pagination**: UI API supports `limit` and `offset` for large result sets
3. **File formats**: Feature Store accepts any format (Parquet, CSV, JSON, etc.)
4. **Versioning**: Feature version is a free-form string (e.g. `"1.0.0"`, `"v2"`, `"2024.01.15"`)
5. **Async operations**: Exchange downloads run asynchronously — poll `/ingest/status` to track progress
6. **Checksums**: Automatically computed on feature upload for integrity verification
7. **CORS**: UI API allows all origins for local development
