# dl-datalake · Local Market Data Lake

[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/release/python-3120/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![Tests](https://img.shields.io/badge/tests-pytest-green.svg)](https://pytest.org)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A **production-grade local data lake** for storing and managing market data (crypto / forex / equities). Designed to run efficiently on commodity hardware (2–4 GB RAM) using columnar storage, smart partitioning, and an incremental-update pipeline.

> **Core goal:** Reliable ingestion, deduplication, and retrieval of historical OHLCV, tick, and custom feature data — without computing features internally.

---

## Architecture

```
dl-datalake/
├── src/dl_datalake/
│   ├── cli.py              # CLI entrypoint  (`dl` command)
│   ├── ingest/             # Exchange connectors (CCXT), CSV ingest
│   │   ├── exchange_connector.py
│   │   └── pipeline.py
│   ├── storage/            # Parquet writer/reader with UPSERT logic
│   ├── metadata/           # SQLite manifest (file registry)
│   ├── orchestration/      # Prefect flows
│   ├── features/           # Feature Store (register external features)
│   ├── client/             # Python client + REST API server
│   └── api/                # FastAPI routers
├── tests/
│   └── integration/        # 12 integration test suites
├── Dockerfile
└── Makefile
```

### Storage Layout

Files are partitioned hierarchically for efficient range queries:

```
data/
└── {EXCHANGE}/
    └── {MARKET}/
        └── {SYMBOL}/
            └── {type}/
                └── {period}/
                    └── {YYYY}/
                        └── {MM}/
                            └── BTCUSDT_1m_202401.parquet
```

### Tech Stack

| Layer | Technology |
|---|---|
| Storage format | Apache Parquet (PyArrow / Polars) |
| Metadata registry | SQLite (`manifest.db`) |
| Exchange connectivity | CCXT (100+ exchanges) |
| Data processing | Polars (fast, memory-efficient) |
| Orchestration | Prefect |
| REST API | FastAPI + Uvicorn |
| Linting / formatting | Ruff, Black |
| Testing | Pytest + Coverage |

---

## Key Features

- **Smart incremental updates** — resumes from the last known timestamp in the manifest; never re-downloads existing data
- **Atomic writes** — write-to-temp-then-rename prevents corrupt files on crash
- **UPSERT/merge logic** — safely merges new data with existing Parquet files, deduplicates, and re-sorts by timestamp
- **Data integrity checks** — verifies row count and timestamp monotonicity after every write
- **Continuity monitoring** — logs gaps and overlaps between downloaded batches
- **Rate-limit handling** — automatic 429/DDoSProtection detection with exponential backoff and retry
- **Feature Store** — version-controlled storage for externally computed feature sets
- **Dual interface** — CLI (`dl`) and Python client API
- **Full history probing** — automatically detects listing date for any symbol

---

## Quick Start

### Requirements

- Python 3.12+

### Installation

```bash
pip install -r requirements.txt
```

### CLI Usage

```bash
# Initialize the manifest database
python -m dl_datalake.cli init

# Download OHLCV data from an exchange
python -m dl_datalake.cli ingest \
  --exchange binance \
  --symbol BTCUSDT \
  --market spot \
  --start-date 2024-01-01

# List available datasets
python -m dl_datalake.cli list --symbol BTCUSDT

# Read data (outputs JSON)
python -m dl_datalake.cli read \
  --exchange BINANCE \
  --symbol BTCUSDT \
  --start 2024-01-01 \
  --end 2024-01-31
```

### Python Client

```python
from dl_datalake.client import DatalakeClient

client = DatalakeClient(base_url="http://localhost:8000")

# List available datasets
datasets = client.list_datasets(symbol="BTCUSDT")

# Read market data
df = client.read(
    exchange="BINANCE",
    symbol="BTCUSDT",
    start="2024-01-01",
    end="2024-01-31",
)
print(df.head())
```

### REST API

Start the API server:

```bash
uvicorn dl_datalake.client.api_server:app --reload --port 8000
```

Interactive docs available at:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

See [API_DOCUMENTATION.md](API_DOCUMENTATION.md) for the full reference.

---

## Development

### Run Tests

```bash
pytest tests/
```

### Run with Coverage

```bash
python run_tests.py
# or
pytest --cov=src/dl_datalake --cov-report=html
```

### Lint & Format

```bash
ruff check src/
ruff format src/
```

### Docker

```bash
docker build -t dl-datalake .
docker run -p 8000:8000 -v $(pwd)/data:/app/data dl-datalake
```

---

## UI

A companion web UI is available in [`../dl-datalake-ui/`](../dl-datalake-ui/). It provides:

- Dataset browser with search and pagination
- One-click download from any CCXT-supported exchange
- Live ingestion progress tracking
- Data preview and CSV export
- Feature Store management

---

## License

MIT
