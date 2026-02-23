# Engineering Guide: Quantitative Data Infrastructure

This document provides a deep dive into the technical architecture, design philosophy, and performance characteristics of `dl-datalake`. It is intended for software engineers and researchers building systematic trading infrastructure and high-performance ML pipelines.

## 1. Core Principles

The system is built on four pillars essential for quantitative research:
- **Reproducibility**: Every data point is versioned and checksummed.
- **Scalability**: Hierarchical partitioning allows handling multi-terabyte datasets on consumer-grade hardware.
- **Interoperability**: Standard formats (Parquet, SQLite, Arrow) ensure zero-copy integration with the modern data science stack.
- **Centralized Data Server**: Acts as a unified data hub, exposing REST and Python APIs to serve multiple research and production consumers simultaneously.

## 2. Storage Architecture

### 2.1 Partitioning Strategy
Data is organized using a hierarchical directory structure to maximize filesystem-level pruning during query execution:

```text
data/
└── {EXCHANGE}/
    └── {MARKET}/
        └── {SYMBOL}/
            └── {TYPE}/        # raw, ticks, agg, features
                └── {PERIOD}/  # 1m, 1h, etc.
                    └── {YYYY}/
                        └── {MM}/
                            └── {DD}/
                                └── data.parquet
```

### 2.2 Data Format & Compression
- **Format**: Apache Parquet (Columnar Storage).
- **Compression**: `snappy` or `zstd` (tunable).
- **Schema Enforcement**: Minimal at the storage layer to allow for **Schema Evolution**. The system relies on a "Uniform View" where core OHLCV columns are guaranteed, but supplementary columns (e.g., `trade_count`, `buy_volume`) can be appended dynamically without breaking existing readers.

## 3. Data Integrity and Atomic Writes

To prevent corruption during interrupted downloads or concurrent writes, `dl-datalake` implements:
- **Atomic Renames**: Data is written to a hidden `.tmp` file and moved to its final destination only after a successful `fsync`.
- **Idempotency**: The ingestion pipeline uses UPSERT logic. Re-running a task for the same time period will merge and deduplicate data based on timestamps, ensuring the final state is deterministic.
- **Integrity Checks**: SHA-256 checksums and gap detection algorithms are run post-ingestion to verify continuity.

## 4. Performance Optimization

### 4.1 Vectorized Processing
The core engine leverages **Polars** and **DuckDB** for all data transformations. This ensures that operations like resampling (OHLCV aggregation) or feature calculation are executed in highly optimized Rust/C++ kernels, bypassing Python's Global Interpreter Lock (GIL).

### 4.2 Query Execution
When using the `DataLakeClient`, queries are optimized to:
1.  **Prune Partitions**: Scan only the relevant date directories.
2.  **Projection Pushdown**: Read only the requested columns.
3.  **Predicate Pushdown**: Filter data (e.g., `close > 50000`) before loading into memory.

## 5. Feature Store Mechanics

The Feature Store is designed for **Research-to-Production parity**:
- **Point-in-Time Correctness**: Features are stored with precise timestamps to prevent look-ahead bias during backtesting.
- **Versioning**: Each feature set can have multiple versions (e.g., `v1.0.0`, `v1.2.0-experimental`). Researchers can swap feature versions in their models by simply changing a metadata flag.
- **Metadata Context**: Features are linked to the specific raw data version they were derived from, ensuring full lineage.

## 6. Development & Extensibility

### 6.1 Custom Ingestors
The `BaseConnector` can be extended to support non-CCXT sources (e.g., proprietary HFT feeds, alternative data).
```python
class ProprietaryFeedConnector(BaseConnector):
    def fetch_data(self, ...):
        # Implementation for specialized binary protocols or WebSocket feeds
        pass
```

### 6.2 Data Server & API Layer
Beyond a local library, `dl-datalake` functions as a **centralized Data Server**. The integrated FastAPI layer enables:
- **Remote Access**: Serve data to multiple researchers or automated agents across a network.
- **Language Agnosticism**: Seamless integration with non-Python systems via JSON/REST.
- **Streaming Previews**: Efficient pagination and previewing of massive Parquet datasets without full file transfers.

### 6.3 Orchestration
While the project includes **Prefect** workflows for basic automation, the library is designed as a "Sidecar" or a standalone service and can be easily integrated into other orchestrators like Airflow, Dagster, or custom Cron systems.

---

## 7. Operational Best Practices
- **SSD vs HDD**: While Parquet handles HDDs well due to sequential reads, an NVMe SSD is highly recommended for tick-level data research to minimize I/O wait times.
- **Memory Management**: For large-scale joins, utilize Polars' `lazy` API to allow the engine to optimize the execution graph and manage memory buffers efficiently.
