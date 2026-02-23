# Data Loading Mechanisms

## 1. Is data loaded only via CCXT?

No, data loading is not limited only to the `ccxt` library. Code analysis shows two main mechanisms:

1.  **Via `ccxt` library**:
    *   Used in the `ExchangeConnector` class (`src/dl_datalake/ingest/exchange_connector.py`).
    *   Responsible for downloading data directly from exchanges (e.g., Binance, Bybit).
    *   Supports downloading historical candles (OHLCV) and funding rate history.

2.  **Via local files (CSV)**:
    *   The `IngestPipeline` class (`src/dl_datalake/ingest/pipeline.py`) implements `ingest_csv` and `ingest_ticks_csv` methods.
    *   This allows loading data from local CSV files, bypassing exchange API requests.
    *   Uses the high-performance `polars.read_csv` method for reading CSVs.

## 2. What data types and format does CCXT return?

The `ccxt` library returns data in standard Python structures, which are then converted to `Polars DataFrame` format for further processing and storage.

### OHLCV (Candles)
*   **Method call**: `self.exchange.fetch_ohlcv(...)`
*   **CCXT response format**: List of lists (`List[List[float/int]]`).
*   **Data structure**: Each inner list contains 6 elements:
    1.  Timestamp (int, milliseconds)
    2.  Open price (float)
    3.  High price (float)
    4.  Low price (float)
    5.  Close price (float)
    6.  Volume (float)
*   **In-project**: This list is converted to a `Polars DataFrame` with columns: `["ts", "open", "high", "low", "close", "volume"]`.

### Funding Rates
*   **Method call**: `self.exchange.fetch_funding_rate_history(...)`
*   **CCXT response format**: List of dictionaries (`List[Dict[str, Any]]`).
*   **Data structure**: Each dictionary contains keys like `info`, `symbol`, `timestamp`, `datetime`, `fundingRate`, and other exchange-specific fields.
*   **In-project**: The list of dictionaries is directly converted to a `Polars DataFrame`. The `timestamp` field is then processed to create partitions by date.

## 3. Does it return Ticks (Trades)?

No, the current implementation of `ExchangeConnector` (and thus the CCXT integration in this project) **does not implement** downloading ticks (trades) directly via the exchange API.

However, the project provides tick loading **via CSV files**:
*   The `ingest_ticks_csv` method in `IngestPipeline` allows loading tick data.
*   It expects a CSV file, which is read using the Polars library.
*   Requires a `ts` (timestamp in milliseconds) column for date partitioning.

## 4. Does the storage system support additional columns for minutes and ticks?

### 1. Additional columns for minutes (OHLCV)
**Yes, it does.**
*   Files are saved in **Parquet** format, which is self-describing (stores the schema inside the file).
*   The `ParquetWriter` class (`src/dl_datalake/storage/writer.py`) does not filter columns before writing.
*   The only requirement for OHLCV: presence of a `ts` (timestamp) column for correct daily partitioning. Any other columns passed in the DataFrame will be saved as-is.
*   The metadata system (`manifest.db`) only stores references to files and does not verify the data schema inside them.

### 2. Tick storage
**Yes, it does.**
*   The project implements a `write_ticks` method in `ParquetWriter`.
*   Ticks are saved similarly to candles: partitioned by date (Year/Month/Day).
*   They are assigned a type like `ticks` in the manifest, allowing them to be cataloged separately from candle data.

## 5. Column count limitations in code

There are places in the code with hardcoded column lists, but this concerns **only the loading/transformation stage**, not the storage system itself.

1.  **CCXT Loader (`ExchangeConnector`)**:
    *   **Hard limit**: Yes. When receiving data from the exchange, the code explicitly converts the list of lists into a DataFrame with a fixed schema: `["ts", "open", "high", "low", "close", "volume"]`. If you want to add data (e.g., trade count), this class must be modified.

2.  **CSV Loader (`IngestPipeline`)**:
    *   **Flexible mode**: If your CSV file **has headers** and includes `ts`, the loader will accept **any number of columns**. It will simply read the file as-is and pass it to `ParquetWriter`.
    *   **Compatibility mode**: If the CSV **has no header** (or no `ts` column), the code attempts to "guess" the format, assuming it's a standard Binance file with 6 columns. Only in this case does the column count limitation apply.

## 6. Is it possible to store calculated features and reports?

### 1. Calculated Features
**Yes, it is possible and intended by the architecture.**
*   The project includes a `src/dl_datalake/features/manager.py` module that implements a `FeatureStore`.
*   The `upload_feature` method allows registering any external file as a `feature_set`.
*   **How it works**:
    1.  You compute features (e.g., RSI, squeezes) and save them to a file (e.g., `.parquet`).
    2.  Call `feature_store.upload_feature(src_path=..., feature_set="rsi_14", version="1.0.0", ...)`.
    3.  The file is copied to `data/features/rsi_14/1.0.0/`.
    4.  An entry with type `rsi_14` is added to the manifest.

### 2. Edge Search Reports
**Yes, technically possible.**
*   Since the `FeatureStore` simply copies files and registers them in the manifest, you can use this mechanism for report storage.
*   The system does not verify file content. You can save a report (PDF, Markdown, JSON) as a "feature".
*   **Recommendation**: Use a descriptive set name, e.g., `feature_set="reports"` or `feature_set="edge_search_results"`.
*   In the manifest, the `type` field is a string, so you can define any type.
*   The versioning mechanism (`version="experiment_1"`) works well for iterative edge search.
