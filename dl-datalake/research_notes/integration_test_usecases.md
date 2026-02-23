# Integration Test Use Cases (No-Mock Policy)

Testing "without mocks" in this project implies working with the real file system, a real SQLite database, and, where possible, real data to guarantee the correctness of the entire pipeline.

The following are the primary scenarios (Use Cases) to be covered by such tests:

## 1. End-to-End Ingestion
Verifying the full data movement cycle from source to disk and catalog.
*   **Scenario**:
    1.  Take a real (or carefully prepared) CSV file with price data.
    2.  Run `IngestPipeline.ingest_csv`.
    3.  **Checks**:
        *   The `.parquet` file is physically created at the correct path (`data/EXCHANGE/MARKET/SYMBOL/raw/...`).
        *   An entry appears in `manifest.db` (verified via SQL query or `ManifestManager`).
        *   Data in the Parquet file is readable and matches the source CSV.

## 2. Storage Integrity
Verifying correct interaction between `ParquetWriter` and the file system.
*   **Scenario**:
    1.  Write a DataFrame spanning multiple days.
    2.  **Checks**:
        *   Data is correctly split into folders by date (Year/Month/Day).
        *   No corrupt files are created (atomic write check).
        *   Re-writing (UPSERT) updates the file instead of duplicating data or corrupting the file.

## 3. Manifest Interaction
Verifying `ManifestManager` logic on a real SQLite file.
*   **Scenario**:
    1.  Add several file entries.
    2.  Perform a search with filters (by symbol, type, exchange).
    3.  Delete entries.
    4.  **Checks**:
        *   The `.db` file is created and initialized.
        *   Queries return correct objects.
        *   Entry deletion from the DB occurs (optional: verify physical file deletion if part of the logic).

## 4. Feature Store Workflow
Verifying the registration mechanism for external files.
*   **Scenario**:
    1.  Create a temporary file with "features" (e.g., text or parquet).
    2.  Call `FeatureStore.upload_feature`.
    3.  **Checks**:
        *   The file is copied to the versioned folder structure (`data/features/set_name/v1/...`).
        *   A manifest entry appears with type `feature` and the correct version.
        *   File checksum in the database matches the actual file on disk.

## 5. Cold Boot / Migration
Verifying the system can start from scratch or recover correctly.
*   **Scenario**:
    1.  Delete the `manifest.db` database file.
    2.  Attempt to initialize `ManifestManager`.
    3.  **Checks**:
        *   The database is re-created.
        *   Tables are created automatically (`create_all`).
        *   The application does not crash when starting with an empty data folder.

## 6. Production Readiness Scenarios

To ensure production reliability, basic "happy path" scenarios are not enough. Robustness and edge case tests must be added:

### 6.1. Concurrency
Verifying behavior under simultaneous write access.
*   **Scenario**:
    1.  Start two parallel processes (or threads) writing data for the *same* symbol for the *same* day.
    2.  **Checks**:
        *   The file is not corrupted (Parquet is readable).
        *   No `PermissionError` occurred during writing (checking file locks or atomic renames).
        *   The final state reflects the last successful process (Last Write Wins) or merged data.

### 6.2. Error Handling & Data Validation
*   **Scenario A (Network Failure)**:
    1.  Start a download via `ExchangeConnector`.
    2.  Abort the connection mid-download.
    3.  **Check**: No temporary `.tmp` files are left behind, blocking future runs.
*   **Scenario B (Bad Data)**:
    1.  Attempt to load a CSV with duplicate timestamps or `NaN` values in prices.
    2.  **Check**: The system either cleans the data (deduplication) or fails with a clear error without polluting the storage.

### 6.3. Performance & Stress
*   **Scenario**: Loading a massive file (e.g., ticks for a year, >1GB).
*   **Check**: The process does not crash due to OOM (Out Of Memory); memory usage is stable (using streaming or chunking in Polars).

## 7. Maintenance & Evolution

Checking how the system survives "long-term."

### 7.1. Schema Evolution
*   **Scenario**:
    1.  Write data with 6 columns (Version 1).
    2.  Update code and write data with 8 columns (Version 2).
    3.  Attempt to read both files in one query.
    4.  **Check**: Reading does not crash; old files are read correctly (with `nulls` in new columns or ignored, depending on logic).

### 7.2. Data Quality & Gap Detection
*   **Scenario**:
    1.  Load history with an intentional date gap (one day missing).
    2.  Run the integrity check utility (if implemented).
    3.  **Check**: The system must detect the gap and report it rather than silently storing fragmented data.

### 7.3. Cleanup Policy
*   **Scenario**:
    1.  Call the deletion method (`delete_entries`) for old data.
    2.  **Check**: Files are physically removed from disk, space is freed, and no "dead links" remain in the manifest.

## 8. Idempotency & Determinism

The "gold standard" for data engineering.
*   **Scenario**:
    1.  Run the data ingestion pipeline (e.g., for "yesterday's" data).
    2.  Ensure success.
    3.  Run the **exact same** pipeline again.
    4.  **Checks**:
        *   The system must not fail with "DuplicateKey" errors.
        *   Data should not be duplicated (row count stays the same).
        *   File checksum remains identical (if data hasn't changed).

**Why it matters**: In production, you will frequently retry failed tasks. Idempotency allows this without fear of corrupting data.

## 9. Portability & Consistency

### 9.1. Path Portability
Running on Windows development but Linux (Docker) production.
*   **Scenario**:
    1.  Write data to the DB on Windows (paths with `\`).
    2.  Attempt to read this data in a Docker container (Linux).
    3.  **Check**: The system must normalize paths and find files successfully despite separator differences.

### 9.2. Audit / Reconciliation (FSCK)
Checking for "zombie files" or "dead links."
*   **Scenario**:
    1.  Create an "orphan": a file on disk not present in `manifest.db`.
    2.  Create a "dead link": an entry in `manifest.db` pointing to a deleted file.
    3.  Run the reconciliation procedure.
    4.  **Check**: The system should return a report of discrepancies.

## 10. Observability & Logging
*   **Scenario**: Force a critical error (e.g., `DiskFull`).
*   **Check**:
    *   An `ERROR` or `CRITICAL` log entry appears.
    *   The log contains context (symbol, file, traceback).
    *   The application exits with a non-zero status code for orchestrators to detect.

## 11. Versioning & Rollback
Testing the architecture's `version` field in the manifest.
*   **Scenario**:
    1.  Load a feature set with `version="1.0.0"`.
    2.  Discover a "bug" and load a fixed set as `version="1.0.1"`.
    3.  Request data via `ManifestManager.get_latest_version`.
    4.  **Check**: The system returns version `1.0.1`.
*   **Rollback**: If version `1.0.1` is deleted, the system should automatically fallback to version `1.0.0`.

## 12. Timezones & Boundaries
Crucial for trading data. A 1-second error can shift a candle to another day.
*   **Scenario**:
    1.  Load data exactly on a midnight boundary (23:59:59.999 and 00:00:00.000).
    2.  **Checks**:
        *   23:59:59 data falls into "yesterday's" folder.
        *   00:00:00 data falls into "today's" folder.
        *   The system uses UTC (or a fixed offset), not the machine's local time.

## 13. Configuration & Permissions
*   **Scenario A (Dry Run)**: Run pipeline with `--dry-run`. Check that logs are written but no files/manifest entries are changed.
*   **Scenario B (Permissions)**: Attempt to write as a restricted user. Check that the system catches `PermissionDenied` and fails with a clear message.
