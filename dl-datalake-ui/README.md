# dl-datalake Web UI

A modern, responsive web application for exploring and managing your local Market Data Lake. Built with **React + TypeScript + Vite** for the frontend and **FastAPI** for the backend.

---

## Features

- **📊 Dataset Browser**: Search, filter, and paginate through your local data lake (OHLCV, ticks, alternate data).
- **📥 Smart Ingestion**: Download historical data from 100+ exchanges using CCXT. Supports incremental updates and probe-based listing date detection.
- **👁️ Data Preview**: Instant table view of any Parquet segment without downloading the full file.
- **💾 CSV Export**: Export any dataset in a format compatible with MetaTrader 4/5 and other trading platforms.
- **🧪 Feature Store Management**: Upload, version, and manage custom feature sets directly from the UI.
- **📉 Live Progress**: Real-time monitoring of active download tasks.

---

## Getting Started

### Prerequisites

- Node.js (v18+)
- Python (v3.12+)
- [dl-datalake](https://github.com/vladtarrow/dl-datalake) (core package)

### 1. Start the Backend

The UI backend provides an extended API for the web interface.

```bash
cd backend
pip install -r requirements.txt
python main.py
```
Backend runs on [http://localhost:8000](http://localhost:8000).

### 2. Start the Frontend

```bash
cd frontend
npm install
npm run dev
```
Frontend runs on [http://localhost:5173](http://localhost:5173).

---

## Project Structure

### Backend (`/backend`)
- `main.py`: Entrypoint and FastAPI configuration.
- `routers/`: API endpoints for datasets, ingestion, and features.
- `schemas.py`: Pydantic models for request/response validation.

### Frontend (`/frontend`)
- `src/features/datasets`: Dataset search, table views, and preview modals.
- `src/features/ingest`: Trading symbol search and download forms.
- `src/api`: Axios clients for interacting with the backend.

---

## Tech Stack

- **Frontend**: React, TypeScript, Vite, Tailwind CSS, TanStack Table, Axios.
- **Backend**: FastAPI, Polars, CCXT, Loguru.
- **Storage**: Apache Parquet (managed by core library), SQLite.

---

## License

MIT
