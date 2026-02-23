import sys
from pathlib import Path

# Add dl-datalake and current dir to sys.path before imports
BASE_DIR = Path(__file__).resolve().parent.parent.parent / "dl-datalake"
sys.path.append(str(BASE_DIR / "src"))
sys.path.append(str(Path(__file__).resolve().parent))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers import data, ingest, features
from dl_datalake.ingest.pipeline import IngestPipeline
import logging_config

# Initialize logging
logging_config.configure_logging()


# Initialize database schema once at startup
# We use the same defaults as ingest router for consistency
BASE_DIR_PIPELINE = Path(__file__).resolve().parent.parent.parent / "dl-datalake"
MANIFEST_PATH = str(BASE_DIR_PIPELINE / "manifest.db")
DATA_ROOT = str(BASE_DIR_PIPELINE / "data")

pipeline = IngestPipeline(data_root=DATA_ROOT, db_path=MANIFEST_PATH)
pipeline.manifest.ensure_tables()

app = FastAPI(title="DL Datalake UI API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # In production, restrict this
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(data.router)
app.include_router(ingest.router)
app.include_router(features.router)

@app.get("/")
def read_root():
    return {"message": "DL Datalake UI API is running"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
