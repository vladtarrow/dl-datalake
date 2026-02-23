import sys
from pathlib import Path
from loguru import logger

def configure_logging():
    """Configure loguru logging."""
    
    # Define log directory relative to project root
    base_dir = Path(__file__).resolve().parent.parent.parent.parent
    log_dir = base_dir / "logs"
    log_dir.mkdir(exist_ok=True)
    
    # Remove default handler to avoid duplication if re-configured
    logger.remove()
    
    # Add stdout handler (console)
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level> | {extra}",
        level="INFO",
        colorize=True
    )
    
    # Add file handler
    # Rotates every 10 MB, keeps 1 week of logs
    logger.add(
        log_dir / "datalake.log",
        rotation="10 MB",
        retention="1 week",
        compression="zip",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message} | {extra}",
        level="WARNING",
        enqueue=True,  # Thread-safe for async/threaded apps
        backtrace=True,
        diagnose=True
    )
    
    logger.info("Logging configured. Writing to logs/datalake.log")
