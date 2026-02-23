"""Manifest management module."""

from datetime import UTC, datetime
from enum import Enum
from typing import Any

from sqlalchemy import BigInteger, Column, DateTime, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()


class DataType(str, Enum):
    """Enums for data types."""

    RAW = "raw"
    TICKS = "ticks"
    AGG = "agg"
    FEATURE = "feature"


class ManifestEntry(Base):
    """ORM model for manifest entries."""

    __tablename__ = "manifest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange = Column(String, nullable=False)
    market = Column(String, nullable=False)
    symbol = Column(String, nullable=False)
    path = Column(String, unique=True, nullable=False)
    type = Column(String, nullable=False)  # raw/agg/feature
    time_from = Column(BigInteger, nullable=True)  # ms since epoch
    time_to = Column(BigInteger, nullable=True)  # ms since epoch
    version = Column(String, default="1.0.0")
    checksum = Column(String, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC))
    script_git_hash = Column(String, nullable=True)
    metadata_json = Column(String, nullable=True)  # Extra info like timeframe for agg


class ManifestManager:
    """Manages the data catalog manifest."""

    def __init__(self, db_path: str = "manifest.db") -> None:
        """Initialize ManifestManager.

        Args:
            db_path: Path to SQLite DB.
        """
        # Increase timeout to 30s to handle concurrent writes from background tasks
        self.engine = create_engine(
            f"sqlite:///{db_path}",
            connect_args={"timeout": 30},
        )
        self.Session = sessionmaker(bind=self.engine)
        self.ensure_tables()

    def ensure_tables(self) -> None:
        """Ensure all required tables exist. Call this once at startup."""
        Base.metadata.create_all(self.engine)

    def add_entry(self, **kwargs: Any) -> int:  # noqa: ANN401
        """Add a new entry to manifest.

        Args:
            **kwargs: ManifestEntry fields.

        Returns:
            ID of created entry.
        """
        with self.Session() as session:
            # SQLAlchemy/SQLite might not handle Path objects automatically
            if "path" in kwargs:
                kwargs["path"] = str(kwargs["path"])

            # Normalize casing to match ParquetWriter and list_entries
            if "exchange" in kwargs and isinstance(kwargs["exchange"], str):
                kwargs["exchange"] = kwargs["exchange"].upper()
            if "market" in kwargs and isinstance(kwargs["market"], str):
                kwargs["market"] = kwargs["market"].upper()
            if "symbol" in kwargs and isinstance(kwargs["symbol"], str):
                kwargs["symbol"] = kwargs["symbol"].upper()

            # Check if entry with this path exists
            existing = (
                session.query(ManifestEntry).filter_by(path=kwargs["path"]).first()
            )
            if existing:
                # Update fields
                for key, value in kwargs.items():
                    setattr(existing, key, value)
                session.commit()
                return existing.id  # pyright: ignore

            entry = ManifestEntry(**kwargs)
            session.add(entry)
            session.commit()
            return entry.id  # pyright: ignore

    def get_latest_version(self, exchange: str, symbol: str, feature_set: str) -> int:
        """Get latest version number for a feature set."""
        with self.Session() as session:
            stmt = (
                session.query(ManifestEntry.version)  # pyright: ignore[reportCallIssue]
                .filter_by(exchange=exchange, symbol=symbol, type=feature_set)
                .order_by(ManifestEntry.version.desc())
                .first()
            )
            # Assuming version is stored as a string and needs conversion to int
            return int(stmt[0]) if stmt else 0  # pyright: ignore

    def list_entries(
        self,
        symbol: str | None = None,
        data_type: str | None = None,
        exchange: str | None = None,
        market: str | None = None,
    ) -> list[ManifestEntry]:
        """List entries matching filters.

        Args:
           symbol: Filter by symbol.
           data_type: Filter by data type.
           exchange: Filter by exchange.
           market: Filter by market type.

        Returns:
            List of ManifestEntry objects.
        """
        with self.Session() as session:
            query = session.query(ManifestEntry)
            if symbol:
                query = query.filter(ManifestEntry.symbol == symbol)
            if data_type:
                query = query.filter(ManifestEntry.type == data_type)
            if exchange:
                query = query.filter(ManifestEntry.exchange == exchange.upper())
            if market:
                query = query.filter(ManifestEntry.market == market.upper())
            return query.all()  # pyright: ignore

    def delete_entries(
        self,
        symbol: str,
        exchange: str | None = None,
        market: str | None = None,
        data_type: str | None = None,
    ) -> list[str]:
        """Delete entries for a symbol.

        Args:
            symbol: Symbol to delete.
            exchange: Optional exchange filter.
            market: Optional market filter.
            data_type: Optional data type filter.

        Returns:
             List of deleted file paths.
        """
        with self.Session() as session:
            query = session.query(ManifestEntry).filter(ManifestEntry.symbol == symbol)
            if exchange:
                query = query.filter(ManifestEntry.exchange == exchange.upper())
            if market:
                query = query.filter(ManifestEntry.market == market.upper())
            if data_type:
                query = query.filter(ManifestEntry.type == data_type)
            entries = query.all()
            paths = [e.path for e in entries]
            _ = query.delete(synchronize_session=False)
            session.commit()
            return paths  # pyright: ignore
