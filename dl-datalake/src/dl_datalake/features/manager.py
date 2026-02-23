"""Feature store manager."""

import hashlib
import shutil
from pathlib import Path

from dl_datalake.metadata.manifest import ManifestManager


class FeatureStore:
    """Manages feature sets."""

    def __init__(self, base_path: str = "data", db_path: str = "manifest.db") -> None:
        """Initialize FeatureStore.

        Args:
           base_path: Root directory.
           db_path: Path to manifest DB.
        """
        self.base_path = Path(base_path)
        self.manifest = ManifestManager(db_path=db_path)

    def _get_feature_path(self, feature_set: str, version: str) -> Path:
        path = self.base_path / "features" / feature_set / version
        path.mkdir(exist_ok=True, parents=True)
        return path

    def upload_feature(  # noqa: PLR0913
        self,
        src_path: str,
        exchange: str,
        market: str,
        symbol: str,
        feature_set: str,
        version: str = "1.0.0",
    ) -> str:
        """Register and store an externally calculated feature set.

        Args:
            src_path: Source file path.
            exchange: Exchange.
            market: Market.
            symbol: Symbol.
            feature_set: Feature set name.
            version: Version.

        Returns:
            Destination path as string.

        Raises:
            FileNotFoundError: If src_path does not exist.
        """
        src = Path(src_path)
        if not src.exists():
            msg = f"Source file {src_path} not found"
            raise FileNotFoundError(msg)

        dest_dir = self._get_feature_path(feature_set, version)
        filename = src.name
        dest_path = dest_dir / filename

        _ = shutil.copy2(src, dest_path)

        # Calculate checksum
        sha256_hash = hashlib.sha256()
        with dest_path.open("rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)

        checksum = sha256_hash.hexdigest()

        _ = self.manifest.add_entry(
            exchange=exchange,
            market=market,
            symbol=symbol,
            type=feature_set,
            path=str(dest_path),
            version=str(version),
            checksum=checksum,
            metadata_json=f'{{"feature_set": "{feature_set}"}}',
        )
        return version
