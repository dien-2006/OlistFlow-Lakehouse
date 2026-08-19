from __future__ import annotations

import argparse
import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from src.common.config import Settings, settings
from src.common.minio_storage import MinioStorage, ObjectStorage

DATASET_FILES = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}
BATCH_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")


@dataclass(frozen=True)
class IngestedFile:
    dataset: str
    object_name: str
    size_bytes: int
    sha256: str
    uploaded: bool


@dataclass(frozen=True)
class BatchManifest:
    batch_id: str
    created_at: str
    files: tuple[IngestedFile, ...]

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), indent=2).encode("utf-8")


class BronzeIngestionService:
    """Upload immutable source batches and publish an auditable manifest."""

    def __init__(
        self,
        storage: ObjectStorage,
        config: Settings = settings,
    ) -> None:
        self._storage = storage
        self._config = config

    def ingest(self, raw_dir: Path, batch_id: str | None = None) -> BatchManifest:
        resolved_batch_id = batch_id or self._new_batch_id()
        self._validate_batch_id(resolved_batch_id)
        self._validate_source_files(raw_dir)
        self._storage.ensure_bucket()

        files = tuple(
            self._ingest_file(raw_dir, resolved_batch_id, dataset, filename)
            for dataset, filename in DATASET_FILES.items()
        )
        manifest = BatchManifest(
            batch_id=resolved_batch_id,
            created_at=datetime.now(timezone.utc).isoformat(),
            files=files,
        )
        self._publish_manifest(manifest)
        return manifest

    def _ingest_file(
        self,
        raw_dir: Path,
        batch_id: str,
        dataset: str,
        filename: str,
    ) -> IngestedFile:
        local_path = raw_dir / filename
        object_name = (
            f"{self._config.bronze_prefix}/{dataset}/" f"batch_id={batch_id}/{filename}"
        )
        already_exists = self._storage.exists(object_name)
        if not already_exists:
            self._storage.upload(local_path, object_name, "text/csv")

        status = "exists" if already_exists else "uploaded"
        print(f"[bronze:{status}] {dataset} -> {object_name}")
        return IngestedFile(
            dataset=dataset,
            object_name=object_name,
            size_bytes=local_path.stat().st_size,
            sha256=self._sha256(local_path),
            uploaded=not already_exists,
        )

    def _publish_manifest(self, manifest: BatchManifest) -> None:
        object_name = (
            f"{self._config.bronze_prefix}/_manifests/{manifest.batch_id}.json"
        )
        self._storage.upload_bytes(manifest.to_json(), object_name, "application/json")

    @staticmethod
    def _validate_source_files(raw_dir: Path) -> None:
        if not raw_dir.is_dir():
            raise NotADirectoryError(raw_dir)
        missing = [
            name for name in DATASET_FILES.values() if not (raw_dir / name).is_file()
        ]
        if missing:
            raise FileNotFoundError(
                f"Missing source files in {raw_dir}: {', '.join(sorted(missing))}"
            )

    @staticmethod
    def _validate_batch_id(batch_id: str) -> None:
        if not BATCH_ID_PATTERN.fullmatch(batch_id):
            raise ValueError(
                "batch_id may contain only letters, numbers, underscores and hyphens"
            )

    @staticmethod
    def _new_batch_id() -> str:
        return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()


def upload_batch(
    raw_dir: Path,
    batch_id: str | None = None,
    storage: ObjectStorage | None = None,
) -> BatchManifest:
    """Compatibility helper for existing notebooks and command usage."""
    return BronzeIngestionService(storage or MinioStorage()).ingest(raw_dir, batch_id)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upload one immutable Olist batch to MinIO"
    )
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--batch-id")
    args = parser.parse_args()
    upload_batch(args.raw_dir, args.batch_id)


if __name__ == "__main__":
    main()
