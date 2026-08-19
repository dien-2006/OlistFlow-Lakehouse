from pathlib import Path

import pytest

from src.bronze.ingest_olist import BronzeIngestionService, DATASET_FILES


class InMemoryStorage:
    """Test double proving that ingestion depends on a port, not MinIO itself."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def ensure_bucket(self, bucket: str | None = None) -> None:
        return None

    def upload(
        self,
        local_path: Path,
        object_name: str,
        content_type: str = "application/octet-stream",
    ) -> None:
        self.objects[object_name] = local_path.read_bytes()

    def upload_bytes(
        self,
        data: bytes,
        object_name: str,
        content_type: str,
    ) -> None:
        self.objects[object_name] = data

    def exists(self, object_name: str) -> bool:
        return object_name in self.objects


def create_source_batch(raw_dir: Path) -> None:
    for filename in DATASET_FILES.values():
        (raw_dir / filename).write_text("header\nvalue\n", encoding="utf-8")


def test_ingestion_creates_files_and_manifest(tmp_path):
    create_source_batch(tmp_path)
    storage = InMemoryStorage()

    manifest = BronzeIngestionService(storage).ingest(tmp_path, "batch_001")

    assert manifest.batch_id == "batch_001"
    assert len(manifest.files) == len(DATASET_FILES)
    assert all(file.uploaded for file in manifest.files)
    assert "bronze/olist/_manifests/batch_001.json" in storage.objects


def test_ingestion_is_idempotent_for_same_batch(tmp_path):
    create_source_batch(tmp_path)
    storage = InMemoryStorage()
    service = BronzeIngestionService(storage)

    service.ingest(tmp_path, "batch_001")
    second_manifest = service.ingest(tmp_path, "batch_001")

    assert all(not file.uploaded for file in second_manifest.files)


def test_ingestion_rejects_unsafe_batch_id(tmp_path):
    create_source_batch(tmp_path)

    with pytest.raises(ValueError, match="batch_id"):
        BronzeIngestionService(InMemoryStorage()).ingest(tmp_path, "../unsafe")
