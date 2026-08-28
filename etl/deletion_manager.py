# etl/deletion_manager.py
from __future__ import annotations
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from sqlalchemy import select
from etl.database_client import CleanupOperation, Dataset, DatabaseClient

logger = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class DeletionManager:
    def __init__(self, db: DatabaseClient, dataset_id: int) -> None:
        self._db = db
        self._dataset_id = dataset_id
        self._base_dir = Path("data") / "datasets" / str(dataset_id)
        self._manifest_path = self._base_dir / ".deletion_manifest.json"
        self._last_op_id: int | None = None

    def delete_all(self) -> dict:
        result = self.clear_files_only()

        try:
            self._cleanup_database_rows()
        except Exception as exc:
            logger.exception("[DELETE] gagal hapus row dataset_id=%d dari database", self._dataset_id)
            self._update_operation_status(
                self._last_op_id, "FAILED",
                completed_at=_now(),
                error_log=f"File terhapus tapi row database gagal dihapus: {exc}",
                deleted_count=result["deleted_count"],
                freed_bytes=result["freed_bytes"],
            )
            raise

        logger.info(
            "[DELETE] dataset_id=%d selesai dihapus total: %d file, %.2f GB dibebaskan",
            self._dataset_id, result["deleted_count"], result["freed_bytes"] / 1e9,
        )
        return {"status": "DELETED", **result}

    def clear_files_only(self) -> dict:
        manifest = self._load_or_create_manifest()
        op_id = self._get_or_create_cleanup_operation(manifest)
        self._last_op_id = op_id
        self._update_operation_status(op_id, "IN_PROGRESS", started_at=_now())

        result = self._delete_files(manifest, op_id)
        self._remove_empty_dirs()
        self._remove_manifest_and_base_dir()
        self._update_operation_status(
            op_id, "COMPLETED", completed_at=_now(),
            deleted_count=result["deleted_count"], freed_bytes=result["freed_bytes"],
        )
        return result

    def _load_or_create_manifest(self) -> dict:
        if self._manifest_path.exists():
            with open(self._manifest_path) as f:
                return json.load(f)
        files = []
        total_bytes = 0
        if self._base_dir.exists():
            for f in self._base_dir.rglob("*"):
                if f.is_file() and f != self._manifest_path:
                    size = f.stat().st_size
                    files.append({"path": str(f), "size_bytes": size, "deleted": False})
                    total_bytes += size
        manifest = {
            "dataset_id": self._dataset_id,
            "created_at": _now().isoformat(),
            "paths": files,
            "total_files": len(files),
            "total_size_bytes": total_bytes,
        }
        self._base_dir.mkdir(parents=True, exist_ok=True)
        self._write_manifest(manifest)
        return manifest

    def _write_manifest(self, manifest: dict) -> None:
        tmp = self._manifest_path.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(manifest, f)
        tmp.replace(self._manifest_path)

    def _get_or_create_cleanup_operation(self, manifest: dict) -> int:
        with self._db.session() as sess:
            op = sess.scalar(
                select(CleanupOperation).where(
                    CleanupOperation.dataset_id == self._dataset_id,
                    CleanupOperation.operation_type == "FULL_DELETE",
                    CleanupOperation.status.in_(["PENDING", "IN_PROGRESS"]),
                ).order_by(CleanupOperation.created_at.desc())
            )
            if op:
                return op.id
            op = CleanupOperation(
                dataset_id=self._dataset_id,
                operation_type="FULL_DELETE",
                status="PENDING",
                total_files=manifest["total_files"],
            )
            sess.add(op)
            sess.flush()
            return op.id

    def _update_operation_status(self, op_id: int, status: str, **fields) -> None:
        with self._db.session() as sess:
            op = sess.get(CleanupOperation, op_id)
            if op is None:
                return
            op.status = status
            for k, v in fields.items():
                setattr(op, k, v)

    def _delete_files(self, manifest: dict, op_id: int) -> dict:
        deleted_count = sum(1 for p in manifest["paths"] if p["deleted"])
        freed_bytes = sum(p["size_bytes"] for p in manifest["paths"] if p["deleted"])
        for item in manifest["paths"]:
            if item["deleted"]:
                continue
            path = Path(item["path"])
            try:
                if path.exists():
                    path.unlink()
                item["deleted"] = True
                deleted_count += 1
                freed_bytes += item["size_bytes"]
            except OSError as exc:
                logger.error("[DELETE] gagal hapus %s: %s", path, exc)
                continue
            if deleted_count % 20 == 0:
                self._write_manifest(manifest)
                self._update_operation_status(op_id, "IN_PROGRESS", deleted_count=deleted_count, freed_bytes=freed_bytes)
        self._write_manifest(manifest)
        self._update_operation_status(op_id, "IN_PROGRESS", deleted_count=deleted_count, freed_bytes=freed_bytes)
        return {"deleted_count": deleted_count, "freed_bytes": freed_bytes}

    def _remove_empty_dirs(self) -> None:
        if not self._base_dir.exists():
            return
        for d in sorted(self._base_dir.rglob("*"), reverse=True):
            if d.is_dir():
                try:
                    d.rmdir()
                except OSError:
                    pass

    def _remove_manifest_and_base_dir(self) -> None:
        try:
            if self._manifest_path.exists():
                self._manifest_path.unlink()
        except OSError:
            pass
        try:
            if self._base_dir.exists():
                self._base_dir.rmdir()
        except OSError:
            pass

    def _cleanup_database_rows(self) -> None:
        with self._db.session() as sess:
            dataset = sess.get(Dataset, self._dataset_id)
            if dataset is None:
                return
            if not dataset.is_deletable:
                raise RuntimeError(f"dataset_id={self._dataset_id} tidak boleh dihapus (is_deletable=False)")
            sess.delete(dataset)