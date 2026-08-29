# etl/pipeline_logger.py
from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
import traceback
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import psutil
from sqlalchemy import func, select

from etl.database_client import DatabaseClient, ProcessingLog

logger = logging.getLogger(__name__)

_SAMPLE_INTERVAL_SEC = 0.5
_TRACEBACK_MAX_CHARS = 4000


def _slug_filename(name: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9_.-]", "_", name).strip("_")
    return slug or "dataset"


@contextmanager
def dataset_log_file(dataset_name: str, logs_dir: str | None = None) -> Iterator[Path]:
    """Attach a per-dataset FileHandler to this module's logger for the
    duration of a batch run, so every [PLOG] event (download progress and
    stage transitions alike) is appended to one .txt file immediately,
    without waiting for the batch to finish. Removed again on exit."""
    logs_dir = logs_dir or os.getenv("LOGS_DIR", "logs_pipeline")
    log_dir_path = Path(logs_dir)
    log_dir_path.mkdir(parents=True, exist_ok=True)
    log_path = log_dir_path / f"{_slug_filename(dataset_name)}.txt"

    handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    handler.setLevel(logging.INFO)

    logger.addHandler(handler)
    try:
        yield log_path
    finally:
        logger.removeHandler(handler)
        handler.close()


class PipelineLogManager:
    """Append-only structured pipeline log, backed by the `processing_logs`
    table. One row per stage STARTED/RUNNING/COMPLETED/FAILED event."""

    def __init__(self, db: DatabaseClient) -> None:
        self._db = db

    def log_event(
        self,
        dataset_id: int,
        scene_id: str,
        module: str,
        stage: str,
        status: str,
        message: str,
        details: dict | None = None,
    ) -> int:
        with self._db.session() as sess:
            row = ProcessingLog(
                dataset_id=dataset_id,
                scene_id=scene_id,
                module=module,
                stage=stage,
                status=status,
                message=message,
                details=details or {},
            )
            sess.add(row)
            sess.flush()
            log_id = row.log_id

        level = logging.ERROR if status == "FAILED" else logging.INFO
        logger.log(
            level, "[PLOG] %s",
            json.dumps(
                {
                    "log_id": log_id, "dataset_id": dataset_id, "scene_id": scene_id,
                    "module": module, "stage": stage, "status": status, "message": message,
                    **(details or {}),
                },
                default=str,
            ),
        )
        return log_id

    def query_logs(
        self,
        dataset_id: int,
        stage: str | None = None,
        status: str | None = None,
        scene_id: str | None = None,
        limit: int = 50,
        offset: int = 0,
        order: str = "desc",
    ) -> tuple[list[dict], int]:
        with self._db.session() as sess:
            stmt = select(ProcessingLog).where(ProcessingLog.dataset_id == dataset_id)
            if stage:
                stmt = stmt.where(ProcessingLog.stage == stage)
            if status:
                stmt = stmt.where(ProcessingLog.status == status)
            if scene_id:
                stmt = stmt.where(ProcessingLog.scene_id == scene_id)

            total = sess.scalar(select(func.count()).select_from(stmt.subquery())) or 0

            order_col = ProcessingLog.created_at.desc() if order == "desc" else ProcessingLog.created_at.asc()
            rows = sess.scalars(stmt.order_by(order_col).limit(limit).offset(offset)).all()
            return [self._to_dict(r) for r in rows], total

    @staticmethod
    def _to_dict(r: ProcessingLog) -> dict:
        return {
            "log_id": r.log_id,
            "timestamp": r.created_at,
            "module": r.module,
            "dataset_id": r.dataset_id,
            "scene_id": r.scene_id,
            "stage": r.stage,
            "status": r.status,
            "message": r.message,
            "details": r.details or {},
        }


class _StageHandle:
    """Passed into a `with plog.stage(...) as st:` block so the caller can
    attach result fields (output path, size, quality score, ...) that get
    merged into the COMPLETED event's details. `st.output(message=...)`
    overrides the default completion message."""

    def __init__(self) -> None:
        self.details: dict[str, Any] = {}
        # Populated after the `with` block exits successfully, so callers can
        # read timing/resource numbers back out (e.g. to also populate
        # processing_jobs.cpu_usage_percent / memory_usage_mb).
        self.duration_seconds: float | None = None
        self.memory_peak_mb: float | None = None
        self.cpu_peak_percent: float | None = None

    def output(self, **kwargs: Any) -> None:
        self.details.update(kwargs)


class _MemorySampler:
    """Background peak memory/CPU sampler for the lifetime of a stage."""

    def __init__(self) -> None:
        self._process = psutil.Process()
        self._stop = threading.Event()
        self._peak_rss_mb = 0.0
        self._peak_cpu_percent = 0.0
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._process.cpu_percent(interval=None)  # prime the counter (first call always returns 0)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                rss_mb = self._process.memory_info().rss / (1024 ** 2)
                cpu_percent = self._process.cpu_percent(interval=None)
                self._peak_rss_mb = max(self._peak_rss_mb, rss_mb)
                self._peak_cpu_percent = max(self._peak_cpu_percent, cpu_percent)
            except Exception:
                pass
            self._stop.wait(_SAMPLE_INTERVAL_SEC)

    def stop(self) -> tuple[float, float]:
        self._stop.set()
        self._thread.join(timeout=2.0)
        return round(self._peak_rss_mb, 2), round(self._peak_cpu_percent, 2)


class PipelineLogger:
    """Thin facade over PipelineLogManager: one-shot `log_event(...)` for
    summary lines, plus the `stage(...)` context manager for timed,
    memory/CPU-sampled STARTED->COMPLETED/FAILED stage blocks."""

    def __init__(self, db: DatabaseClient) -> None:
        self._mgr = PipelineLogManager(db)

    def log_event(
        self,
        dataset_id: int,
        scene_id: str,
        module: str,
        stage: str,
        status: str,
        message: str,
        details: dict | None = None,
    ) -> int:
        return self._mgr.log_event(dataset_id, scene_id, module, stage, status, message, details)

    @contextmanager
    def stage(
        self,
        dataset_id: int,
        scene_id: str,
        module: str,
        stage: str,
        message: str,
        **input_details: Any,
    ) -> Iterator[_StageHandle]:
        handle = _StageHandle()
        self._mgr.log_event(dataset_id, scene_id, module, stage, "STARTED", message, input_details)

        sampler = _MemorySampler()
        sampler.start()
        started = time.monotonic()
        try:
            yield handle
        except Exception as exc:
            duration = round(time.monotonic() - started, 3)
            peak_mb, peak_cpu = sampler.stop()
            self._mgr.log_event(
                dataset_id, scene_id, module, stage, "FAILED",
                f"{stage} failed: {exc}",
                {
                    **input_details,
                    "duration_seconds": duration,
                    "memory_peak_mb": peak_mb,
                    "cpu_peak_percent": peak_cpu,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "traceback": traceback.format_exc()[-_TRACEBACK_MAX_CHARS:],
                },
            )
            raise
        else:
            duration = round(time.monotonic() - started, 3)
            peak_mb, peak_cpu = sampler.stop()
            handle.duration_seconds = duration
            handle.memory_peak_mb = peak_mb
            handle.cpu_peak_percent = peak_cpu
            completion_message = handle.details.pop("message", None) or f"{stage} completed"
            self._mgr.log_event(
                dataset_id, scene_id, module, stage, "COMPLETED",
                completion_message,
                {
                    **input_details,
                    **handle.details,
                    "duration_seconds": duration,
                    "memory_peak_mb": peak_mb,
                    "cpu_peak_percent": peak_cpu,
                },
            )
