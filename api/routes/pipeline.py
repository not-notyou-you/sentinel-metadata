# api/routes/pipeline.py
from __future__ import annotations

import logging
import threading

from fastapi import APIRouter, Depends
from sqlalchemy import select

from etl.database_client import DatabaseClient, ProcessingJob, SatelliteScene
from etl.metadata_manager import MetadataManager

router = APIRouter()
logger = logging.getLogger(__name__)

_trigger_lock = threading.Lock()
_trigger_running = False


def _get_db() -> DatabaseClient:
    from api.main import get_db
    return get_db()


@router.get(
    "/status/current",
    summary="Status pipeline saat ini",
    description="Status tahap pipeline untuk scene yang paling baru diproses, dipakai untuk progress rail di beranda.",
)
async def current_pipeline_status(db: DatabaseClient = Depends(_get_db)) -> dict:
    with db.session() as sess:
        latest_job = sess.execute(
            select(ProcessingJob).order_by(ProcessingJob.created_at.desc()).limit(1)
        ).scalar_one_or_none()

        if not latest_job:
            return {"active": False, "scene_id": None, "stages": [], "overall_status": "NONE"}

        scene = sess.get(SatelliteScene, latest_job.scene_id)
        scene_id = latest_job.scene_id
        product_identifier = scene.product_identifier if scene else None

    meta = MetadataManager(db)
    stages = meta.get_pipeline_status(scene_id)

    if not stages:
        overall = "NOT_STARTED"
    elif all(s["status"] == "SUCCESS" for s in stages):
        overall = "COMPLETE"
    elif any(s["status"] == "FAILED" for s in stages):
        overall = "FAILED"
    else:
        overall = "IN_PROGRESS"

    return {
        "active": overall == "IN_PROGRESS",
        "scene_id": scene_id,
        "product_identifier": product_identifier,
        "stages": stages,
        "overall_status": overall,
    }


def _run_pipeline_background() -> None:
    global _trigger_running
    from etl.scheduler import load_pipeline_config, run_pipeline_once
    try:
        cfg = load_pipeline_config()
        run_pipeline_once(cfg)
    except Exception:
        logger.exception("[pipeline] background run failed")
    finally:
        with _trigger_lock:
            _trigger_running = False


@router.post(
    "/trigger",
    summary="Jalankan pipeline sekarang",
    description="Memicu satu siklus pipeline (discover, download, kalibrasi, crop, filter, export, QA) di background thread.",
)
async def trigger_pipeline() -> dict:
    global _trigger_running
    with _trigger_lock:
        if _trigger_running:
            return {"started": False, "message": "Pipeline run sudah berjalan"}
        _trigger_running = True

    thread = threading.Thread(target=_run_pipeline_background, daemon=True)
    thread.start()
    return {"started": True, "message": "Pipeline run dimulai di background"}