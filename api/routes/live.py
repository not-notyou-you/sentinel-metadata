# api/routes/live.py
"""
GET  /api/live            — live-mode status
POST /api/live/toggle     — enable/disable live mode
POST /api/live/clear      — clear backfill date range
POST /api/live/backfill   — queue a backfill for a date range
"""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends

from etl.database_client import DatabaseClient, LiveConfig

router = APIRouter()


def _get_db() -> DatabaseClient:
    from api.main import get_db
    return get_db()


def _get_or_create_config(sess) -> LiveConfig:
    lc = sess.query(LiveConfig).first()
    if not lc:
        lc = LiveConfig(enabled=False)
        sess.add(lc)
        sess.flush()
    return lc


@router.get("/live", summary="Get live-mode status")
async def get_live_status(db: DatabaseClient = Depends(_get_db)) -> dict:
    with db.session() as sess:
        lc = _get_or_create_config(sess)
        return {
            "enabled": lc.enabled,
            "last_check": str(lc.last_check_datetime) if lc.last_check_datetime else None,
            "recent_scenes": [],
        }


@router.post("/live/toggle", summary="Enable or disable live mode")
async def toggle_live(enabled: bool, db: DatabaseClient = Depends(_get_db)) -> dict:
    with db.session() as sess:
        lc = _get_or_create_config(sess)
        lc.enabled = enabled
        lc.updated_at = datetime.now()
        return {
            "enabled": lc.enabled,
            "message": f"Live mode {'enabled' if enabled else 'disabled'}",
        }


@router.post("/live/clear", summary="Clear the backfill date range")
async def clear_live(db: DatabaseClient = Depends(_get_db)) -> dict:
    with db.session() as sess:
        lc = _get_or_create_config(sess)
        lc.backfill_date_start = None
        lc.backfill_date_end = None
        return {"status": "cleared"}


@router.post("/live/backfill", summary="Queue a backfill for a date range")
async def backfill_live(
    date_start: str,
    date_end: str,
    db: DatabaseClient = Depends(_get_db),
) -> dict:
    with db.session() as sess:
        lc = _get_or_create_config(sess)
        lc.backfill_date_start = date_start
        lc.backfill_date_end = date_end
        # TODO: enqueue actual backfill job once job runner exists
        return {"job_id": 1, "status": "QUEUED", "scene_count": 0}
