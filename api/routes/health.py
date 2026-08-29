# api/routes/health.py
"""GET /api/health — database connectivity and pool statistics."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends

from api.schemas import HealthResponse
from etl.database_client import DatabaseClient

router = APIRouter()


async def _get_db() -> DatabaseClient:
    from api.main import get_db
    return get_db()


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Health check",
    description="Returns database connectivity status and connection pool statistics.",
)
async def health_check(db: DatabaseClient = Depends(_get_db)) -> HealthResponse:
    info = db.check_health()
    return HealthResponse(
        status        = "ok" if info.get("connected") else "degraded",
        db_connected  = info.get("connected", False),
        pool_size     = info.get("pool_size"),
        checked_out   = info.get("checked_out"),
        timestamp     = datetime.now(tz=timezone.utc),
    )
