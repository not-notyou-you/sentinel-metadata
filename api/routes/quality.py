# api/routes/quality.py
"""
GET /api/quality/{scene_id} — quality metrics for all bands of a scene
GET /api/quality/summary     — aggregated quality statistics
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select

from api.schemas import QualityMetricItem, QualityResponse
from etl.database_client import DatabaseClient, QualityMetric, SatelliteScene
from etl.metadata_manager import MetadataManager

router = APIRouter()
logger = logging.getLogger(__name__)


def _get_db() -> DatabaseClient:
    from api.main import get_db
    return get_db()


@router.get(
    "/{scene_id}",
    response_model=QualityResponse,
    summary="Scene quality metrics",
    description=(
        "Retrieve radiometric quality metrics for all processed bands of a scene. "
        "Returns per-band backscatter statistics, nodata percentage, speckle index, "
        "and composite quality score."
    ),
)
async def get_quality(
    scene_id: int,
    db: DatabaseClient = Depends(_get_db),
) -> QualityResponse:
    with db.session() as sess:
        scene = sess.get(SatelliteScene, scene_id)
        if not scene:
            raise HTTPException(404, f"Scene {scene_id} not found")

        metrics = sess.scalars(
            select(QualityMetric)
            .where(QualityMetric.scene_id == scene_id)
            .order_by(QualityMetric.band_name)
        ).all()

    if not metrics:
        raise HTTPException(
            404,
            f"No quality metrics found for scene {scene_id}. "
            "Run Module 6 (QUALITY_ANALYTICS) first."
        )

    band_items = [
        QualityMetricItem(
            metric_id               = m.metric_id,
            scene_id                = m.scene_id,
            product_id              = m.product_id,
            band_name               = m.band_name,
            assessed_at             = m.assessed_at,
            total_pixels            = m.total_pixels,
            valid_pixels            = m.valid_pixels,
            nodata_pixels           = m.nodata_pixels,
            nodata_percent          = None,  # DB generated column, read via raw SQL if needed
            backscatter_mean_db     = float(m.backscatter_mean_db) if m.backscatter_mean_db else None,
            backscatter_std_db      = float(m.backscatter_std_db) if m.backscatter_std_db else None,
            backscatter_min_db      = float(m.backscatter_min_db) if m.backscatter_min_db else None,
            backscatter_max_db      = float(m.backscatter_max_db) if m.backscatter_max_db else None,
            cloud_threshold_percent = float(m.cloud_threshold_percent),
            radiometric_consistency = m.radiometric_consistency,
            speckle_index           = float(m.speckle_index) if m.speckle_index else None,
            quality_score           = float(m.quality_score),
            quality_flag            = m.quality_flag,
            notes                   = m.notes,
            created_at              = m.created_at,
        )
        for m in metrics
    ]

    flags = [b.quality_flag for b in band_items]
    if all(f == "PASS" for f in flags):
        overall = "PASS"
    elif any(f == "FAIL" for f in flags):
        overall = "FAIL"
    else:
        overall = "WARNING"

    return QualityResponse(scene_id=scene_id, bands=band_items, overall_quality=overall)


@router.get(
    "/summary/stats",
    summary="Quality summary statistics",
    description="Aggregated quality statistics across all processed scenes.",
)
async def quality_summary(
    db:        DatabaseClient = Depends(_get_db),
    region_id: int | None     = Query(None),
    n_days:    int            = Query(30, ge=1, le=365),
) -> dict:
    """Returns count of PASS/FAIL/WARNING scenes and average quality score."""
    from datetime import datetime, timedelta, timezone
    from sqlalchemy import and_

    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=n_days)

    with db.session() as sess:
        stmt = select(
            QualityMetric.quality_flag,
            func.count(QualityMetric.metric_id).label("count"),
            func.avg(QualityMetric.quality_score).label("avg_score"),
        ).where(
            QualityMetric.assessed_at >= cutoff
        ).group_by(QualityMetric.quality_flag)

        rows = sess.execute(stmt).all()

    summary = {
        "period_days": n_days,
        "region_id":   region_id,
        "flags":       {r.quality_flag: {"count": r.count, "avg_score": round(float(r.avg_score or 0), 2)} for r in rows},
    }
    return summary
