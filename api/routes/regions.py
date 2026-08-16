# api/routes/regions.py
from __future__ import annotations
from fastapi import APIRouter, Depends
from geoalchemy2.shape import to_shape
from sqlalchemy import select
from api.schemas import RegionItem, RegionListResponse
from etl.database_client import DatabaseClient, RegionOfInterest

router = APIRouter()


def _get_db() -> DatabaseClient:
    from api.main import get_db
    return get_db()


@router.get("", response_model=RegionListResponse, summary="Daftar wilayah preset")
async def list_regions(db: DatabaseClient = Depends(_get_db)) -> RegionListResponse:
    with db.session() as sess:
        rows = sess.scalars(
            select(RegionOfInterest)
            .where(RegionOfInterest.is_active == True)
            .order_by(RegionOfInterest.name)
        ).all()
        items = []
        for r in rows:
            bounds = to_shape(r.bbox).bounds
            items.append(RegionItem(
                region_id=r.region_id,
                region_code=r.region_code,
                name=r.name,
                description=r.description,
                bbox=list(bounds),
                area_km2=float(r.area_km2) if r.area_km2 else None,
            ))
    return RegionListResponse(items=items)