# api/routes/datasets.py
"""
POST   /api/datasets                — queue a new dataset build
GET    /api/datasets                — list datasets
GET    /api/datasets/{id}           — dataset detail
POST   /api/datasets/{id}/pause     — pause processing
POST   /api/datasets/{id}/resume    — resume processing
DELETE /api/datasets/{id}           — delete dataset (and its scene/product rows)
GET    /api/datasets/{id}/status    — scene-level progress summary
GET    /api/datasets/{id}/download  — download built dataset (not yet implemented)
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from etl.database_client import (
    DatabaseClient,
    Dataset,
    DatasetProduct,
    DatasetScene,
    ProductTierEnum,
)

router = APIRouter()
logger = logging.getLogger(__name__)


def _get_db() -> DatabaseClient:
    from api.main import get_db
    return get_db()


# ---------------------------------------------------------------------------
# Request / response schemas
# ---------------------------------------------------------------------------

class DatasetCreateRequest(BaseModel):
    location: str
    date_start: str
    date_end: str
    tiers: list[str] = Field(..., min_length=1)
    name: str
    description: str | None = None
    quality_settings: dict | None = None


class DatasetCreateResponse(BaseModel):
    dataset_id: int
    status: str
    created_at: str


class DatasetListItem(BaseModel):
    id: int
    name: str
    location: str
    status: str
    created_at: str


class DatasetListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[DatasetListItem]


class DatasetDetail(BaseModel):
    id: int
    name: str
    location: str
    date_start: str
    date_end: str
    required_tiers: list | None
    quality_settings: dict | None
    status: str
    created_at: str


class DatasetStatusResponse(BaseModel):
    dataset_id: int
    status: str
    total_scenes: int
    completed: int
    failed: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_dataset_or_404(sess, dataset_id: int) -> Dataset:
    ds = sess.get(Dataset, dataset_id)
    if not ds:
        raise HTTPException(404, f"Dataset {dataset_id} not found")
    return ds


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.post(
    "",
    response_model=DatasetCreateResponse,
    status_code=201,
    summary="Queue a new dataset build",
)
async def create_dataset(
    payload: DatasetCreateRequest,
    db: DatabaseClient = Depends(_get_db),
) -> DatasetCreateResponse:
    valid_tiers = {t.value for t in ProductTierEnum}
    invalid = [t for t in payload.tiers if t not in valid_tiers]
    if invalid:
        raise HTTPException(400, f"Invalid tiers: {invalid}. Valid: {sorted(valid_tiers)}")

    with db.session() as sess:
        ds = Dataset(
            name=payload.name,
            location=payload.location,
            date_start=payload.date_start,
            date_end=payload.date_end,
            required_tiers=payload.tiers,
            quality_settings=payload.quality_settings or {},
            status="QUEUED",
        )
        sess.add(ds)
        sess.flush()
        return DatasetCreateResponse(
            dataset_id=ds.id,
            status=ds.status,
            created_at=str(ds.created_at),
        )


@router.get(
    "",
    response_model=DatasetListResponse,
    summary="List datasets",
)
async def list_datasets(
    limit: int = 50,
    offset: int = 0,
    db: DatabaseClient = Depends(_get_db),
) -> DatasetListResponse:
    with db.session() as sess:
        total = sess.query(Dataset).count()
        items = (
            sess.query(Dataset)
            .order_by(Dataset.created_at.desc())
            .limit(limit)
            .offset(offset)
            .all()
        )
        return DatasetListResponse(
            total=total,
            limit=limit,
            offset=offset,
            items=[
                DatasetListItem(
                    id=d.id,
                    name=d.name,
                    location=d.location,
                    status=d.status,
                    created_at=str(d.created_at),
                )
                for d in items
            ],
        )


@router.get(
    "/{dataset_id}",
    response_model=DatasetDetail,
    summary="Get dataset detail",
)
async def get_dataset(
    dataset_id: int,
    db: DatabaseClient = Depends(_get_db),
) -> DatasetDetail:
    with db.session() as sess:
        ds = _get_dataset_or_404(sess, dataset_id)
        return DatasetDetail(
            id=ds.id,
            name=ds.name,
            location=ds.location,
            date_start=str(ds.date_start),
            date_end=str(ds.date_end),
            required_tiers=ds.required_tiers,
            quality_settings=ds.quality_settings,
            status=ds.status,
            created_at=str(ds.created_at),
        )


@router.post(
    "/{dataset_id}/pause",
    summary="Pause dataset processing",
)
async def pause_dataset(
    dataset_id: int,
    db: DatabaseClient = Depends(_get_db),
) -> dict:
    with db.session() as sess:
        ds = _get_dataset_or_404(sess, dataset_id)
        ds.status = "PAUSED"
        return {"dataset_id": dataset_id, "status": ds.status}


@router.post(
    "/{dataset_id}/resume",
    summary="Resume dataset processing",
)
async def resume_dataset(
    dataset_id: int,
    db: DatabaseClient = Depends(_get_db),
) -> dict:
    with db.session() as sess:
        ds = _get_dataset_or_404(sess, dataset_id)
        ds.status = "PROCESSING"
        return {"dataset_id": dataset_id, "status": ds.status}


@router.delete(
    "/{dataset_id}",
    summary="Delete a dataset",
    description="Deletes the dataset along with its dataset_products and dataset_scenes rows.",
)
async def delete_dataset(
    dataset_id: int,
    db: DatabaseClient = Depends(_get_db),
) -> dict:
    with db.session() as sess:
        ds = _get_dataset_or_404(sess, dataset_id)
        sess.query(DatasetProduct).filter(DatasetProduct.dataset_id == dataset_id).delete()
        sess.query(DatasetScene).filter(DatasetScene.dataset_id == dataset_id).delete()
        sess.delete(ds)
        return {"deleted": True, "dataset_id": dataset_id}


@router.get(
    "/{dataset_id}/status",
    response_model=DatasetStatusResponse,
    summary="Get dataset scene-level progress",
)
async def get_dataset_status(
    dataset_id: int,
    db: DatabaseClient = Depends(_get_db),
) -> DatasetStatusResponse:
    with db.session() as sess:
        ds = _get_dataset_or_404(sess, dataset_id)
        total = sess.query(DatasetScene).filter(DatasetScene.dataset_id == dataset_id).count()
        completed = sess.query(DatasetScene).filter(
            DatasetScene.dataset_id == dataset_id, DatasetScene.status == "COMPLETED"
        ).count()
        failed = sess.query(DatasetScene).filter(
            DatasetScene.dataset_id == dataset_id, DatasetScene.status == "FAILED"
        ).count()
        return DatasetStatusResponse(
            dataset_id=dataset_id,
            status=ds.status,
            total_scenes=total,
            completed=completed,
            failed=failed,
        )


@router.get(
    "/{dataset_id}/download",
    summary="Download built dataset",
    description="Not yet implemented — reserved for future zip export of dataset products.",
)
async def download_dataset(dataset_id: int) -> None:
    raise HTTPException(status_code=501, detail="Download not yet implemented")
