# api/routes/datasets.py
from __future__ import annotations
import logging
import os
import re
import tempfile
import zipfile
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from starlette.background import BackgroundTask
from api.schemas import (
    DatasetCreateRequest,
    DatasetCreateResponse,
    DatasetDeleteResponse,
    DatasetDetail,
    DatasetListResponse,
    DatasetPauseRequest,
    DatasetPauseResponse,
    DatasetProgressResponse,
    DatasetResumeResponse,
    DeletionProgressResponse,
)
from etl.database_client import DatabaseClient
from etl.dataset_manager import DatasetManager

router = APIRouter()
logger = logging.getLogger(__name__)


def _get_db() -> DatabaseClient:
    from api.main import get_db
    return get_db()


def _mgr(db: DatabaseClient) -> DatasetManager:
    return DatasetManager(db)


def _slugify(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]", "_", name.strip()) or "dataset"


@router.post("", response_model=DatasetCreateResponse, summary="Buat dataset baru")
async def create_dataset(
    req: DatasetCreateRequest,
    db: DatabaseClient = Depends(_get_db),
) -> DatasetCreateResponse:
    try:
        result = _mgr(db).create_dataset(
            location=req.location,
            date_start=req.date_start,
            date_end=req.date_end,
            tiers=req.tiers,
            name=req.name,
            description=req.description,
            quality_settings=req.quality_settings.model_dump() if req.quality_settings else None,
        )
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(400, str(exc))
    return DatasetCreateResponse(**result)


@router.get("", response_model=DatasetListResponse, summary="List dataset")
async def list_datasets(
    db: DatabaseClient = Depends(_get_db),
    limit: int = Query(20, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> DatasetListResponse:
    result = _mgr(db).list_datasets(limit=limit, offset=offset, dataset_kind="STANDARD")
    return DatasetListResponse(**result)


@router.get("/{dataset_id}", response_model=DatasetDetail, summary="Detail dataset")
async def get_dataset(dataset_id: int, db: DatabaseClient = Depends(_get_db)) -> DatasetDetail:
    result = _mgr(db).get_dataset(dataset_id)
    if result is None:
        raise HTTPException(404, f"Dataset {dataset_id} tidak ditemukan")
    return DatasetDetail(**result)


@router.get("/{dataset_id}/status", response_model=DatasetProgressResponse, summary="Progres pipeline dataset")
async def get_dataset_status(dataset_id: int, db: DatabaseClient = Depends(_get_db)) -> DatasetProgressResponse:
    result = _mgr(db).get_progress(dataset_id)
    if result is None:
        raise HTTPException(404, f"Dataset {dataset_id} tidak ditemukan")
    return DatasetProgressResponse(**result)


@router.post("/{dataset_id}/pause", response_model=DatasetPauseResponse, summary="Pause dataset")
async def pause_dataset(
    dataset_id: int,
    req: DatasetPauseRequest = DatasetPauseRequest(),
    db: DatabaseClient = Depends(_get_db),
) -> DatasetPauseResponse:
    try:
        result = _mgr(db).pause_dataset(dataset_id, reason=req.reason)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    return DatasetPauseResponse(**result)


@router.post("/{dataset_id}/resume", response_model=DatasetResumeResponse, summary="Resume dataset")
async def resume_dataset(dataset_id: int, db: DatabaseClient = Depends(_get_db)) -> DatasetResumeResponse:
    try:
        result = _mgr(db).resume_dataset(dataset_id)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    return DatasetResumeResponse(**result)


@router.delete("/{dataset_id}", response_model=DatasetDeleteResponse, summary="Hapus dataset")
async def delete_dataset(
    dataset_id: int,
    force: bool = Query(False, description="Paksa hentikan proses yang sedang berjalan lalu hapus"),
    db: DatabaseClient = Depends(_get_db),
) -> DatasetDeleteResponse:
    try:
        result = _mgr(db).delete_dataset(dataset_id, force=force)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    return DatasetDeleteResponse(**result)


@router.get("/{dataset_id}/deletion-progress", response_model=DeletionProgressResponse, summary="Progres penghapusan")
async def get_deletion_progress(dataset_id: int, db: DatabaseClient = Depends(_get_db)) -> DeletionProgressResponse:
    result = _mgr(db).get_deletion_progress(dataset_id)
    if result is None:
        raise HTTPException(404, "Tidak ada proses penghapusan untuk dataset ini")
    return DeletionProgressResponse(**result)


@router.get("/{dataset_id}/download", summary="Unduh dataset (ZIP)")
async def download_dataset(dataset_id: int, db: DatabaseClient = Depends(_get_db)) -> FileResponse:
    info = _mgr(db).get_dataset(dataset_id)
    if info is None:
        raise HTTPException(404, f"Dataset {dataset_id} tidak ditemukan")

    base_dir = Path("data") / "datasets" / str(dataset_id)
    files = [f for f in base_dir.rglob("*") if f.is_file()] if base_dir.exists() else []
    if not files:
        raise HTTPException(404, "Dataset belum memiliki file untuk diunduh")

    tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
    tmp.close()
    with zipfile.ZipFile(tmp.name, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in files:
            zf.write(f, arcname=str(f.relative_to(base_dir)))

    filename = f"{_slugify(info['name'])}.zip"
    return FileResponse(
        tmp.name,
        filename=filename,
        media_type="application/zip",
        background=BackgroundTask(os.remove, tmp.name),
    )
