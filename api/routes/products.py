# api/routes/products.py
"""
GET /api/products              — list products with filters
GET /api/products/{id}         — product detail
GET /api/products/{id}/verify  — integrity hash check
"""

from __future__ import annotations

import logging
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from sqlalchemy import func, select

from api.schemas import IntegrityCheckResponse, ProductItem, ProductListResponse
from etl.database_client import DataProduct, DatabaseClient, ProductTierEnum
from etl.lineage_tracker import LineageTracker
from etl.metadata_manager import MetadataManager

router = APIRouter()
logger = logging.getLogger(__name__)


def _get_db() -> DatabaseClient:
    from api.main import get_db
    return get_db()


def _product_to_schema(p: DataProduct) -> ProductItem:
    return ProductItem(
        product_id       = p.product_id,
        product_uuid     = str(p.product_uuid),
        scene_id         = p.scene_id,
        job_id           = p.job_id,
        product_tier     = p.product_tier.value,
        product_type     = p.product_type,
        band_name        = p.band_name,
        file_name        = p.file_name,
        file_path        = p.file_path,
        file_size_mb     = float(p.file_size_mb),
        file_format      = p.file_format,
        data_hash_sha256 = p.data_hash_sha256,
        crs              = p.crs,
        pixel_size_m     = float(p.pixel_size_m) if p.pixel_size_m else None,
        rows             = p.rows,
        cols             = p.cols,
        band_count       = p.band_count,
        storage_location = p.storage_location.value,
        is_valid         = p.is_valid,
        is_latest        = p.is_latest,
        created_at       = p.created_at,
    )


@router.get(
    "",
    response_model=ProductListResponse,
    summary="List data products",
    description="List output products with optional filters for tier, band, scene, validity.",
)
async def list_products(
    db:          DatabaseClient = Depends(_get_db),
    scene_id:    int | None     = Query(None, description="Filter by scene_id"),
    tier:        str | None     = Query(None, description="RAW | BRONZE | SILVER | GOLD"),
    band_name:   str | None     = Query(None, description="VV | VH | VV_VH"),
    latest_only: bool           = Query(True,  description="Only is_latest=TRUE products"),
    valid_only:  bool           = Query(True,  description="Only is_valid=TRUE products"),
    limit:       int            = Query(20, ge=1, le=200),
    offset:      int            = Query(0,  ge=0),
) -> ProductListResponse:
    with db.session() as sess:
        stmt = select(DataProduct)

        if scene_id:
            stmt = stmt.where(DataProduct.scene_id == scene_id)
        if tier:
            try:
                stmt = stmt.where(DataProduct.product_tier == ProductTierEnum(tier))
            except ValueError:
                raise HTTPException(400, f"Invalid tier: {tier}. Valid: RAW, BRONZE, SILVER, GOLD")
        if band_name:
            stmt = stmt.where(DataProduct.band_name == band_name)
        if latest_only:
            stmt = stmt.where(DataProduct.is_latest == True)
        if valid_only:
            stmt = stmt.where(DataProduct.is_valid == True)

        total    = sess.scalar(select(func.count()).select_from(stmt.subquery()))
        products = sess.scalars(
            stmt.order_by(DataProduct.created_at.desc()).limit(limit).offset(offset)
        ).all()

        return ProductListResponse(
            total  = total or 0,
            limit  = limit,
            offset = offset,
            items  = [_product_to_schema(p) for p in products],
        )


@router.get(
    "/{product_id}",
    response_model=ProductItem,
    summary="Get product detail",
    description="Retrieve full metadata for a single data product.",
)
async def get_product(
    product_id: int,
    db: DatabaseClient = Depends(_get_db),
) -> ProductItem:
    with db.session() as sess:
        p = sess.get(DataProduct, product_id)
        if not p:
            raise HTTPException(404, f"Product {product_id} not found")
        return _product_to_schema(p)


@router.get(
    "/{product_id}/download",
    summary="Download product file",
    description="Stream the output file (COG, filtered TIFF) for download.",
)
async def download_product(
    product_id: int,
    db: DatabaseClient = Depends(_get_db),
) -> FileResponse:
    with db.session() as sess:
        p = sess.get(DataProduct, product_id)
        if not p:
            raise HTTPException(404, f"Product {product_id} not found")
        file_path = Path(p.file_path)
        file_name = p.file_name

    if not file_path.exists():
        raise HTTPException(
            404,
            f"File not found on disk: {file_path}. "
            "Storage may be remote (S3/GCS) — use direct cloud URL."
        )

    return FileResponse(
        path         = str(file_path),
        filename     = file_name,
        media_type   = "image/tiff",
        headers      = {"Content-Disposition": f'attachment; filename="{file_name}"'},
    )


@router.get(
    "/{product_id}/verify",
    response_model=IntegrityCheckResponse,
    summary="Verify file integrity",
    description="Recompute SHA-256 and compare against stored hash. Detects file corruption.",
)
async def verify_product(
    product_id: int,
    db: DatabaseClient = Depends(_get_db),
) -> IntegrityCheckResponse:
    with db.session() as sess:
        p = sess.get(DataProduct, product_id)
        if not p:
            raise HTTPException(404, f"Product {product_id} not found")
        file_path = p.file_path

    if not Path(file_path).exists():
        raise HTTPException(404, f"File not found on disk: {file_path}")

    tracker = LineageTracker(db)
    result  = tracker.verify_integrity(product_id, file_path)
    return IntegrityCheckResponse(**result)
