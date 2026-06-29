# api/schemas.py
"""
Pydantic v2 request/response schemas for all API endpoints.

Author : Julius Marselinus (BRONTO) - NIM 00000111989
Program: Sistem Informasi - Universitas Multimedia Nusantara
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# SHARED BASE MODELS
# ---------------------------------------------------------------------------

class OkResponse(BaseModel):
    """Generic success confirmation."""
    ok: bool = True
    message: str = "Success"


# ---------------------------------------------------------------------------
# SCENE SCHEMAS
# ---------------------------------------------------------------------------

class SceneListItem(BaseModel):
    """Summary representation of a satellite scene (list view)."""
    scene_id:             int
    scene_uuid:           str
    product_identifier:   str
    platform:             str
    instrument_mode:      str
    polarization_vv:      bool
    polarization_vh:      bool
    acquisition_datetime: datetime
    orbit_direction:      str
    orbit_number:         int | None
    relative_orbit:       int | None
    cloud_cover_percent:  float | None
    resolution_m:         int
    region_id:            int
    is_available:         bool
    created_at:           datetime

    model_config = {"from_attributes": True}


class SceneDetail(SceneListItem):
    """Full scene detail including file paths and checksums."""
    raw_file_path:        str | None
    raw_file_size_mb:     float | None
    download_url:         str | None
    checksum_md5:         str | None
    incidence_angle_near: float | None
    incidence_angle_far:  float | None
    updated_at:           datetime


class SceneListResponse(BaseModel):
    """Paginated scene list response."""
    total:  int
    limit:  int
    offset: int
    items:  list[SceneListItem]


class SceneQueryParams(BaseModel):
    """Query filters for scene list endpoint."""
    region_id:       int | None = None
    orbit_direction: str | None = Field(None, pattern="^(ASCENDING|DESCENDING)$")
    date_from:       datetime | None = None
    date_to:         datetime | None = None
    min_quality:     float | None = Field(None, ge=0, le=100)
    only_gold:       bool = False
    limit:           int  = Field(20, ge=1, le=200)
    offset:          int  = Field(0,  ge=0)


# ---------------------------------------------------------------------------
# PRODUCT SCHEMAS
# ---------------------------------------------------------------------------

class ProductItem(BaseModel):
    """Data product representation."""
    product_id:       int
    product_uuid:     str
    scene_id:         int
    job_id:           int
    product_tier:     str
    product_type:     str
    band_name:        str
    file_name:        str
    file_path:        str
    file_size_mb:     float
    file_format:      str
    data_hash_sha256: str
    crs:              str
    pixel_size_m:     float | None
    rows:             int | None
    cols:             int | None
    band_count:       int
    storage_location: str
    is_valid:         bool
    is_latest:        bool
    created_at:       datetime

    model_config = {"from_attributes": True}


class ProductListResponse(BaseModel):
    total:  int
    limit:  int
    offset: int
    items:  list[ProductItem]


class IntegrityCheckResponse(BaseModel):
    """Response for hash integrity verification."""
    product_id:    int
    file_path:     str
    stored_hash:   str
    computed_hash: str
    integrity_ok:  bool
    file_size_mb:  float


# ---------------------------------------------------------------------------
# QUALITY SCHEMAS
# ---------------------------------------------------------------------------

class QualityMetricItem(BaseModel):
    """Quality metric for one band of a scene."""
    metric_id:               int
    scene_id:                int
    product_id:              int
    band_name:               str
    assessed_at:             datetime
    total_pixels:            int
    valid_pixels:            int
    nodata_pixels:           int
    nodata_percent:          float | None
    backscatter_mean_db:     float | None
    backscatter_std_db:      float | None
    backscatter_min_db:      float | None
    backscatter_max_db:      float | None
    cloud_threshold_percent: float
    radiometric_consistency: bool | None
    speckle_index:           float | None
    quality_score:           float
    quality_flag:            str
    notes:                   str | None
    created_at:              datetime

    model_config = {"from_attributes": True}


class QualityResponse(BaseModel):
    """Quality metrics for all bands of a scene."""
    scene_id: int
    bands:    list[QualityMetricItem]
    overall_quality: str  # PASS if all bands pass, else FAIL/WARNING


# ---------------------------------------------------------------------------
# LINEAGE SCHEMAS
# ---------------------------------------------------------------------------

class LineageStep(BaseModel):
    """A single transformation step in the provenance chain."""
    lineage_id:            int
    parent_product_id:     int
    child_product_id:      int
    transformation_type:   str
    stage_id:              int
    job_id:                int
    transformation_params: dict[str, Any]
    input_checksum:        str | None
    output_checksum:       str | None
    created_at:            datetime


class LineageResponse(BaseModel):
    """Full transformation chain for a product."""
    product_id: int
    direction:  str
    chain:      list[LineageStep]
    total_steps: int


# ---------------------------------------------------------------------------
# JOB / PIPELINE STATUS SCHEMAS
# ---------------------------------------------------------------------------

class JobStatusItem(BaseModel):
    """Execution status for a single pipeline stage."""
    job_id:         int
    stage_name:     str
    stage_order:    int
    attempt_number: int
    status:         str
    queued_at:      datetime
    started_at:     datetime | None
    completed_at:   datetime | None
    error_message:  str | None


class PipelineStatusResponse(BaseModel):
    """Full pipeline status for a scene."""
    scene_id: int
    stages:   list[JobStatusItem]
    overall_status: str  # 'COMPLETE' | 'IN_PROGRESS' | 'FAILED' | 'NOT_STARTED'


# ---------------------------------------------------------------------------
# HEALTH SCHEMA
# ---------------------------------------------------------------------------

class HealthResponse(BaseModel):
    """API and database health status."""
    status:      str
    db_connected: bool
    pool_size:   int | None
    checked_out: int | None
    api_version: str = "1.0.0"
    timestamp:   datetime
