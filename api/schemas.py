# api/schemas.py
from __future__ import annotations
from datetime import date, datetime
from typing import Any
from uuid import UUID
from pydantic import BaseModel, Field, field_validator, model_validator


class OkResponse(BaseModel):
    ok: bool = True
    message: str = "Success"


class SceneListItem(BaseModel):
    scene_id: int
    scene_uuid: str
    product_identifier: str
    platform: str
    instrument_mode: str
    polarization_vv: bool
    polarization_vh: bool
    acquisition_datetime: datetime
    orbit_direction: str
    orbit_number: int | None
    relative_orbit: int | None
    cloud_cover_percent: float | None
    resolution_m: int
    region_id: int
    is_available: bool
    created_at: datetime
    model_config = {"from_attributes": True}


class SceneDetail(SceneListItem):
    raw_file_path: str | None
    raw_file_size_mb: float | None
    download_url: str | None
    checksum_md5: str | None
    incidence_angle_near: float | None
    incidence_angle_far: float | None
    updated_at: datetime


class SceneListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[SceneListItem]


class SceneQueryParams(BaseModel):
    region_id: int | None = None
    orbit_direction: str | None = Field(None, pattern="^(ASCENDING|DESCENDING)$")
    date_from: datetime | None = None
    date_to: datetime | None = None
    min_quality: float | None = Field(None, ge=0, le=100)
    only_gold: bool = False
    limit: int = Field(20, ge=1, le=200)
    offset: int = Field(0, ge=0)


class ProductItem(BaseModel):
    product_id: int
    product_uuid: str
    scene_id: int
    job_id: int
    dataset_id: int | None = None
    product_tier: str
    product_type: str
    band_name: str
    file_name: str
    file_path: str
    file_size_mb: float
    file_format: str
    data_hash_sha256: str
    crs: str
    pixel_size_m: float | None
    rows: int | None
    cols: int | None
    band_count: int
    storage_location: str
    is_valid: bool
    is_latest: bool
    created_at: datetime
    model_config = {"from_attributes": True}


class ProductListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[ProductItem]


class IntegrityCheckResponse(BaseModel):
    product_id: int
    file_path: str
    stored_hash: str
    computed_hash: str
    integrity_ok: bool
    file_size_mb: float


class QualityMetricItem(BaseModel):
    metric_id: int
    scene_id: int
    product_id: int
    band_name: str
    assessed_at: datetime
    total_pixels: int
    valid_pixels: int
    nodata_pixels: int
    nodata_percent: float | None
    backscatter_mean_db: float | None
    backscatter_std_db: float | None
    backscatter_min_db: float | None
    backscatter_max_db: float | None
    cloud_threshold_percent: float
    radiometric_consistency: bool | None
    speckle_index: float | None
    quality_score: float
    quality_flag: str
    notes: str | None
    created_at: datetime
    model_config = {"from_attributes": True}


class QualityResponse(BaseModel):
    scene_id: int
    bands: list[QualityMetricItem]
    overall_quality: str


class LineageStep(BaseModel):
    lineage_id: int
    parent_product_id: int
    child_product_id: int
    transformation_type: str
    stage_id: int
    job_id: int
    transformation_params: dict[str, Any]
    input_checksum: str | None
    output_checksum: str | None
    created_at: datetime


class LineageResponse(BaseModel):
    product_id: int
    direction: str
    chain: list[LineageStep]
    total_steps: int


class JobStatusItem(BaseModel):
    job_id: int
    stage_name: str
    stage_order: int
    attempt_number: int
    status: str
    queued_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    error_message: str | None


class PipelineStatusResponse(BaseModel):
    scene_id: int
    stages: list[JobStatusItem]
    overall_status: str


class HealthResponse(BaseModel):
    status: str
    db_connected: bool
    pool_size: int | None
    checked_out: int | None
    api_version: str = "1.0.0"
    timestamp: datetime


class DatasetQualitySettings(BaseModel):
    min_cloud_cover: float | None = None
    min_quality_score: float | None = None
    resolution_m: int | None = None


class DatasetCreateRequest(BaseModel):
    location: str
    date_start: date
    date_end: date
    tiers: list[str] = Field(default_factory=lambda: ["GOLD"])
    name: str
    description: str | None = None
    quality_settings: DatasetQualitySettings | None = None

    @field_validator("tiers")
    @classmethod
    def _validate_tiers(cls, v: list[str]) -> list[str]:
        allowed = {"RAW", "BRONZE", "SILVER", "GOLD"}
        normalized = [t.upper() for t in v]
        invalid = set(normalized) - allowed
        if invalid:
            raise ValueError(f"Invalid tiers: {invalid}. Valid: {allowed}")
        if not normalized:
            raise ValueError("tiers must not be empty")
        return normalized

    @model_validator(mode="after")
    def _validate_date_range(self) -> "DatasetCreateRequest":
        if self.date_end < self.date_start:
            raise ValueError("date_end must be >= date_start")
        return self


class DatasetCreateResponse(BaseModel):
    dataset_id: int
    job_id: int
    status: str


class DatasetItem(BaseModel):
    dataset_id: int
    dataset_uuid: str
    name: str
    description: str | None
    location_label: str | None
    date_start: date
    date_end: date
    required_tiers: list[str]
    dataset_kind: str
    status: str
    total_scenes: int
    completed_scenes: int
    failed_scenes: int
    total_size_bytes: int
    is_deletable: bool
    live_enabled: bool
    created_at: datetime
    updated_at: datetime
    model_config = {"from_attributes": True}


class DatasetDetail(DatasetItem):
    bbox_wkt: str
    region_id: int | None
    quality_settings: dict[str, Any]
    live_last_checked_at: datetime | None
    deleted_at: datetime | None


class DatasetListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[DatasetItem]


class SceneJobStateItem(BaseModel):
    id: int
    job_id: int
    product_identifier: str
    scene_id: int | None
    current_stage: str | None
    stage_status: str
    attempt_number: int
    max_retries: int
    last_error: str | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    model_config = {"from_attributes": True}


class DatasetJobItem(BaseModel):
    job_id: int
    job_uuid: str
    dataset_id: int
    job_type: str
    status: str
    paused_at: datetime | None
    paused_by: str | None
    pause_reason: str | None
    resumed_at: datetime | None
    resume_count: int
    total_scenes: int
    downloaded_count: int
    processed_count: int
    failed_count: int
    cleaned_count: int
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    model_config = {"from_attributes": True}


class DatasetProgressResponse(BaseModel):
    dataset_id: int
    job_id: int | None
    status: str
    total_scenes: int
    downloaded_count: int
    processed_count: int
    failed_count: int
    cleaned_count: int
    progress_percent: int
    paused: bool
    pause_reason: str | None
    scenes: list[SceneJobStateItem]


class DatasetPauseRequest(BaseModel):
    reason: str = "user_requested"


class DatasetPauseResponse(BaseModel):
    status: str
    reason: str | None


class DatasetResumeResponse(BaseModel):
    status: str
    resume_count: int


class DatasetDeleteResponse(BaseModel):
    status: str
    dataset_id: int


class CleanupOperationItem(BaseModel):
    id: int
    dataset_id: int
    job_id: int | None
    operation_type: str
    status: str
    total_files: int
    deleted_count: int
    freed_bytes: int
    error_log: str | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    model_config = {"from_attributes": True}


class DeletionProgressResponse(BaseModel):
    status: str
    total_files: int
    deleted_count: int
    freed_bytes: int
    progress_percent: int


class LiveSourceItem(BaseModel):
    source_name: str
    enabled: bool
    last_check: datetime | None
    last_ingest: datetime | None
    next_check: datetime | None
    model_config = {"from_attributes": True}


class LiveStatusResponse(BaseModel):
    dataset_id: int
    enabled: bool
    status: str
    required_tiers: list[str]
    bbox_wkt: str
    total_size_bytes: int
    last_checked_at: datetime | None
    sources: list[LiveSourceItem]


class LiveToggleRequest(BaseModel):
    enabled: bool


class LiveToggleResponse(BaseModel):
    enabled: bool


class LiveClearResponse(BaseModel):
    status: str
    freed_bytes: int
    deleted_count: int


class LiveBackfillRequest(BaseModel):
    date_start: date
    date_end: date

    @model_validator(mode="after")
    def _validate_date_range(self) -> "LiveBackfillRequest":
        if self.date_end < self.date_start:
            raise ValueError("date_end must be >= date_start")
        return self


class LiveBackfillResponse(BaseModel):
    status: str
    job_id: int
    date_range: str


class LiveSceneItem(BaseModel):
    product_id: int
    scene_date: datetime
    tier: str
    size_mb: float


class RegionItem(BaseModel):
    region_id: int
    region_code: str
    name: str
    description: str | None
    bbox: list[float]
    area_km2: float | None


class RegionListResponse(BaseModel):
    items: list[RegionItem]