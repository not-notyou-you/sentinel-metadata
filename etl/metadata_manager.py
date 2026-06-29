# etl/metadata_manager.py
"""
Metadata management layer: insert and query operations for all pipeline entities.
Called by ETL modules after each processing stage completes.

Author : Julius Marselinus (BRONTO) - NIM 00000111989
Program: Sistem Informasi - Universitas Multimedia Nusantara
"""

from __future__ import annotations

import logging
import socket
from datetime import datetime, timedelta, timezone
from typing import Any, Union

from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from etl.database_client import (
    AlertEvent,
    AlertEventTypeEnum,
    AlertSeverityEnum,
    DataProduct,
    DatabaseClient,
    JobStatusEnum,
    OrbitDirectionEnum,
    ProcessingJob,
    ProcessingStage,
    ProductTierEnum,
    QualityMetric,
    SatelliteScene,
    StorageLocationEnum,
)

logger = logging.getLogger(__name__)


def _to_enum(enum_cls, value: Union[str, "Enum"], param_name: str = "value") -> "Enum":
    """
    Convert a string or existing enum member to the target enum type.

    Args:
        enum_cls   : The target Enum class.
        value      : String or existing member of enum_cls.
        param_name : Parameter name for error messages.

    Returns:
        An instance of enum_cls.

    Raises:
        ValueError : If the string is not a valid member name.
        TypeError  : If value is neither str nor enum_cls.
    """
    if isinstance(value, enum_cls):
        return value
    if isinstance(value, str):
        try:
            return enum_cls(value)
        except ValueError:
            valid = [e.value for e in enum_cls]
            raise ValueError(
                f"Invalid {param_name} '{value}'. Must be one of {valid}"
            ) from None
    raise TypeError(
        f"{param_name} must be a string or {enum_cls.__name__}, "
        f"got {type(value).__name__}"
    )


class MetadataManager:
    """
    High-level CRUD interface for all pipeline metadata operations.

    Each public method corresponds to a specific pipeline hook:
        Module 1 → insert_satellite_scene()
        Module 2 → insert_data_product() [tier=BRONZE]
        Module 3 → insert_data_product() [tier=SILVER]
        Module 4 → insert_data_product() [tier=GOLD] + insert_quality_metrics()
        Module 5 → insert_processing_job() / update_job_status()
        Module 6 → insert_quality_metrics() / insert_alert_event()

    Args:
        db: Initialized DatabaseClient instance
    """

    def __init__(self, db: DatabaseClient) -> None:
        self._db = db

    # ------------------------------------------------------------------
    # PROCESSING JOB OPERATIONS
    # ------------------------------------------------------------------

    def insert_processing_job(
        self,
        scene_id: int,
        stage_name: str,
        attempt_number: int = 1,
        parameters: dict | None = None,
    ) -> int:
        """
        Create a new processing job record (status=QUEUED).
        Called at the START of each ETL stage.

        Args:
            scene_id       : FK to satellite_scenes
            stage_name     : e.g. 'DOWNLOAD', 'CROP', 'LEE_FILTER'
            attempt_number : Retry attempt counter (default 1)
            parameters     : ETL parameters dict stored as JSONB

        Returns:
            job_id (int) of the newly created job

        Raises:
            ValueError : If stage_name not found in processing_stages
            SQLAlchemyError : On database error
        """
        with self._db.session() as sess:
            stage = sess.scalar(
                select(ProcessingStage).where(ProcessingStage.stage_name == stage_name)
            )
            if not stage:
                raise ValueError(
                    f"Unknown stage_name: '{stage_name}'. Check processing_stages table."
                )

            job = ProcessingJob(
                scene_id        = scene_id,
                stage_id        = stage.stage_id,
                attempt_number  = attempt_number,
                status          = JobStatusEnum.QUEUED,
                worker_hostname = socket.gethostname(),
                parameters_json = parameters or {},
            )
            sess.add(job)
            sess.flush()
            job_id = job.job_id

        logger.info(
            "[JOB] Created job_id=%d scene=%d stage=%s attempt=%d",
            job_id, scene_id, stage_name, attempt_number,
        )
        return job_id

    def start_job(self, job_id: int) -> None:
        """Mark job as RUNNING and record started_at timestamp."""
        with self._db.session() as sess:
            job = sess.get(ProcessingJob, job_id)
            if not job:
                raise ValueError(f"job_id={job_id} not found")
            job.status     = JobStatusEnum.RUNNING
            job.started_at = datetime.now(tz=timezone.utc)
        logger.info("[JOB] job_id=%d → RUNNING", job_id)

    def complete_job(
        self,
        job_id: int,
        status: Union[str, JobStatusEnum] = JobStatusEnum.SUCCESS,
        output_size_mb: float | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        """
        Mark job as SUCCESS or FAILED with completion timestamp.
        Called at the END of each ETL stage (success or failure).

        Args:
            job_id         : Processing job to update
            status         : SUCCESS or FAILED (or CANCELLED)
            output_size_mb : Size of produced output file
            error_code     : Structured error code if failed
            error_message  : Full traceback / error text if failed
        """
        status_enum = _to_enum(JobStatusEnum, status, "status")

        with self._db.session() as sess:
            job = sess.get(ProcessingJob, job_id)
            if not job:
                raise ValueError(f"job_id={job_id} not found")
            job.status        = status_enum
            job.completed_at  = datetime.now(tz=timezone.utc)
            job.output_size_mb = output_size_mb
            job.error_code    = error_code
            job.error_message = error_message

        logger.info("[JOB] job_id=%d → %s", job_id, status_enum.value)

    # ------------------------------------------------------------------
    # MODULE 1: SATELLITE SCENE REGISTRATION
    # ------------------------------------------------------------------

    def insert_satellite_scene(
        self,
        product_identifier: str,
        acquisition_datetime: datetime,
        region_id: int,
        bbox_wkt: str,
        orbit_direction: Union[str, OrbitDirectionEnum] = OrbitDirectionEnum.ASCENDING,
        polarization_vv: bool = True,
        polarization_vh: bool = True,
        orbit_number: int | None = None,
        relative_orbit: int | None = None,
        cloud_cover_percent: float | None = None,
        incidence_angle_near: float | None = None,
        incidence_angle_far: float | None = None,
        resolution_m: int = 10,
        raw_file_path: str | None = None,
        raw_file_size_mb: float | None = None,
        download_url: str | None = None,
        checksum_md5: str | None = None,
        instrument_mode: str = "IW",
    ) -> int:
        """
        Register a new Sentinel-1 scene after Module 1 (download/recovery).

        Args:
            product_identifier   : ESA Copernicus Hub unique product ID
            acquisition_datetime : UTC acquisition timestamp
            region_id            : FK to regions_of_interest
            bbox_wkt             : WKT polygon string (POLYGON((lon lat, ...)))
            orbit_direction      : OrbitDirectionEnum or 'ASCENDING' / 'DESCENDING'
            polarization_vv      : VV band available
            polarization_vh      : VH band available
            orbit_number         : Absolute orbit number
            relative_orbit       : Relative orbit number (1–175)
            cloud_cover_percent  : Optical cloud coverage estimate
            incidence_angle_near : Near-range incidence angle degrees
            incidence_angle_far  : Far-range incidence angle degrees
            resolution_m         : Ground resolution in meters
            raw_file_path        : Local file path of downloaded scene
            raw_file_size_mb     : File size in MB
            download_url         : Source download URL
            checksum_md5         : MD5 hash of raw file
            instrument_mode      : Acquisition mode (IW, EW, SM)

        Returns:
            scene_id (int) of the newly registered scene

        Raises:
            ValueError : If scene already exists (duplicate product_identifier)
        """
        orbit_enum = _to_enum(OrbitDirectionEnum, orbit_direction, "orbit_direction")

        with self._db.session() as sess:
            # Check for duplicate
            existing = sess.scalar(
                select(SatelliteScene.scene_id).where(
                    SatelliteScene.product_identifier == product_identifier
                )
            )
            if existing:
                logger.warning(
                    "[SCENE] Duplicate scene skipped: %s (scene_id=%d)",
                    product_identifier, existing,
                )
                return existing

            scene = SatelliteScene(
                product_identifier   = product_identifier,
                acquisition_datetime = acquisition_datetime,
                region_id            = region_id,
                bbox                 = f"SRID=4326;{bbox_wkt}",
                orbit_direction      = orbit_enum,
                polarization_vv      = polarization_vv,
                polarization_vh      = polarization_vh,
                orbit_number         = orbit_number,
                relative_orbit       = relative_orbit,
                cloud_cover_percent  = cloud_cover_percent,
                incidence_angle_near = incidence_angle_near,
                incidence_angle_far  = incidence_angle_far,
                resolution_m         = resolution_m,
                raw_file_path        = raw_file_path,
                raw_file_size_mb     = raw_file_size_mb,
                download_url         = download_url,
                checksum_md5         = checksum_md5,
                instrument_mode      = instrument_mode,
                is_available         = True,
            )
            sess.add(scene)
            sess.flush()
            scene_id = scene.scene_id

        logger.info(
            "[SCENE] Registered scene_id=%d pid=%s acq=%s",
            scene_id, product_identifier, acquisition_datetime.isoformat(),
        )
        return scene_id

    # ------------------------------------------------------------------
    # MODULE 2, 3, 4: DATA PRODUCT REGISTRATION
    # ------------------------------------------------------------------

    def insert_data_product(
        self,
        scene_id: int,
        job_id: int,
        product_tier: Union[str, ProductTierEnum],
        product_type: str,
        band_name: str,
        file_path: str,
        file_name: str,
        file_size_mb: float,
        data_hash_sha256: str,
        file_format: str = "TIFF",
        crs: str = "EPSG:4326",
        pixel_size_m: float | None = None,
        nodata_value: float | None = None,
        rows: int | None = None,
        cols: int | None = None,
        band_count: int = 1,
        storage_location: Union[str, StorageLocationEnum] = StorageLocationEnum.LOCAL,
    ) -> int:
        """
        Register an output product artifact (COG, filtered TIFF, cropped TIFF).
        Called after Module 2 (BRONZE), Module 3 (SILVER), Module 4 (GOLD).

        Args:
            scene_id         : Parent scene FK
            job_id           : Producing job FK
            product_tier     : ProductTierEnum or tier string
            product_type     : 'CROPPED_TIFF' | 'LEE_FILTERED' | 'COG' etc.
            band_name        : 'VV' | 'VH' | 'VV_VH'
            file_path        : Full file storage path
            file_name        : Filename only
            file_size_mb     : Output file size in MB
            data_hash_sha256 : SHA-256 hex digest of file content
            file_format      : 'TIFF' | 'COG' | 'NetCDF'
            crs              : Coordinate reference system string
            pixel_size_m     : Ground sampling distance in meters
            nodata_value     : NoData sentinel value
            rows             : Raster row count
            cols             : Raster column count
            band_count       : Number of bands in file
            storage_location : StorageLocationEnum or location string

        Returns:
            product_id (int) of the newly registered product
        """
        tier_enum = _to_enum(ProductTierEnum, product_tier, "product_tier")
        location_enum = _to_enum(StorageLocationEnum, storage_location, "storage_location")

        # Mark older products for same scene/band/tier as not-latest
        with self._db.session() as sess:
            sess.query(DataProduct).filter(
                and_(
                    DataProduct.scene_id     == scene_id,
                    DataProduct.band_name    == band_name,
                    DataProduct.product_tier == tier_enum,
                    DataProduct.is_latest    == True,
                )
            ).update({"is_latest": False})

            product = DataProduct(
                scene_id         = scene_id,
                job_id           = job_id,
                product_tier     = tier_enum,
                product_type     = product_type,
                band_name        = band_name,
                file_name        = file_name,
                file_path        = file_path,
                file_size_mb     = file_size_mb,
                data_hash_sha256 = data_hash_sha256,
                file_format      = file_format,
                crs              = crs,
                pixel_size_m     = pixel_size_m,
                nodata_value     = nodata_value,
                rows             = rows,
                cols             = cols,
                band_count       = band_count,
                storage_location = location_enum,
                is_valid         = True,
                is_latest        = True,
            )
            sess.add(product)
            sess.flush()
            product_id = product.product_id

        logger.info(
            "[PRODUCT] Registered product_id=%d scene=%d band=%s tier=%s file=%s",
            product_id, scene_id, band_name, tier_enum.value, file_name,
        )
        return product_id

    # ------------------------------------------------------------------
    # MODULE 6: QUALITY METRICS
    # ------------------------------------------------------------------

    def insert_quality_metrics(
        self,
        scene_id: int,
        product_id: int,
        band_name: str,
        total_pixels: int,
        valid_pixels: int,
        nodata_pixels: int,
        quality_score: float,
        backscatter_mean_db: float | None = None,
        backscatter_std_db: float | None = None,
        backscatter_min_db: float | None = None,
        backscatter_max_db: float | None = None,
        cloud_threshold_percent: float = 20.0,
        radiometric_consistency: bool | None = None,
        speckle_index: float | None = None,
        quality_flag: str = "UNCHECKED",
        notes: str | None = None,
    ) -> int:
        """
        Persist quality validation results after Module 6 analytics.

        Args:
            scene_id                : Parent scene FK
            product_id              : Assessed product FK
            band_name               : 'VV' or 'VH'
            total_pixels            : Total raster pixel count
            valid_pixels            : Non-nodata pixel count
            nodata_pixels           : NoData pixel count
            quality_score           : Composite score 0–100
            backscatter_mean_db     : Mean backscatter (dB)
            backscatter_std_db      : Std dev backscatter (dB)
            backscatter_min_db      : Min backscatter (dB)
            backscatter_max_db      : Max backscatter (dB)
            cloud_threshold_percent : Cloud mask threshold used
            radiometric_consistency : Passed radiometric check?
            speckle_index           : Coefficient of variation (lower=better)
            quality_flag            : 'PASS' | 'FAIL' | 'WARNING' | 'UNCHECKED'
            notes                   : Optional analyst annotation

        Returns:
            metric_id (int)
        """
        with self._db.session() as sess:
            metric = QualityMetric(
                scene_id                = scene_id,
                product_id              = product_id,
                band_name               = band_name,
                total_pixels            = total_pixels,
                valid_pixels            = valid_pixels,
                nodata_pixels           = nodata_pixels,
                quality_score           = round(quality_score, 2),
                backscatter_mean_db     = backscatter_mean_db,
                backscatter_std_db      = backscatter_std_db,
                backscatter_min_db      = backscatter_min_db,
                backscatter_max_db      = backscatter_max_db,
                cloud_threshold_percent = cloud_threshold_percent,
                radiometric_consistency = radiometric_consistency,
                speckle_index           = speckle_index,
                quality_flag            = quality_flag,
                notes                   = notes,
            )
            sess.add(metric)
            sess.flush()
            metric_id = metric.metric_id

        logger.info(
            "[QUALITY] metric_id=%d scene=%d band=%s score=%.2f flag=%s",
            metric_id, scene_id, band_name, quality_score, quality_flag,
        )

        # Auto-trigger alert on quality failure
        if quality_flag == "FAIL":
            self.insert_alert_event(
                event_type = AlertEventTypeEnum.QUALITY_WARNING,
                severity   = AlertSeverityEnum.WARNING,
                title      = f"Quality FAIL: scene={scene_id} band={band_name}",
                message    = (
                    f"Quality score {quality_score:.1f}/100 below threshold. "
                    f"NoData={nodata_pixels}/{total_pixels} pixels."
                ),
                scene_id   = scene_id,
                metadata   = {
                    "quality_score": quality_score,
                    "band": band_name,
                    "product_id": product_id,
                },
            )

        return metric_id

    # ------------------------------------------------------------------
    # ALERT EVENTS
    # ------------------------------------------------------------------

    def insert_alert_event(
        self,
        event_type: Union[str, AlertEventTypeEnum],
        title: str,
        message: str,
        severity: Union[str, AlertSeverityEnum] = AlertSeverityEnum.INFO,
        scene_id: int | None = None,
        job_id: int | None = None,
        product_id: int | None = None,
        metadata: dict | None = None,
    ) -> int:
        """
        Create a monitoring alert event.

        Args:
            event_type  : AlertEventTypeEnum or event type string
            title       : Short alert title (shown in dashboards)
            message     : Full alert description
            severity    : AlertSeverityEnum or severity string
            scene_id    : Related scene (optional)
            job_id      : Related job (optional)
            product_id  : Related product (optional)
            metadata    : Flexible context dict stored as JSONB

        Returns:
            alert_id (int)
        """
        type_enum = _to_enum(AlertEventTypeEnum, event_type, "event_type")
        severity_enum = _to_enum(AlertSeverityEnum, severity, "severity")

        with self._db.session() as sess:
            alert = AlertEvent(
                event_type    = type_enum,
                severity      = severity_enum,
                scene_id      = scene_id,
                job_id        = job_id,
                product_id    = product_id,
                title         = title,
                message       = message,
                metadata_json = metadata or {},
                is_resolved   = False,
            )
            sess.add(alert)
            sess.flush()
            alert_id = alert.alert_id

        log_level = logging.WARNING if severity_enum != AlertSeverityEnum.INFO else logging.INFO
        logger.log(
            log_level,
            "[ALERT] alert_id=%d [%s] %s: %s",
            alert_id, severity_enum.value, title, message,
        )
        return alert_id

    # ------------------------------------------------------------------
    # QUERY OPERATIONS
    # ------------------------------------------------------------------

    def query_latest_scenes(
        self,
        n_days: int = 30,
        region_id: int | None = None,
        only_unprocessed: bool = False,
    ) -> list[dict]:
        """
        Retrieve latest Sentinel-1 scenes within n_days.

        Args:
            n_days           : Look-back window in days (default 30)
            region_id        : Filter by specific AOI (optional)
            only_unprocessed : Return only scenes with no GOLD product yet

        Returns:
            List of scene dicts with keys:
                scene_id, product_identifier, acquisition_datetime,
                orbit_direction, region_name, has_gold_product
        """
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=n_days)

        with self._db.session() as sess:
            stmt = (
                select(
                    SatelliteScene.scene_id,
                    SatelliteScene.product_identifier,
                    SatelliteScene.acquisition_datetime,
                    SatelliteScene.orbit_direction,
                    SatelliteScene.polarization_vv,
                    SatelliteScene.polarization_vh,
                )
                .where(
                    and_(
                        SatelliteScene.acquisition_datetime >= cutoff,
                        SatelliteScene.is_available == True,
                    )
                )
                .order_by(SatelliteScene.acquisition_datetime.desc())
            )

            if region_id:
                stmt = stmt.where(SatelliteScene.region_id == region_id)

            rows = sess.execute(stmt).fetchall()

            results = []
            for row in rows:
                has_gold = False
                if only_unprocessed or True:  # always compute for filtering
                    has_gold = sess.scalar(
                        select(func.count(DataProduct.product_id)).where(
                            and_(
                                DataProduct.scene_id     == row.scene_id,
                                DataProduct.product_tier == ProductTierEnum.GOLD,
                                DataProduct.is_latest    == True,
                                DataProduct.is_valid     == True,
                            )
                        )
                    ) > 0

                if only_unprocessed and has_gold:
                    continue

                results.append({
                    "scene_id":             row.scene_id,
                    "product_identifier":   row.product_identifier,
                    "acquisition_datetime": row.acquisition_datetime.isoformat(),
                    "orbit_direction":      row.orbit_direction.value if row.orbit_direction else None,
                    "polarization_vv":      row.polarization_vv,
                    "polarization_vh":      row.polarization_vh,
                    "has_gold_product":     has_gold,
                })

        logger.info(
            "[QUERY] latest_scenes n_days=%d region=%s results=%d",
            n_days, region_id, len(results),
        )
        return results

    def get_scene_by_id(self, scene_id: int) -> dict | None:
        """
        Retrieve full scene metadata by scene_id.

        Returns:
            dict of scene attributes or None if not found
        """
        with self._db.session() as sess:
            scene = sess.get(SatelliteScene, scene_id)
            if not scene:
                return None
            return {
                "scene_id":             scene.scene_id,
                "scene_uuid":           str(scene.scene_uuid),
                "product_identifier":   scene.product_identifier,
                "platform":             scene.platform,
                "instrument_mode":      scene.instrument_mode,
                "polarization_vv":      scene.polarization_vv,
                "polarization_vh":      scene.polarization_vh,
                "acquisition_datetime": scene.acquisition_datetime.isoformat(),
                "orbit_direction":      scene.orbit_direction.value,
                "orbit_number":         scene.orbit_number,
                "relative_orbit":       scene.relative_orbit,
                "cloud_cover_percent":  float(scene.cloud_cover_percent) if scene.cloud_cover_percent else None,
                "resolution_m":         scene.resolution_m,
                "region_id":            scene.region_id,
                "raw_file_path":        scene.raw_file_path,
                "raw_file_size_mb":     float(scene.raw_file_size_mb) if scene.raw_file_size_mb else None,
                "is_available":         scene.is_available,
                "created_at":           scene.created_at.isoformat(),
            }

    def get_quality_by_scene(self, scene_id: int) -> list[dict]:
        """
        Retrieve all quality metrics for a given scene.

        Returns:
            List of metric dicts (one per band)
        """
        with self._db.session() as sess:
            metrics = sess.scalars(
                select(QualityMetric).where(QualityMetric.scene_id == scene_id)
            ).all()

            return [
                {
                    "metric_id":               m.metric_id,
                    "band_name":               m.band_name,
                    "quality_score":           float(m.quality_score),
                    "quality_flag":            m.quality_flag,
                    "total_pixels":            m.total_pixels,
                    "valid_pixels":            m.valid_pixels,
                    "nodata_pixels":           m.nodata_pixels,
                    "backscatter_mean_db":     float(m.backscatter_mean_db) if m.backscatter_mean_db else None,
                    "backscatter_std_db":      float(m.backscatter_std_db)  if m.backscatter_std_db  else None,
                    "radiometric_consistency": m.radiometric_consistency,
                    "speckle_index":           float(m.speckle_index) if m.speckle_index else None,
                    "assessed_at":             m.assessed_at.isoformat(),
                }
                for m in metrics
            ]

    def get_products_by_scene(
        self,
        scene_id: int,
        tier: Union[str, ProductTierEnum, None] = None,
        latest_only: bool = True,
    ) -> list[dict]:
        """
        Retrieve data products for a scene, optionally filtered by tier.

        Args:
            scene_id    : Parent scene
            tier        : ProductTierEnum or tier string (RAW/BRONZE/SILVER/GOLD)
            latest_only : Return only is_latest=TRUE products

        Returns:
            List of product dicts
        """
        tier_enum = _to_enum(ProductTierEnum, tier, "tier") if tier is not None else None

        with self._db.session() as sess:
            stmt = select(DataProduct).where(DataProduct.scene_id == scene_id)

            if tier_enum:
                stmt = stmt.where(DataProduct.product_tier == tier_enum)
            if latest_only:
                stmt = stmt.where(DataProduct.is_latest == True)

            products = sess.scalars(stmt).all()

            return [
                {
                    "product_id":       p.product_id,
                    "product_uuid":     str(p.product_uuid),
                    "product_tier":     p.product_tier.value,
                    "product_type":     p.product_type,
                    "band_name":        p.band_name,
                    "file_path":        p.file_path,
                    "file_name":        p.file_name,
                    "file_size_mb":     float(p.file_size_mb),
                    "file_format":      p.file_format,
                    "data_hash_sha256": p.data_hash_sha256,
                    "crs":              p.crs,
                    "rows":             p.rows,
                    "cols":             p.cols,
                    "is_valid":         p.is_valid,
                    "is_latest":        p.is_latest,
                    "created_at":       p.created_at.isoformat(),
                }
                for p in products
            ]

    def get_pipeline_status(self, scene_id: int) -> list[dict]:
        """
        Return per-stage job execution status for a scene.

        Returns:
            List of job status dicts ordered by stage_order
        """
        with self._db.session() as sess:
            jobs = sess.scalars(
                select(ProcessingJob)
                .join(ProcessingStage)
                .where(ProcessingJob.scene_id == scene_id)
                .order_by(ProcessingStage.stage_order)
            ).all()

            return [
                {
                    "job_id":         j.job_id,
                    "stage_name":     j.stage.stage_name,
                    "stage_order":    j.stage.stage_order,
                    "attempt_number": j.attempt_number,
                    "status":         j.status.value,
                    "queued_at":      j.queued_at.isoformat(),
                    "started_at":     j.started_at.isoformat() if j.started_at else None,
                    "completed_at":   j.completed_at.isoformat() if j.completed_at else None,
                    "error_message":  j.error_message,
                }
                for j in jobs
            ]