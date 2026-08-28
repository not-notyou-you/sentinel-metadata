# etl/module5_orchestrator.py
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import rasterio
from sqlalchemy import func, or_, select

from etl.database_client import (
    AlertEventTypeEnum,
    AlertSeverityEnum,
    DatabaseClient,
    DataProduct,
    Dataset,
    DatasetScene,
    JobStatusEnum,
    ProcessingJob,
    ProcessingStage,
    ProductTierEnum,
    RegionOfInterest,
)
from etl.lineage_tracker import LineageTracker
from etl.metadata_manager import MetadataManager

logger = logging.getLogger(__name__)


@dataclass
class SceneContext:
    scene_id: int
    product_identifier: str
    region_id: int
    raw_file_path: str = ""
    raw_vv_path: str = ""
    raw_vh_path: str = ""

    job_ids: dict[str, int] = field(default_factory=dict)
    product_ids: dict[str, int] = field(default_factory=dict)

    calib_vv_path: str = ""
    calib_vh_path: str = ""
    crop_vv_path: str = ""
    crop_vh_path: str = ""
    lee_vv_path: str = ""
    lee_vh_path: str = ""
    cog_vv_path: str = ""
    cog_vh_path: str = ""

    completed_stages: list[str] = field(default_factory=list)
    failed_stage: str | None = None
    error_message: str | None = None


class PipelineOrchestrator:
    STAGE_ORDER = [
        "DOWNLOAD",
        "CROP",
        "LEE_FILTER",
        "COG_EXPORT",
        "QUALITY_ANALYTICS",
    ]

    def __init__(self, db: DatabaseClient, output_dir: str = "processed") -> None:
        self._db = db
        self._meta = MetadataManager(db)
        self._lineage = LineageTracker(db)
        self._out_dir = Path(output_dir)
        self._out_dir.mkdir(parents=True, exist_ok=True)

    def get_completed_stages(self, scene_id: int) -> list[str]:
        with self._db.session() as sess:
            rows = sess.execute(
                select(ProcessingStage.stage_name)
                .join(ProcessingJob, ProcessingJob.stage_id == ProcessingStage.stage_id)
                .where(
                    ProcessingJob.scene_id == scene_id,
                    ProcessingJob.status == JobStatusEnum.SUCCESS,
                )
                .order_by(ProcessingStage.stage_order)
            ).scalars().all()
        return list(rows)

    def get_last_successful_product(self, scene_id: int, band: str, tier: str) -> DataProduct | None:
        with self._db.session() as sess:
            return sess.scalar(
                select(DataProduct).where(
                    DataProduct.scene_id == scene_id,
                    DataProduct.band_name == band,
                    DataProduct.product_tier == ProductTierEnum(tier),
                    DataProduct.is_latest == True,
                    DataProduct.is_valid == True,
                )
            )

    def is_stage_complete(self, scene_id: int, stage_name: str) -> bool:
        return stage_name in self.get_completed_stages(scene_id)

    def _last_attempt_number(self, scene_id: int, stage_name: str) -> int:
        with self._db.session() as sess:
            stage_id = sess.scalar(
                select(ProcessingStage.stage_id).where(ProcessingStage.stage_name == stage_name)
            )
            if stage_id is None:
                return 0
            last = sess.scalar(
                select(func.max(ProcessingJob.attempt_number)).where(
                    ProcessingJob.scene_id == scene_id,
                    ProcessingJob.stage_id == stage_id,
                )
            )
            return last or 0

    def _raster_dims(self, path: str) -> tuple[int | None, int | None]:
        try:
            with rasterio.open(path) as src:
                return src.height, src.width
        except Exception:
            return None, None

    def _register_product(
        self,
        ctx: SceneContext,
        job_id: int,
        tier: str,
        product_type: str,
        band: str,
        path: str,
        file_format: str = "TIFF",
    ) -> int:
        file_hash = self._lineage.compute_sha256(path)
        size_mb = round(Path(path).stat().st_size / (1024 ** 2), 3)
        rows, cols = self._raster_dims(path)
        return self._meta.insert_data_product(
            scene_id=ctx.scene_id,
            job_id=job_id,
            product_tier=tier,
            product_type=product_type,
            band_name=band,
            file_path=path,
            file_name=Path(path).name,
            file_size_mb=size_mb,
            data_hash_sha256=file_hash,
            file_format=file_format,
            rows=rows,
            cols=cols,
        )

    def _run_stage(
        self,
        ctx: SceneContext,
        stage_name: str,
        fn: Callable[[SceneContext], None],
        params: dict | None = None,
        max_retries: int = 2,
    ) -> bool:
        if self.is_stage_complete(ctx.scene_id, stage_name):
            logger.info("[ORCH] stage=%s scene=%d already complete, skipping", stage_name, ctx.scene_id)
            ctx.completed_stages.append(stage_name)
            return True

        attempt = self._last_attempt_number(ctx.scene_id, stage_name)
        start_attempt = attempt
        delay = 5.0
        while attempt <= start_attempt + max_retries:
            attempt += 1
            job_id = self._meta.insert_processing_job(
                scene_id=ctx.scene_id,
                stage_name=stage_name,
                attempt_number=attempt,
                parameters=params or {},
            )
            ctx.job_ids[stage_name] = job_id
            self._meta.start_job(job_id)
            try:
                logger.info("[ORCH] stage=%s attempt=%d (call attempt %d/%d) scene=%d",
                            stage_name, attempt, attempt - start_attempt, max_retries + 1, ctx.scene_id)
                fn(ctx)
                self._meta.complete_job(job_id, status=JobStatusEnum.SUCCESS)
                ctx.completed_stages.append(stage_name)
                logger.info("[ORCH] stage=%s complete scene=%d", stage_name, ctx.scene_id)
                return True
            except Exception as exc:
                err_msg = f"{type(exc).__name__}: {exc}"
                logger.error("[ORCH] stage=%s attempt=%d failed: %s", stage_name, attempt, err_msg)
                if attempt > start_attempt + max_retries:
                    self._meta.complete_job(
                        job_id,
                        status=JobStatusEnum.FAILED,
                        error_code="STAGE_FAILED",
                        error_message=err_msg,
                    )
                    ctx.failed_stage = stage_name
                    ctx.error_message = err_msg
                    self._meta.insert_alert_event(
                        event_type=AlertEventTypeEnum.PIPELINE_ERROR,
                        severity=AlertSeverityEnum.CRITICAL,
                        title=f"Pipeline failed at {stage_name}",
                        message=err_msg,
                        scene_id=ctx.scene_id,
                        job_id=job_id,
                        metadata={"stage": stage_name, "attempt": attempt},
                    )
                    return False
                self._meta.complete_job(
                    job_id,
                    status=JobStatusEnum.FAILED,
                    error_code="RETRYING",
                    error_message=err_msg,
                )
                time.sleep(delay)
                delay *= 2
        return False

    def _stage_download(self, ctx: SceneContext) -> None:
        if not ctx.raw_file_path:
            raise ValueError("raw_file_path (SAFE/zip) must be set before running the pipeline")
        if not Path(ctx.raw_file_path).exists():
            raise FileNotFoundError(
                f"Raw SAFE/zip not found: {ctx.raw_file_path}. "
                "module1_download must run with keep_raw=True so this file survives for calibration."
            )
        if not ctx.raw_vv_path or not Path(ctx.raw_vv_path).exists():
            raise FileNotFoundError(f"Raw VV band not found: {ctx.raw_vv_path}")
        if not ctx.raw_vh_path or not Path(ctx.raw_vh_path).exists():
            raise FileNotFoundError(f"Raw VH band not found: {ctx.raw_vh_path}")

    def _stage_crop(self, ctx: SceneContext) -> None:
        from etl.module1b_calibrate import run as calibrate_run
        from etl.module2_crop import run as crop_run

        calib_dir = self._out_dir / "calibrated" / str(ctx.scene_id)
        calib_dir.mkdir(parents=True, exist_ok=True)
        ctx.calib_vv_path, ctx.calib_vh_path = calibrate_run(
            ctx.raw_file_path, ctx.raw_vv_path, ctx.raw_vh_path, str(calib_dir)
        )

        base = self._out_dir / "bronze" / str(ctx.scene_id)
        base.mkdir(parents=True, exist_ok=True)
        ctx.crop_vv_path, ctx.crop_vh_path = crop_run(ctx.calib_vv_path, ctx.calib_vh_path, str(base))

        for stale_path in (ctx.calib_vv_path, ctx.calib_vh_path):
            Path(stale_path).unlink(missing_ok=True)
        logger.info("[ORCH] calibrated intermediates removed for scene=%d", ctx.scene_id)

        for band, path in [("VV", ctx.crop_vv_path), ("VH", ctx.crop_vh_path)]:
            pid = self._register_product(ctx, ctx.job_ids["CROP"], "BRONZE", "CROPPED_TIFF", band, path)
            ctx.product_ids[f"BRONZE_{band}"] = pid

    def _stage_lee_filter(self, ctx: SceneContext) -> None:
        from etl.module3_lee_filter import run as lee_run

        base = self._out_dir / "silver" / str(ctx.scene_id)
        base.mkdir(parents=True, exist_ok=True)
        window_size = 7
        looks = 1
        ctx.lee_vv_path, ctx.lee_vh_path = lee_run(
            ctx.crop_vv_path, ctx.crop_vh_path, str(base), window_size=window_size, looks=looks,
        )

        for band, path in [("VV", ctx.lee_vv_path), ("VH", ctx.lee_vh_path)]:
            pid = self._register_product(ctx, ctx.job_ids["LEE_FILTER"], "SILVER", "LEE_FILTERED", band, path)
            ctx.product_ids[f"SILVER_{band}"] = pid
            self._lineage.record_transformation(
                parent_product_id=ctx.product_ids[f"BRONZE_{band}"],
                child_product_id=pid,
                transformation_type="LEE_FILTER",
                job_id=ctx.job_ids["LEE_FILTER"],
                params={"window_size": window_size, "looks": looks},
            )

    def _stage_cog_export(self, ctx: SceneContext) -> None:
        from etl.module4_cog_export import run as cog_run

        base = self._out_dir / "gold" / str(ctx.scene_id)
        base.mkdir(parents=True, exist_ok=True)
        compression = "LZW"
        blocksize = 512
        ctx.cog_vv_path, ctx.cog_vh_path = cog_run(
            ctx.lee_vv_path, ctx.lee_vh_path, str(base), compression=compression, blocksize=blocksize,
        )

        for band, path in [("VV", ctx.cog_vv_path), ("VH", ctx.cog_vh_path)]:
            pid = self._register_product(
                ctx, ctx.job_ids["COG_EXPORT"], "GOLD", "COG", band, path, file_format="COG",
            )
            ctx.product_ids[f"GOLD_{band}"] = pid
            self._lineage.record_transformation(
                parent_product_id=ctx.product_ids[f"SILVER_{band}"],
                child_product_id=pid,
                transformation_type="COG_EXPORT",
                job_id=ctx.job_ids["COG_EXPORT"],
                params={"compression": compression, "blocksize": blocksize},
            )

    def _stage_quality_analytics(self, ctx: SceneContext) -> None:
        from etl.module6_analytics import compute_band_metrics

        for band, path in [("VV", ctx.cog_vv_path), ("VH", ctx.cog_vh_path)]:
            pid = ctx.product_ids.get(f"GOLD_{band}")
            if not pid:
                continue
            m = compute_band_metrics(path, band)
            self._meta.insert_quality_metrics(
                scene_id=ctx.scene_id,
                product_id=pid,
                band_name=band,
                total_pixels=m.total_pixels,
                valid_pixels=m.valid_pixels,
                nodata_pixels=m.nodata_pixels,
                quality_score=m.quality_score,
                backscatter_mean_db=m.backscatter_mean_db,
                backscatter_std_db=m.backscatter_std_db,
                backscatter_min_db=m.backscatter_min_db,
                backscatter_max_db=m.backscatter_max_db,
                radiometric_consistency=m.radiometric_consistency,
                speckle_index=m.speckle_index,
                quality_flag=m.quality_flag,
            )

    def run(self, ctx: SceneContext) -> SceneContext:
        logger.info("[ORCH] pipeline start scene=%d pid=%s", ctx.scene_id, ctx.product_identifier)
        start = time.time()

        stages = [
            ("DOWNLOAD", self._stage_download, {}),
            ("CROP", self._stage_crop, {"bbox": "JABODETABEK"}),
            ("LEE_FILTER", self._stage_lee_filter, {"window_size": 7}),
            ("COG_EXPORT", self._stage_cog_export, {"compression": "LZW"}),
            ("QUALITY_ANALYTICS", self._stage_quality_analytics, {}),
        ]

        for stage_name, fn, params in stages:
            success = self._run_stage(ctx, stage_name, fn, params)
            if not success:
                logger.error("[ORCH] pipeline aborted stage=%s scene=%d", stage_name, ctx.scene_id)
                return ctx

        elapsed = time.time() - start
        logger.info("[ORCH] pipeline complete scene=%d elapsed=%.1fs", ctx.scene_id, elapsed)
        return ctx

    def run_batch(self, scene_contexts: list[SceneContext], max_workers: int = 3) -> dict:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        total = len(scene_contexts)
        success = 0
        failed = []

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(self.run, ctx): ctx for ctx in scene_contexts}
            for i, future in enumerate(as_completed(futures), 1):
                ctx = futures[future]
                result = future.result()
                logger.info("[ORCH] batch progress %d/%d scene=%d", i, total, ctx.scene_id)
                if result.failed_stage:
                    failed.append(ctx.scene_id)
                else:
                    success += 1

        summary = {
            "total": total,
            "success": success,
            "failed": len(failed),
            "failed_scene_ids": failed,
        }
        logger.info("[ORCH] batch complete: %s", summary)
        return summary


# ---------------------------------------------------------------------------
# DATASET JOBS (on-demand builds queued via /api/datasets)
# ---------------------------------------------------------------------------

def _resolve_region(session, location: str) -> tuple[int, str] | None:
    """Look up a regions_of_interest row by name or region_code (case-insensitive).

    Returns (region_id, bbox_wkt) or None if the dataset's free-text `location`
    doesn't match a known region.
    """
    row = session.execute(
        select(RegionOfInterest.region_id, func.ST_AsText(RegionOfInterest.bbox))
        .where(
            or_(
                func.lower(RegionOfInterest.name) == location.lower(),
                func.lower(RegionOfInterest.region_code) == location.lower(),
            )
        )
    ).first()
    return tuple(row) if row else None


def _delete_tier_files(ctx: SceneContext, tier: str) -> float:
    """Delete the on-disk band files for one intermediate tier, freeing space
    once a dataset's required_tiers no longer needs them. DB rows for those
    products are left in place (lineage/history stays intact)."""
    paths = {
        "BRONZE": (ctx.crop_vv_path, ctx.crop_vh_path),
        "SILVER": (ctx.lee_vv_path, ctx.lee_vh_path),
    }.get(tier, ())

    freed_mb = 0.0
    for path in paths:
        if path and Path(path).exists():
            freed_mb += Path(path).stat().st_size / (1024 ** 2)
            Path(path).unlink(missing_ok=True)

    if freed_mb:
        logger.info("[ORCH] tier=%s intermediate files removed, freed %.1f MB", tier, freed_mb)
    return freed_mb


def run_dataset_job(dataset_id: int, session) -> dict:
    """
    Run the full pipeline (DOWNLOAD → CROP → LEE_FILTER → COG_EXPORT →
    QUALITY_ANALYTICS) over every scene found for a queued Dataset's
    location/date range, applying tier-based cleanup once GOLD exists.

    `session` is used for Dataset/DatasetScene bookkeeping only. Stage
    execution (processing_jobs, data_products, quality_metrics) runs through
    a PipelineOrchestrator on its own DatabaseClient, same as run_pipeline_once
    in etl/scheduler.py.
    """
    import etl.module1_download as m1

    dataset = session.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        return {"error": "Dataset not found"}

    region = _resolve_region(session, dataset.location)
    if region is None:
        dataset.status = "FAILED"
        session.commit()
        return {"error": f"Unknown location: {dataset.location}"}
    region_id, bbox_wkt = region

    required_tiers = set(dataset.required_tiers or [])
    date_from = datetime.combine(dataset.date_start, datetime.min.time(), tzinfo=timezone.utc)
    date_to = datetime.combine(dataset.date_end, datetime.min.time(), tzinfo=timezone.utc)

    db = DatabaseClient.from_env()
    orch = PipelineOrchestrator(db)

    stage_plan = [
        ("DOWNLOAD", orch._stage_download, {}),
        ("CROP", orch._stage_crop, {"bbox": dataset.location}),
        ("LEE_FILTER", orch._stage_lee_filter, {"window_size": 7}),
        ("COG_EXPORT", orch._stage_cog_export, {"compression": "LZW"}),
        ("QUALITY_ANALYTICS", orch._stage_quality_analytics, {}),
    ]

    total_scenes = 0
    completed_scenes = 0
    failed_scenes = 0

    dataset.status = "PROCESSING"
    session.commit()

    try:
        scenes = m1.discover_scenes(bbox_wkt=bbox_wkt, date_from=date_from, date_to=date_to)
        logger.info("[ORCH] dataset=%d found %d scene(s) for %s", dataset_id, len(scenes), dataset.location)

        for scene_meta in scenes:
            total_scenes += 1
            # scene_id is unknown until download+registration succeed; 0 is a
            # placeholder (dataset_scenes.scene_id is NOT NULL) for a scene that
            # failed before it could be registered.
            scene_id = 0
            stage_name = "DOWNLOAD"
            dataset_scene = DatasetScene(
                dataset_id=dataset_id,
                scene_id=scene_id,
                stage_name=stage_name,
                status="RUNNING",
                progress_percent=0,
            )
            session.add(dataset_scene)
            session.commit()

            try:
                result = m1.download_scene(scene_meta, output_dir="recovered_temp", keep_raw=True)

                scene_id = orch._meta.insert_satellite_scene(
                    product_identifier=result.product_identifier,
                    acquisition_datetime=result.acquisition_datetime,
                    region_id=region_id,
                    bbox_wkt=bbox_wkt,
                    orbit_direction=result.orbit_direction,
                    orbit_number=result.orbit_number,
                    relative_orbit=result.relative_orbit,
                    cloud_cover_percent=result.cloud_cover,
                    raw_file_path=result.zip_path if result.kept_raw else None,
                    raw_file_size_mb=result.file_size_mb,
                    download_url=result.download_url,
                    checksum_md5=result.checksum_md5,
                )
                dataset_scene.scene_id = scene_id

                ctx = SceneContext(
                    scene_id=scene_id,
                    product_identifier=result.product_identifier,
                    region_id=region_id,
                    raw_file_path=result.zip_path,
                    raw_vv_path=result.vv_tif_path,
                    raw_vh_path=result.vh_tif_path,
                )

                for i, (stage_name, fn, params) in enumerate(stage_plan, start=1):
                    dataset_scene.stage_name = stage_name
                    session.commit()

                    if not orch._run_stage(ctx, stage_name, fn, params):
                        raise RuntimeError(ctx.error_message or f"{stage_name} failed")

                    if stage_name == "COG_EXPORT":
                        for tier in ("BRONZE", "SILVER"):
                            if tier not in required_tiers:
                                _delete_tier_files(ctx, tier)

                    dataset_scene.progress_percent = int(100 * i / len(stage_plan))
                    session.commit()

                dataset_scene.status = "COMPLETED"
                completed_scenes += 1

            except Exception as exc:
                dataset_scene.status = "FAILED"
                dataset_scene.error_message = str(exc)
                failed_scenes += 1
                logger.error(
                    "[ORCH] dataset=%d scene=%s failed at stage=%s: %s",
                    dataset_id, scene_id or "?", stage_name, exc, exc_info=True,
                )

            session.commit()

    finally:
        db.dispose()

    dataset.status = "COMPLETED" if failed_scenes == 0 else "COMPLETED_WITH_ERRORS"
    session.commit()

    logger.info(
        "[ORCH] dataset=%d job complete: total=%d completed=%d failed=%d",
        dataset_id, total_scenes, completed_scenes, failed_scenes,
    )
    return {"total": total_scenes, "completed": completed_scenes, "failed": failed_scenes}