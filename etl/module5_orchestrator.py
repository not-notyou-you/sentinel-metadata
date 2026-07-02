# etl/module5_orchestrator.py
"""
Module 5: Pipeline Orchestrator with PostgreSQL-backed checkpoint system.
Coordinates Module 1–6, handles retries, and resumes from last checkpoint.

Author : Julius Marselinus (BRONTO) - NIM 00000111989
Program: Sistem Informasi - Universitas Multimedia Nusantara
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from sqlalchemy import select

from etl.database_client import (
    DatabaseClient,
    DataProduct,
    JobStatusEnum,
    ProcessingJob,
    ProcessingStage,
    ProductTierEnum,
)
from etl.lineage_tracker import LineageTracker
from etl.metadata_manager import MetadataManager

logger = logging.getLogger(__name__)


@dataclass
class SceneContext:
    """
    Carries all state for a single scene through the pipeline.
    Populated progressively as each module completes.
    """
    # Input
    scene_id:            int
    product_identifier:  str
    region_id:           int
    raw_file_path:       str = ""
    raw_vv_path:         str = ""
    raw_vh_path:         str = ""

    # Job IDs per stage (filled as stages complete)
    job_ids: dict[str, int] = field(default_factory=dict)

    # Product IDs per stage
    product_ids: dict[str, int] = field(default_factory=dict)

    # Derived outputs
    crop_vv_path:   str = ""
    crop_vh_path:   str = ""
    lee_vv_path:    str = ""
    lee_vh_path:    str = ""
    cog_vv_path:    str = ""
    cog_vh_path:    str = ""

    # Pipeline state
    completed_stages: list[str] = field(default_factory=list)
    failed_stage:     str | None = None
    error_message:    str | None = None


class PipelineOrchestrator:
    """
    Orchestrates the full Sentinel-1 ETL pipeline (Modules 1–6).

    Features:
    - PostgreSQL-backed checkpointing: resume from last successful stage
    - Per-stage retry with exponential backoff
    - Lineage tracking across all stages
    - Alert events on failure

    Stage Order:
        1. DOWNLOAD          → register scene in DB
        2. CROP              → spatial subset to Jabodetabek
        3. LEE_FILTER        → speckle reduction
        4. COG_EXPORT        → Cloud-Optimized GeoTIFF
        5. (this module)     → orchestration / checkpointing
        6. QUALITY_ANALYTICS → metrics computation

    Args:
        db         : DatabaseClient instance
        output_dir : Base directory for processed outputs
    """

    STAGE_ORDER = [
        "DOWNLOAD",
        "CROP",
        "LEE_FILTER",
        "COG_EXPORT",
        "QUALITY_ANALYTICS",
    ]

    def __init__(
        self,
        db: DatabaseClient,
        output_dir: str = "processed",
    ) -> None:
        self._db       = db
        self._meta     = MetadataManager(db)
        self._lineage  = LineageTracker(db)
        self._out_dir  = Path(output_dir)
        self._out_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # CHECKPOINT QUERIES (PostgreSQL-backed)
    # ------------------------------------------------------------------

    def get_completed_stages(self, scene_id: int) -> list[str]:
        """
        Query DB for successfully completed stages for a scene.
        Used to determine resume point after failure or restart.

        Returns:
            List of stage_name strings that have status=SUCCESS
        """
        with self._db.session() as sess:
            rows = sess.execute(
                select(ProcessingStage.stage_name)
                .join(ProcessingJob, ProcessingJob.stage_id == ProcessingStage.stage_id)
                .where(
                    ProcessingJob.scene_id == scene_id,
                    ProcessingJob.status   == JobStatusEnum.SUCCESS,
                )
                .order_by(ProcessingStage.stage_order)
            ).scalars().all()
        return list(rows)

    def get_last_successful_product(
        self, scene_id: int, band: str, tier: str
    ) -> DataProduct | None:
        """
        Retrieve the latest valid product for a scene/band/tier.
        Used for lineage linking when resuming mid-pipeline.
        """
        with self._db.session() as sess:
            return sess.scalar(
                select(DataProduct).where(
                    DataProduct.scene_id     == scene_id,
                    DataProduct.band_name    == band,
                    DataProduct.product_tier == ProductTierEnum(tier),
                    DataProduct.is_latest    == True,
                    DataProduct.is_valid     == True,
                )
            )

    def is_stage_complete(self, scene_id: int, stage_name: str) -> bool:
        """Check if a specific stage has already succeeded for this scene."""
        return stage_name in self.get_completed_stages(scene_id)

    # ------------------------------------------------------------------
    # STAGE EXECUTION WRAPPER
    # ------------------------------------------------------------------

    def _run_stage(
        self,
        ctx: SceneContext,
        stage_name: str,
        fn: Callable[[SceneContext], None],
        params: dict | None = None,
        max_retries: int = 2,
    ) -> bool:
        """
        Execute a single pipeline stage with retry logic and DB tracking.

        Args:
            ctx         : Current scene context
            stage_name  : Stage name (must match processing_stages.stage_name)
            fn          : Callable that performs the stage logic
            params      : ETL parameters to store in job record
            max_retries : Max retry attempts on failure

        Returns:
            True if stage succeeded, False if all attempts failed
        """
        # Skip if already done (checkpoint resume)
        if self.is_stage_complete(ctx.scene_id, stage_name):
            logger.info("[ORCH] Stage %s already complete for scene=%d — skipping",
                        stage_name, ctx.scene_id)
            ctx.completed_stages.append(stage_name)
            return True

        attempt = 0
        delay   = 5.0  # seconds

        while attempt <= max_retries:
            attempt += 1
            job_id = self._meta.insert_processing_job(
                scene_id       = ctx.scene_id,
                stage_name     = stage_name,
                attempt_number = attempt,
                parameters     = params or {},
            )
            ctx.job_ids[stage_name] = job_id
            self._meta.start_job(job_id)

            try:
                logger.info("[ORCH] ▶ Stage %s (attempt %d/%d) scene=%d",
                            stage_name, attempt, max_retries + 1, ctx.scene_id)
                fn(ctx)
                self._meta.complete_job(job_id, status=JobStatusEnum.SUCCESS)
                ctx.completed_stages.append(stage_name)
                logger.info("[ORCH] ✓ Stage %s complete (scene=%d)", stage_name, ctx.scene_id)
                return True

            except Exception as exc:
                err_msg = f"{type(exc).__name__}: {exc}"
                logger.error("[ORCH] ✗ Stage %s failed (attempt %d): %s",
                             stage_name, attempt, err_msg)

                if attempt > max_retries:
                    self._meta.complete_job(
                        job_id,
                        status=JobStatusEnum.FAILED,
                        error_code="STAGE_FAILED",
                        error_message=err_msg,
                    )
                    ctx.failed_stage   = stage_name
                    ctx.error_message  = err_msg
                    self._meta.insert_alert_event(
                        event_type = "PIPELINE_ERROR",
                        severity   = "CRITICAL",
                        title      = f"Pipeline failed at {stage_name}",
                        message    = err_msg,
                        scene_id   = ctx.scene_id,
                        job_id     = job_id,
                        metadata   = {"stage": stage_name, "attempt": attempt},
                    )
                    return False
                else:
                    self._meta.complete_job(
                        job_id,
                        status=JobStatusEnum.FAILED,
                        error_code="RETRYING",
                        error_message=err_msg,
                    )
                    logger.info("[ORCH] Retrying in %.1fs...", delay)
                    time.sleep(delay)
                    delay *= 2

        return False

    # ------------------------------------------------------------------
    # PIPELINE STAGES (stubs — wire your actual module functions here)
    # ------------------------------------------------------------------

    def _stage_download(self, ctx: SceneContext) -> None:
        """
        Module 1: Download / georeferencing recovery.
        Replace the body with actual module1_download.run(ctx) call.
        """
        logger.info("[M1] Downloading scene %s", ctx.product_identifier)
        # --- WIRE YOUR MODULE 1 HERE ---
        # from etl.module1_download import recover_scene
        # ctx.raw_file_path = recover_scene(ctx.product_identifier, ctx.region_id)
        # For now: validate file exists if already downloaded
        if ctx.raw_file_path and not Path(ctx.raw_file_path).exists():
            raise FileNotFoundError(f"Raw file not found: {ctx.raw_file_path}")

    def _stage_crop(self, ctx: SceneContext) -> None:
        """
        Module 2: Spatial cropping to Jabodetabek bbox.
        Registers BRONZE products and records lineage.
        """
        logger.info("[M2] Cropping scene=%d", ctx.scene_id)
        # --- WIRE YOUR MODULE 2 HERE ---
        # from etl.module2_crop import crop_to_bbox
        # vv_path, vh_path = crop_to_bbox(ctx.raw_file_path, JABODETABEK_BBOX)

        # Stub: define expected output paths
        base = self._out_dir / "bronze" / str(ctx.scene_id)
        base.mkdir(parents=True, exist_ok=True)
        ctx.crop_vv_path = str(base / f"{ctx.scene_id}_VV_crop.tif")
        ctx.crop_vh_path = str(base / f"{ctx.scene_id}_VH_crop.tif")

        # Register products (called after actual module writes files)
        for band, path in [("VV", ctx.crop_vv_path), ("VH", ctx.crop_vh_path)]:
            # In real impl: file must exist before hash
            stub_hash = hashlib.sha256(f"{ctx.scene_id}_{band}_BRONZE".encode()).hexdigest()
            pid = self._meta.insert_data_product(
                scene_id         = ctx.scene_id,
                job_id           = ctx.job_ids["CROP"],
                product_tier     = "BRONZE",
                product_type     = "CROPPED_TIFF",
                band_name        = band,
                file_path        = path,
                file_name        = Path(path).name,
                file_size_mb     = 45.0,   # stub
                data_hash_sha256 = stub_hash,
                rows=5000, cols=5000,
            )
            ctx.product_ids[f"BRONZE_{band}"] = pid

    def _stage_lee_filter(self, ctx: SceneContext) -> None:
        """
        Module 3: Lee adaptive filter speckle reduction.
        Registers SILVER products and records CROP→LEE lineage.
        """
        logger.info("[M3] Lee filtering scene=%d", ctx.scene_id)
        base = self._out_dir / "silver" / str(ctx.scene_id)
        base.mkdir(parents=True, exist_ok=True)
        ctx.lee_vv_path = str(base / f"{ctx.scene_id}_VV_lee.tif")
        ctx.lee_vh_path = str(base / f"{ctx.scene_id}_VH_lee.tif")

        lee_params = {"window_size": 7, "looks": 1, "sigma": 0.9}

        for band, path in [("VV", ctx.lee_vv_path), ("VH", ctx.lee_vh_path)]:
            stub_hash = hashlib.sha256(f"{ctx.scene_id}_{band}_SILVER".encode()).hexdigest()
            pid = self._meta.insert_data_product(
                scene_id         = ctx.scene_id,
                job_id           = ctx.job_ids["LEE_FILTER"],
                product_tier     = "SILVER",
                product_type     = "LEE_FILTERED",
                band_name        = band,
                file_path        = path,
                file_name        = Path(path).name,
                file_size_mb     = 42.0,
                data_hash_sha256 = stub_hash,
                rows=5000, cols=5000,
            )
            ctx.product_ids[f"SILVER_{band}"] = pid

            # Record BRONZE → SILVER lineage
            self._lineage.record_transformation(
                parent_product_id   = ctx.product_ids[f"BRONZE_{band}"],
                child_product_id    = pid,
                transformation_type = "LEE_FILTER",
                job_id              = ctx.job_ids["LEE_FILTER"],
                params              = lee_params,
            )

    def _stage_cog_export(self, ctx: SceneContext) -> None:
        """
        Module 4: COG export + normalization.
        Registers GOLD products and records SILVER→GOLD lineage.
        """
        logger.info("[M4] COG export scene=%d", ctx.scene_id)
        base = self._out_dir / "gold" / str(ctx.scene_id)
        base.mkdir(parents=True, exist_ok=True)
        ctx.cog_vv_path = str(base / f"{ctx.scene_id}_VV_cog.tif")
        ctx.cog_vh_path = str(base / f"{ctx.scene_id}_VH_cog.tif")

        cog_params = {"compression": "LZW", "blocksize": 512, "overview_levels": [2, 4, 8, 16]}

        for band, path in [("VV", ctx.cog_vv_path), ("VH", ctx.cog_vh_path)]:
            stub_hash = hashlib.sha256(f"{ctx.scene_id}_{band}_GOLD".encode()).hexdigest()
            pid = self._meta.insert_data_product(
                scene_id         = ctx.scene_id,
                job_id           = ctx.job_ids["COG_EXPORT"],
                product_tier     = "GOLD",
                product_type     = "COG",
                band_name        = band,
                file_path        = path,
                file_name        = Path(path).name,
                file_size_mb     = 38.0,
                data_hash_sha256 = stub_hash,
                file_format      = "COG",
                rows=5000, cols=5000,
            )
            ctx.product_ids[f"GOLD_{band}"] = pid

            self._lineage.record_transformation(
                parent_product_id   = ctx.product_ids[f"SILVER_{band}"],
                child_product_id    = pid,
                transformation_type = "COG_EXPORT",
                job_id              = ctx.job_ids["COG_EXPORT"],
                params              = cog_params,
            )

    def _stage_quality_analytics(self, ctx: SceneContext) -> None:
        """
        Module 6: Quality metrics computation.
        Computes and stores quality scores for GOLD products.
        """
        logger.info("[M6] Quality analytics scene=%d", ctx.scene_id)

        import random  # Replace with actual rasterio stats in Module 6
        random.seed(ctx.scene_id)

        for band in ["VV", "VH"]:
            pid        = ctx.product_ids.get(f"GOLD_{band}")
            if not pid:
                continue

            total      = 5000 * 5000
            nodata     = random.randint(0, int(total * 0.05))
            valid      = total - nodata
            mean_db    = random.uniform(-20.0, -5.0)
            std_db     = random.uniform(1.0, 4.0)
            speckle    = std_db / abs(mean_db)  # simplified CV
            score      = max(0, min(100, 85 - (nodata / total * 100) - (speckle * 10)))
            flag       = "PASS" if score >= 60 else "FAIL"

            self._meta.insert_quality_metrics(
                scene_id                = ctx.scene_id,
                product_id              = pid,
                band_name               = band,
                total_pixels            = total,
                valid_pixels            = valid,
                nodata_pixels           = nodata,
                quality_score           = round(score, 2),
                backscatter_mean_db     = round(mean_db, 4),
                backscatter_std_db      = round(std_db, 4),
                backscatter_min_db      = round(mean_db - 3 * std_db, 4),
                backscatter_max_db      = round(mean_db + 3 * std_db, 4),
                radiometric_consistency = True,
                speckle_index           = round(speckle, 4),
                quality_flag            = flag,
            )

    # ------------------------------------------------------------------
    # MAIN RUN
    # ------------------------------------------------------------------

    def run(self, ctx: SceneContext) -> SceneContext:
        """
        Execute the full pipeline for a single scene with checkpoint resume.

        Args:
            ctx : SceneContext initialized with scene_id, product_identifier, etc.

        Returns:
            Updated SceneContext with completed_stages, product_ids, job_ids populated

        Example:
            ctx = SceneContext(
                scene_id=1,
                product_identifier="S1A_IW_GRDH_...",
                region_id=1,
                raw_file_path="/data/raw/S1A_IW_GRDH.zip"
            )
            result = orchestrator.run(ctx)
        """
        logger.info("[ORCH] ═══ Pipeline start: scene=%d pid=%s ═══",
                    ctx.scene_id, ctx.product_identifier)
        start = time.time()

        stages = [
            ("DOWNLOAD",          self._stage_download,         {"action": "recover"}),
            ("CROP",              self._stage_crop,             {"bbox": "JABODETABEK"}),
            ("LEE_FILTER",        self._stage_lee_filter,       {"window_size": 7}),
            ("COG_EXPORT",        self._stage_cog_export,       {"compression": "LZW"}),
            ("QUALITY_ANALYTICS", self._stage_quality_analytics, {}),
        ]

        for stage_name, fn, params in stages:
            success = self._run_stage(ctx, stage_name, fn, params)
            if not success:
                logger.error("[ORCH] ═══ Pipeline ABORTED at stage=%s scene=%d ═══",
                             stage_name, ctx.scene_id)
                return ctx

        elapsed = time.time() - start
        logger.info("[ORCH] ═══ Pipeline COMPLETE: scene=%d (%.1fs) ═══",
                    ctx.scene_id, elapsed)
        return ctx

    def run_batch(self, scene_contexts: list[SceneContext]) -> dict:
        """
        Process multiple scenes sequentially. Failed scenes do not block others.

        Args:
            scene_contexts : List of SceneContext objects to process

        Returns:
            Summary dict: {total, success, failed, failed_scene_ids}
        """
        total   = len(scene_contexts)
        success = 0
        failed  = []

        for i, ctx in enumerate(scene_contexts, 1):
            logger.info("[ORCH] Batch progress: %d/%d scene=%d", i, total, ctx.scene_id)
            result = self.run(ctx)
            if result.failed_stage:
                failed.append(ctx.scene_id)
            else:
                success += 1

        summary = {
            "total":            total,
            "success":          success,
            "failed":           len(failed),
            "failed_scene_ids": failed,
        }
        logger.info("[ORCH] Batch complete: %s", summary)
        return summary