# tests/test_etl_pipeline.py
"""
Integration tests: end-to-end ETL pipeline flow (Module 1-6).
Tests metadata tracking, lineage recording, and checkpoint system.

Author : Julius Marselinus (BRONTO) - NIM 00000111989
Program: Sistem Informasi - Universitas Multimedia Nusantara

Run:
    pytest tests/test_etl_pipeline.py -v
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

import pytest
from sqlalchemy import text


def fake_hash(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


# ---------------------------------------------------------------------------
# 1. END-TO-END PIPELINE FLOW
# ---------------------------------------------------------------------------

class TestEndToEndFlow:
    """Verify the complete Module 1–6 pipeline produces correct DB state."""

    def test_full_pipeline_seed(self, db_client, sample_region):
        """
        Run the seed_data script and verify all 11 tables are populated.
        This is the primary integration test for the entire pipeline.
        """
        from etl.seed_data import seed, verify_seed
        ids = seed(db_client)
        verify_seed(db_client, ids)

        assert ids["scene_id"]    > 0
        assert ids["gold_vv_id"]  > 0
        assert ids["gold_vh_id"]  > 0
        assert ids["vv_metric_id"] > 0
        assert ids["vh_metric_id"] > 0

    def test_pipeline_all_jobs_succeed(self, db_client, sample_region):
        """After full pipeline run, all 5 jobs must have status=SUCCESS."""
        from etl.seed_data import seed

        ids = seed(db_client)
        scene_id = ids["scene_id"]

        with db_client.session() as sess:
            failed = sess.scalar(text("""
                SELECT COUNT(*) FROM processing_jobs
                WHERE scene_id = :sid AND status = 'FAILED'
            """), {"sid": scene_id})
        assert failed == 0, f"{failed} failed jobs found — expected 0"

        with db_client.session() as sess:
            success = sess.scalar(text("""
                SELECT COUNT(*) FROM processing_jobs
                WHERE scene_id = :sid AND status = 'SUCCESS'
            """), {"sid": scene_id})
        assert success == 5, f"Expected 5 successful stages, got {success}"

    def test_pipeline_product_tiers(self, db_client, sample_region):
        """After pipeline, each band must have RAW, BRONZE, SILVER, GOLD products."""
        from etl.seed_data import seed
        from etl.metadata_manager import MetadataManager

        ids  = seed(db_client)
        meta = MetadataManager(db_client)

        for band in ["VV", "VH"]:
            for tier in ["RAW", "BRONZE", "SILVER", "GOLD"]:
                products = meta.get_products_by_scene(
                    ids["scene_id"], tier=tier, latest_only=True
                )
                band_products = [p for p in products if p["band_name"] == band]
                assert len(band_products) >= 1, (
                    f"Missing {tier} product for band={band}"
                )


# ---------------------------------------------------------------------------
# 2. METADATA TRACKING
# ---------------------------------------------------------------------------

class TestMetadataTracking:
    """Verify scene → job → product → quality linkage is complete."""

    def test_scene_to_jobs_link(self, db_client, meta, sample_scene):
        """Each pipeline stage creates exactly one job linked to the scene."""
        stages = ["DOWNLOAD", "CROP", "LEE_FILTER", "COG_EXPORT", "QUALITY_ANALYTICS"]
        for stage in stages:
            job_id = meta.insert_processing_job(sample_scene, stage)
            meta.start_job(job_id)
            meta.complete_job(job_id)

        status = meta.get_pipeline_status(sample_scene)
        assert len(status) == 5

        stage_names = {s["stage_name"] for s in status}
        assert stage_names == set(stages)

    def test_product_linked_to_job(self, db_client, meta, sample_scene):
        """Data products must be linked to the producing job."""
        job_id = meta.insert_processing_job(sample_scene, "COG_EXPORT")
        meta.start_job(job_id)

        prod_id = meta.insert_data_product(
            scene_id=sample_scene, job_id=job_id,
            product_tier="GOLD", product_type="COG",
            band_name="VV", file_path="/tmp/test_vv.tif", file_name="test_vv.tif",
            file_size_mb=40.0, data_hash_sha256=fake_hash("TRACK_VV"),
        )

        products = meta.get_products_by_scene(sample_scene, tier="GOLD")
        vv = next((p for p in products if p["band_name"] == "VV"), None)
        assert vv is not None
        assert vv["job_id"] == job_id

    def test_quality_linked_to_product(self, db_client, meta, sample_scene):
        """Quality metrics must reference both scene and product."""
        job_id  = meta.insert_processing_job(sample_scene, "QUALITY_ANALYTICS")
        meta.start_job(job_id)
        prod_id = meta.insert_data_product(
            scene_id=sample_scene, job_id=job_id,
            product_tier="GOLD", product_type="COG",
            band_name="VH", file_path="/tmp/vh.tif", file_name="vh.tif",
            file_size_mb=38.0, data_hash_sha256=fake_hash("QUALITY_VH"),
        )
        metric_id = meta.insert_quality_metrics(
            scene_id=sample_scene, product_id=prod_id, band_name="VH",
            total_pixels=500000, valid_pixels=490000, nodata_pixels=10000,
            quality_score=78.5, quality_flag="PASS",
        )

        metrics = meta.get_quality_by_scene(sample_scene)
        vh = next((m for m in metrics if m["band_name"] == "VH"), None)
        assert vh is not None
        assert vh["quality_score"] == 78.5

    def test_is_latest_flag_update(self, db_client, meta, sample_scene):
        """Inserting a new product for same scene/band/tier marks old one as not latest."""
        job_id = meta.insert_processing_job(sample_scene, "COG_EXPORT")

        # First product
        pid1 = meta.insert_data_product(
            scene_id=sample_scene, job_id=job_id,
            product_tier="GOLD", product_type="COG",
            band_name="VV", file_path="/tmp/v1.tif", file_name="v1.tif",
            file_size_mb=40.0, data_hash_sha256=fake_hash("LATEST_V1"),
        )
        # Second product (same tier+band → should mark pid1 as not latest)
        pid2 = meta.insert_data_product(
            scene_id=sample_scene, job_id=job_id,
            product_tier="GOLD", product_type="COG",
            band_name="VV", file_path="/tmp/v2.tif", file_name="v2.tif",
            file_size_mb=39.0, data_hash_sha256=fake_hash("LATEST_V2"),
        )

        from etl.database_client import DataProduct
        with db_client.session() as sess:
            p1 = sess.get(DataProduct, pid1)
            p2 = sess.get(DataProduct, pid2)
        assert p1.is_latest is False, "Old product should be marked not latest"
        assert p2.is_latest is True,  "New product should be latest"


# ---------------------------------------------------------------------------
# 3. DATA QUALITY TESTS
# ---------------------------------------------------------------------------

class TestDataQuality:
    """Verify quality score computation and flag assignment."""

    def test_quality_score_range(self, db_client, meta, sample_scene):
        """Quality score must always be in range [0, 100]."""
        job_id  = meta.insert_processing_job(sample_scene, "QUALITY_ANALYTICS")
        prod_id = meta.insert_data_product(
            scene_id=sample_scene, job_id=job_id,
            product_tier="GOLD", product_type="COG", band_name="VV",
            file_path="/tmp/score_test.tif", file_name="score_test.tif",
            file_size_mb=38.0, data_hash_sha256=fake_hash("SCORE_RANGE"),
        )
        metric_id = meta.insert_quality_metrics(
            scene_id=sample_scene, product_id=prod_id, band_name="VV",
            total_pixels=100000, valid_pixels=90000, nodata_pixels=10000,
            quality_score=72.3, quality_flag="PASS",
        )
        metrics = meta.get_quality_by_scene(sample_scene)
        for m in metrics:
            assert 0 <= m["quality_score"] <= 100, (
                f"Quality score {m['quality_score']} out of range [0, 100]"
            )

    def test_quality_flag_values(self, db_client, meta, sample_scene):
        """quality_flag must be one of: PASS, FAIL, WARNING, UNCHECKED."""
        valid_flags = {"PASS", "FAIL", "WARNING", "UNCHECKED"}
        job_id  = meta.insert_processing_job(sample_scene, "QUALITY_ANALYTICS")
        prod_id = meta.insert_data_product(
            scene_id=sample_scene, job_id=job_id,
            product_tier="GOLD", product_type="COG", band_name="VH",
            file_path="/tmp/flag_test.tif", file_name="flag_test.tif",
            file_size_mb=38.0, data_hash_sha256=fake_hash("FLAG_TEST"),
        )
        meta.insert_quality_metrics(
            scene_id=sample_scene, product_id=prod_id, band_name="VH",
            total_pixels=100000, valid_pixels=70000, nodata_pixels=30000,
            quality_score=45.0, quality_flag="FAIL",
        )
        metrics = meta.get_quality_by_scene(sample_scene)
        for m in metrics:
            assert m["quality_flag"] in valid_flags, (
                f"Invalid quality_flag: {m['quality_flag']}"
            )

    def test_nodata_percent_consistency(self, db_client, meta, sample_scene):
        """nodata_pixels must not exceed total_pixels."""
        job_id  = meta.insert_processing_job(sample_scene, "QUALITY_ANALYTICS")
        prod_id = meta.insert_data_product(
            scene_id=sample_scene, job_id=job_id,
            product_tier="GOLD", product_type="COG", band_name="VV",
            file_path="/tmp/nodata_test.tif", file_name="nodata_test.tif",
            file_size_mb=38.0, data_hash_sha256=fake_hash("NODATA_TEST"),
        )
        with pytest.raises(Exception):
            # nodata_pixels > total_pixels should violate business logic
            meta.insert_quality_metrics(
                scene_id=sample_scene, product_id=prod_id, band_name="VV",
                total_pixels=100,
                valid_pixels=-50,  # invalid
                nodata_pixels=200, # > total
                quality_score=150, # > 100: violates CHECK constraint
                quality_flag="FAIL",
            )


# ---------------------------------------------------------------------------
# 4. LINEAGE TRACKING TESTS
# ---------------------------------------------------------------------------

class TestLineageTracking:
    """Verify parent-child lineage recording and traversal."""

    def test_lineage_chain_recorded(self, db_client, lineage, meta, sample_scene):
        """Full pipeline lineage (RAW→BRONZE→SILVER→GOLD) must be queryable."""
        dl_job   = meta.insert_processing_job(sample_scene, "DOWNLOAD")
        crop_job = meta.insert_processing_job(sample_scene, "CROP")
        lee_job  = meta.insert_processing_job(sample_scene, "LEE_FILTER")
        cog_job  = meta.insert_processing_job(sample_scene, "COG_EXPORT")

        raw_id = meta.insert_data_product(
            scene_id=sample_scene, job_id=dl_job,
            product_tier="RAW",    product_type="ORIGINAL_TIFF", band_name="VV",
            file_path="/tmp/raw.tif", file_name="raw.tif",
            file_size_mb=400.0, data_hash_sha256=fake_hash("LIN_RAW"),
        )
        bronze_id = meta.insert_data_product(
            scene_id=sample_scene, job_id=crop_job,
            product_tier="BRONZE", product_type="CROPPED_TIFF", band_name="VV",
            file_path="/tmp/bronze.tif", file_name="bronze.tif",
            file_size_mb=48.0, data_hash_sha256=fake_hash("LIN_BRONZE"),
        )
        silver_id = meta.insert_data_product(
            scene_id=sample_scene, job_id=lee_job,
            product_tier="SILVER", product_type="LEE_FILTERED", band_name="VV",
            file_path="/tmp/silver.tif", file_name="silver.tif",
            file_size_mb=45.0, data_hash_sha256=fake_hash("LIN_SILVER"),
        )
        gold_id = meta.insert_data_product(
            scene_id=sample_scene, job_id=cog_job,
            product_tier="GOLD",   product_type="COG", band_name="VV",
            file_path="/tmp/gold.tif", file_name="gold.tif",
            file_size_mb=41.0, data_hash_sha256=fake_hash("LIN_GOLD"),
        )

        lineage.record_transformation(raw_id,    bronze_id, "CROP",       crop_job)
        lineage.record_transformation(bronze_id, silver_id, "LEE_FILTER", lee_job)
        lineage.record_transformation(silver_id, gold_id,   "COG_EXPORT", cog_job)

        # Trace ancestors of GOLD → should find 3 steps
        chain = lineage.get_lineage_chain(gold_id, direction="ancestors")
        assert len(chain) == 3, f"Expected 3 lineage steps, got {len(chain)}"

        transform_types = {step["transformation_type"] for step in chain}
        assert "CROP"       in transform_types
        assert "LEE_FILTER" in transform_types
        assert "COG_EXPORT" in transform_types

    def test_descendants_chain(self, db_client, lineage, meta, sample_scene):
        """Descendants traversal from RAW should reach GOLD."""
        dl_job  = meta.insert_processing_job(sample_scene, "DOWNLOAD")
        cr_job  = meta.insert_processing_job(sample_scene, "CROP")

        raw_id    = meta.insert_data_product(
            scene_id=sample_scene, job_id=dl_job,
            product_tier="RAW", product_type="ORIGINAL_TIFF", band_name="VH",
            file_path="/tmp/raw_desc.tif", file_name="raw_desc.tif",
            file_size_mb=400.0, data_hash_sha256=fake_hash("DESC_RAW"),
        )
        bronze_id = meta.insert_data_product(
            scene_id=sample_scene, job_id=cr_job,
            product_tier="BRONZE", product_type="CROPPED_TIFF", band_name="VH",
            file_path="/tmp/brnz_desc.tif", file_name="brnz_desc.tif",
            file_size_mb=48.0, data_hash_sha256=fake_hash("DESC_BRONZE"),
        )
        lineage.record_transformation(raw_id, bronze_id, "CROP", cr_job)

        chain = lineage.get_lineage_chain(raw_id, direction="descendants")
        assert len(chain) >= 1
        assert any(s["child_product_id"] == bronze_id for s in chain)


# ---------------------------------------------------------------------------
# 5. CHECKPOINT / RESUME TESTS
# ---------------------------------------------------------------------------

class TestCheckpointSystem:
    """Verify PostgreSQL-backed checkpoint and pipeline resume logic."""

    def test_completed_stages_query(self, db_client, meta, sample_scene):
        """get_completed_stages returns correct stage names for successful jobs."""
        from etl.module5_orchestrator import PipelineOrchestrator
        orch = PipelineOrchestrator(db_client)

        # Simulate DOWNLOAD success
        job_id = meta.insert_processing_job(sample_scene, "DOWNLOAD")
        meta.start_job(job_id)
        meta.complete_job(job_id)

        completed = orch.get_completed_stages(sample_scene)
        assert "DOWNLOAD" in completed
        assert "CROP"     not in completed

    def test_is_stage_complete_true(self, db_client, meta, sample_scene):
        """is_stage_complete returns True for a stage that succeeded."""
        from etl.module5_orchestrator import PipelineOrchestrator
        orch   = PipelineOrchestrator(db_client)
        job_id = meta.insert_processing_job(sample_scene, "CROP")
        meta.start_job(job_id)
        meta.complete_job(job_id)

        assert orch.is_stage_complete(sample_scene, "CROP") is True
        assert orch.is_stage_complete(sample_scene, "LEE_FILTER") is False

    def test_failed_job_not_in_completed(self, db_client, meta, sample_scene):
        """A FAILED job must NOT appear in completed stages."""
        from etl.module5_orchestrator import PipelineOrchestrator
        from etl.database_client import JobStatusEnum
        orch   = PipelineOrchestrator(db_client)
        job_id = meta.insert_processing_job(sample_scene, "LEE_FILTER")
        meta.start_job(job_id)
        meta.complete_job(job_id, status=JobStatusEnum.FAILED,
                          error_code="TIMEOUT", error_message="Lee filter timed out")

        completed = orch.get_completed_stages(sample_scene)
        assert "LEE_FILTER" not in completed
