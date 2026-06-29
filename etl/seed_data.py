# etl/seed_data.py
"""
Seed Data Generator: Insert 1 synthetic Sentinel-1 scene through the full pipeline.
Creates realistic test data for all 11 tables.

Author : Julius Marselinus (BRONTO) - NIM 00000111989
Program: Sistem Informasi - Universitas Multimedia Nusantara

Run:
    python -m etl.seed_data
"""

from __future__ import annotations

import hashlib
import logging
import sys
from datetime import datetime, timezone

from sqlalchemy import text

from etl.database_client import (
    AlertEventTypeEnum,
    AlertSeverityEnum,
    DatabaseClient,
    JobStatusEnum,
    OrbitDirectionEnum,
    ProcessingStage,
    ProductTierEnum,
    RegionOfInterest,
    SatelliteScene,
    StorageLocationEnum,
)
from etl.lineage_tracker import LineageTracker
from etl.metadata_manager import MetadataManager

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("seed_data")


# ---------------------------------------------------------------------------
# Synthetic data constants
# ---------------------------------------------------------------------------

JABODETABEK_WKT = (
    "POLYGON((106.4 -6.7, 107.2 -6.7, 107.2 -5.9, 106.4 -5.9, 106.4 -6.7))"
)

SCENE_PRODUCT_ID = "S1A_IW_GRDH_1SDV_20240115T225041_20240115T225106_052186_064F3A_B5C2"

ACQUISITION_DT = datetime(2024, 1, 15, 22, 50, 41, tzinfo=timezone.utc)


def _fake_hash(label: str) -> str:
    """Generate deterministic fake SHA-256 hash for testing."""
    return hashlib.sha256(label.encode()).hexdigest()


def seed(db: DatabaseClient) -> dict:
    """
    Insert one complete synthetic Sentinel-1 scene through all pipeline stages.

    Inserts into:
        regions_of_interest (1 row if not exists)
        satellite_scenes    (1 row)
        processing_jobs     (5 rows — one per stage)
        data_products       (4 rows — RAW + 2×BRONZE VV/VH + 2×SILVER + 2×GOLD)
        quality_metrics     (2 rows — VV + VH for GOLD product)
        data_lineage        (6 rows — 3 stages × 2 bands)
        alert_events        (1 info row)

    Returns:
        dict with all inserted IDs for verification
    """
    meta    = MetadataManager(db)
    lineage = LineageTracker(db)
    ids     = {}

    logger.info("═══ SEED DATA START ═══")

    # ------------------------------------------------------------------
    # 1. Ensure Jabodetabek ROI exists
    # ------------------------------------------------------------------
    with db.session() as sess:
        roi = sess.scalar(
            text("SELECT region_id FROM regions_of_interest WHERE region_code = 'JABODTK'")
        )
        if not roi:
            sess.execute(text("""
                INSERT INTO regions_of_interest
                    (region_code, name, description, bbox, area_km2, admin_level, country_code, is_active)
                VALUES
                    ('JABODTK', 'Jabodetabek',
                     'Jakarta-Bogor-Depok-Tangerang-Bekasi metropolitan area',
                     ST_GeomFromText(:wkt, 4326),
                     6392.0, 2, 'ID', TRUE)
                ON CONFLICT (region_code) DO NOTHING
            """), {"wkt": JABODETABEK_WKT})
            roi = sess.scalar(
                text("SELECT region_id FROM regions_of_interest WHERE region_code = 'JABODTK'")
            )
        ids["region_id"] = roi
    logger.info("[SEED] ROI region_id=%d", ids["region_id"])

    # ------------------------------------------------------------------
    # 2. Register satellite scene (Module 1)
    # ------------------------------------------------------------------
    scene_id = meta.insert_satellite_scene(
        product_identifier   = SCENE_PRODUCT_ID,
        acquisition_datetime = ACQUISITION_DT,
        region_id            = ids["region_id"],
        bbox_wkt             = JABODETABEK_WKT,
        orbit_direction      = OrbitDirectionEnum.ASCENDING,
        polarization_vv      = True,
        polarization_vh      = True,
        orbit_number         = 52186,
        relative_orbit       = 98,
        cloud_cover_percent  = 12.5,
        incidence_angle_near = 30.8,
        incidence_angle_far  = 46.2,
        resolution_m         = 10,
        raw_file_path        = "/data/raw/S1A_IW_GRDH_20240115.zip",
        raw_file_size_mb     = 847.3,
        download_url         = "https://scihub.copernicus.eu/dhus/odata/v1/Products('052186')",
        checksum_md5         = "d41d8cd98f00b204e9800998ecf8427e",
        instrument_mode      = "IW",
    )
    ids["scene_id"] = scene_id
    logger.info("[SEED] Scene scene_id=%d", scene_id)

    # ------------------------------------------------------------------
    # 3. DOWNLOAD job (Module 1)
    # ------------------------------------------------------------------
    dl_job_id = meta.insert_processing_job(
        scene_id, "DOWNLOAD", parameters={"source": "copernicus"}
    )
    meta.start_job(dl_job_id)

    # RAW products (VV + VH)
    raw_vv_id = meta.insert_data_product(
        scene_id=scene_id, job_id=dl_job_id,
        product_tier=ProductTierEnum.RAW, product_type="ORIGINAL_TIFF",
        band_name="VV",
        file_path="/data/raw/S1A_20240115_VV.tif",
        file_name="S1A_20240115_VV.tif",
        file_size_mb=423.6,
        data_hash_sha256=_fake_hash("RAW_VV"),
        rows=23000, cols=16600,
    )
    raw_vh_id = meta.insert_data_product(
        scene_id=scene_id, job_id=dl_job_id,
        product_tier=ProductTierEnum.RAW, product_type="ORIGINAL_TIFF",
        band_name="VH",
        file_path="/data/raw/S1A_20240115_VH.tif",
        file_name="S1A_20240115_VH.tif",
        file_size_mb=423.7,
        data_hash_sha256=_fake_hash("RAW_VH"),
        rows=23000, cols=16600,
    )
    meta.complete_job(dl_job_id, output_size_mb=847.3)
    ids.update({"dl_job_id": dl_job_id, "raw_vv_id": raw_vv_id, "raw_vh_id": raw_vh_id})
    logger.info("[SEED] DOWNLOAD complete. RAW products: VV=%d VH=%d", raw_vv_id, raw_vh_id)

    # ------------------------------------------------------------------
    # 4. CROP job (Module 2) → BRONZE
    # ------------------------------------------------------------------
    crop_job_id = meta.insert_processing_job(
        scene_id, "CROP",
        parameters={"bbox": JABODETABEK_WKT, "target_crs": "EPSG:4326"}
    )
    meta.start_job(crop_job_id)

    bronze_vv_id = meta.insert_data_product(
        scene_id=scene_id, job_id=crop_job_id,
        product_tier=ProductTierEnum.BRONZE, product_type="CROPPED_TIFF",
        band_name="VV",
        file_path="/processed/bronze/1/S1A_20240115_VV_crop.tif",
        file_name="S1A_20240115_VV_crop.tif",
        file_size_mb=48.2, data_hash_sha256=_fake_hash("BRONZE_VV"),
        rows=5500, cols=5800, pixel_size_m=10.0,
    )
    bronze_vh_id = meta.insert_data_product(
        scene_id=scene_id, job_id=crop_job_id,
        product_tier=ProductTierEnum.BRONZE, product_type="CROPPED_TIFF",
        band_name="VH",
        file_path="/processed/bronze/1/S1A_20240115_VH_crop.tif",
        file_name="S1A_20240115_VH_crop.tif",
        file_size_mb=48.1, data_hash_sha256=_fake_hash("BRONZE_VH"),
        rows=5500, cols=5800, pixel_size_m=10.0,
    )
    meta.complete_job(crop_job_id, output_size_mb=96.3)
    ids.update({"crop_job_id": crop_job_id, "bronze_vv_id": bronze_vv_id, "bronze_vh_id": bronze_vh_id})

    # RAW → BRONZE lineage
    lineage.record_transformation(raw_vv_id, bronze_vv_id, "CROP", crop_job_id,
                                  {"bbox": "JABODETABEK", "resampling": "bilinear"})
    lineage.record_transformation(raw_vh_id, bronze_vh_id, "CROP", crop_job_id,
                                  {"bbox": "JABODETABEK", "resampling": "bilinear"})
    logger.info("[SEED] CROP complete. BRONZE: VV=%d VH=%d", bronze_vv_id, bronze_vh_id)

    # ------------------------------------------------------------------
    # 5. LEE_FILTER job (Module 3) → SILVER
    # ------------------------------------------------------------------
    lee_job_id = meta.insert_processing_job(
        scene_id, "LEE_FILTER",
        parameters={"window_size": 7, "looks": 1, "sigma": 0.9}
    )
    meta.start_job(lee_job_id)

    silver_vv_id = meta.insert_data_product(
        scene_id=scene_id, job_id=lee_job_id,
        product_tier=ProductTierEnum.SILVER, product_type="LEE_FILTERED",
        band_name="VV",
        file_path="/processed/silver/1/S1A_20240115_VV_lee.tif",
        file_name="S1A_20240115_VV_lee.tif",
        file_size_mb=45.8, data_hash_sha256=_fake_hash("SILVER_VV"),
        rows=5500, cols=5800,
    )
    silver_vh_id = meta.insert_data_product(
        scene_id=scene_id, job_id=lee_job_id,
        product_tier=ProductTierEnum.SILVER, product_type="LEE_FILTERED",
        band_name="VH",
        file_path="/processed/silver/1/S1A_20240115_VH_lee.tif",
        file_name="S1A_20240115_VH_lee.tif",
        file_size_mb=45.6, data_hash_sha256=_fake_hash("SILVER_VH"),
        rows=5500, cols=5800,
    )
    meta.complete_job(lee_job_id, output_size_mb=91.4)
    ids.update({"lee_job_id": lee_job_id, "silver_vv_id": silver_vv_id, "silver_vh_id": silver_vh_id})

    lineage.record_transformation(bronze_vv_id, silver_vv_id, "LEE_FILTER", lee_job_id,
                                  {"window_size": 7, "looks": 1, "sigma": 0.9})
    lineage.record_transformation(bronze_vh_id, silver_vh_id, "LEE_FILTER", lee_job_id,
                                  {"window_size": 7, "looks": 1, "sigma": 0.9})
    logger.info("[SEED] LEE_FILTER complete. SILVER: VV=%d VH=%d", silver_vv_id, silver_vh_id)

    # ------------------------------------------------------------------
    # 6. COG_EXPORT job (Module 4) → GOLD
    # ------------------------------------------------------------------
    cog_job_id = meta.insert_processing_job(
        scene_id, "COG_EXPORT",
        parameters={"compression": "LZW", "blocksize": 512, "overview_levels": [2,4,8,16]}
    )
    meta.start_job(cog_job_id)

    gold_vv_id = meta.insert_data_product(
        scene_id=scene_id, job_id=cog_job_id,
        product_tier=ProductTierEnum.GOLD, product_type="COG",
        band_name="VV",
        file_path="/processed/gold/1/S1A_20240115_VV_cog.tif",
        file_name="S1A_20240115_VV_cog.tif",
        file_size_mb=41.2, data_hash_sha256=_fake_hash("GOLD_VV"),
        file_format="COG", rows=5500, cols=5800, pixel_size_m=10.0,
    )
    gold_vh_id = meta.insert_data_product(
        scene_id=scene_id, job_id=cog_job_id,
        product_tier=ProductTierEnum.GOLD, product_type="COG",
        band_name="VH",
        file_path="/processed/gold/1/S1A_20240115_VH_cog.tif",
        file_name="S1A_20240115_VH_cog.tif",
        file_size_mb=41.0, data_hash_sha256=_fake_hash("GOLD_VH"),
        file_format="COG", rows=5500, cols=5800, pixel_size_m=10.0,
    )
    meta.complete_job(cog_job_id, output_size_mb=82.2)
    ids.update({"cog_job_id": cog_job_id, "gold_vv_id": gold_vv_id, "gold_vh_id": gold_vh_id})

    lineage.record_transformation(silver_vv_id, gold_vv_id, "COG_EXPORT", cog_job_id,
                                  {"compression": "LZW", "blocksize": 512})
    lineage.record_transformation(silver_vh_id, gold_vh_id, "COG_EXPORT", cog_job_id,
                                  {"compression": "LZW", "blocksize": 512})
    logger.info("[SEED] COG_EXPORT complete. GOLD: VV=%d VH=%d", gold_vv_id, gold_vh_id)

    # ------------------------------------------------------------------
    # 7. QUALITY_ANALYTICS (Module 6)
    # ------------------------------------------------------------------
    qa_job_id = meta.insert_processing_job(scene_id, "QUALITY_ANALYTICS",
                                           parameters={"cloud_threshold": 20.0})
    meta.start_job(qa_job_id)

    total = 5500 * 5800  # 31,900,000 pixels

    vv_metric_id = meta.insert_quality_metrics(
        scene_id=scene_id, product_id=gold_vv_id, band_name="VV",
        total_pixels=total, valid_pixels=total - 320000, nodata_pixels=320000,
        quality_score=82.4,
        backscatter_mean_db=-12.37, backscatter_std_db=2.81,
        backscatter_min_db=-28.44, backscatter_max_db=1.92,
        radiometric_consistency=True, speckle_index=0.227,
        quality_flag="PASS",
        notes="Good quality acquisition. Low cloud cover. Normal backscatter range.",
    )
    vh_metric_id = meta.insert_quality_metrics(
        scene_id=scene_id, product_id=gold_vh_id, band_name="VH",
        total_pixels=total, valid_pixels=total - 318500, nodata_pixels=318500,
        quality_score=81.1,
        backscatter_mean_db=-19.82, backscatter_std_db=3.14,
        backscatter_min_db=-34.61, backscatter_max_db=-4.23,
        radiometric_consistency=True, speckle_index=0.258,
        quality_flag="PASS",
        notes="Good quality. VH cross-pol within normal IW mode range.",
    )
    meta.complete_job(qa_job_id)
    ids.update({"qa_job_id": qa_job_id, "vv_metric_id": vv_metric_id, "vh_metric_id": vh_metric_id})

    # Info alert: data arrived and processed
    alert_id = meta.insert_alert_event(
        event_type=AlertEventTypeEnum.DATA_ARRIVAL,
        title=f"New scene processed: scene_id={scene_id}",
        message=f"Scene {SCENE_PRODUCT_ID} completed full pipeline. QA PASS (VV=82.4, VH=81.1).",
        severity=AlertSeverityEnum.INFO,
        scene_id=scene_id,
        metadata={"vv_score": 82.4, "vh_score": 81.1, "pipeline_complete": True},
    )
    ids["alert_id"] = alert_id

    logger.info("[SEED] QUALITY_ANALYTICS complete. Metrics: VV=%d VH=%d", vv_metric_id, vh_metric_id)
    logger.info("═══ SEED DATA COMPLETE ═══")
    logger.info("Summary IDs: %s", ids)

    return ids


def verify_seed(db: DatabaseClient, ids: dict) -> None:
    """Run basic verification queries against seeded data."""
    logger.info("═══ VERIFICATION START ═══")

    with db.session() as sess:
        # 1. Scene count
        from sqlalchemy import text
        scene_count = sess.scalar(text("SELECT COUNT(*) FROM satellite_scenes"))
        logger.info("[VERIFY] satellite_scenes count: %d", scene_count)
        assert scene_count >= 1

        # 2. Gold products
        gold_count = sess.scalar(
            text("SELECT COUNT(*) FROM data_products WHERE product_tier = 'GOLD' AND is_latest = TRUE")
        )
        logger.info("[VERIFY] GOLD latest products: %d", gold_count)
        assert gold_count >= 2, "Expected VV + VH gold products"

        # 3. Lineage chain
        lin_count = sess.scalar(
            text("SELECT COUNT(*) FROM data_lineage WHERE job_id = :jid"),
            {"jid": ids["cog_job_id"]}
        )
        logger.info("[VERIFY] Lineage records for COG job: %d", lin_count)
        assert lin_count == 2

        # 4. Quality metrics
        qm_count = sess.scalar(
            text("SELECT COUNT(*) FROM quality_metrics WHERE scene_id = :sid"),
            {"sid": ids["scene_id"]}
        )
        logger.info("[VERIFY] Quality metrics for scene: %d", qm_count)
        assert qm_count == 2

        # 5. All jobs succeeded
        failed_jobs = sess.scalar(
            text("SELECT COUNT(*) FROM processing_jobs WHERE scene_id = :sid AND status = 'FAILED'"),
            {"sid": ids["scene_id"]}
        )
        logger.info("[VERIFY] Failed jobs: %d", failed_jobs)
        assert failed_jobs == 0

    logger.info("═══ ALL VERIFICATIONS PASSED ═══")


if __name__ == "__main__":
    db = DatabaseClient.from_env()
    try:
        inserted_ids = seed(db)
        verify_seed(db, inserted_ids)
        print("\n✅ Seed data inserted and verified successfully.")
        print(f"   Scene ID  : {inserted_ids['scene_id']}")
        print(f"   GOLD VV   : product_id={inserted_ids['gold_vv_id']}")
        print(f"   GOLD VH   : product_id={inserted_ids['gold_vh_id']}")
        print(f"   QA VV     : metric_id={inserted_ids['vv_metric_id']}")
    except Exception as e:
        logger.error("Seed failed: %s", e, exc_info=True)
        sys.exit(1)
    finally:
        db.dispose()