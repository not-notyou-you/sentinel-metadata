# etl/module5_orchestrator.py
from __future__ import annotations
import logging
import os
import re
import shutil
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from queue import Queue
import rasterio
from shapely import wkt as shapely_wkt
from etl.database_client import DatabaseClient, DatasetJob
from etl.dataset_manager import (
    DatasetManager,
    compute_skip_stages,
    compute_tiers_to_delete,
    get_cancel_event,
    get_pause_event,
)
from etl.lineage_tracker import LineageTracker
from etl.metadata_manager import MetadataManager
from etl.module1_download import discover_scenes, download_scene
from etl.module1b_calibrate import run as calibrate_run
from etl.module2_crop import run as crop_run
from etl.module3_lee_filter import run as lee_run
from etl.module4_cog_export import run as cog_run
from etl.module6_analytics import compute_band_metrics

logger = logging.getLogger(__name__)

_MAX_CONCURRENT_SCENE_PIPELINES = int(os.getenv("PIPELINE_MAX_CONCURRENT_SCENES", "2"))
_pipeline_semaphore = threading.Semaphore(_MAX_CONCURRENT_SCENE_PIPELINES)


@dataclass
class _JobContext:
    db: DatabaseClient
    dsmgr: DatasetManager
    meta: MetadataManager
    lineage: LineageTracker
    job_id: int
    dataset_id: int
    region_id: int
    bbox_wkt: str
    bbox_tuple: tuple[float, float, float, float]
    required_tiers: list[str]
    skip_stages: set[str]
    min_quality_score: float
    base_dir: Path
    pause_event: threading.Event
    cancel_event: threading.Event


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _slug(product_identifier: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", product_identifier)


def _dataset_base_dir(dataset_id: int) -> Path:
    return Path("data") / "datasets" / str(dataset_id)


def _bbox_tuple_from_wkt(wkt_str: str) -> tuple[float, float, float, float]:
    return shapely_wkt.loads(wkt_str).bounds


def _file_size_mb(path: str) -> float:
    return round(Path(path).stat().st_size / (1024 ** 2), 3)


def _raster_dims(path: str) -> tuple[int | None, int | None]:
    try:
        with rasterio.open(path) as src:
            return src.height, src.width
    except Exception:
        return None, None


def _dir_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def _process_scene(jc: _JobContext, scene_meta: dict, dl_result) -> tuple[int, list[str]]:
    pid = scene_meta["product_identifier"]
    produced_tiers: list[str] = []

    scene_id = jc.meta.insert_satellite_scene(
        product_identifier=dl_result.product_identifier,
        acquisition_datetime=dl_result.acquisition_datetime,
        region_id=jc.region_id,
        bbox_wkt=jc.bbox_wkt,
        orbit_direction=dl_result.orbit_direction,
        orbit_number=dl_result.orbit_number,
        relative_orbit=dl_result.relative_orbit,
        cloud_cover_percent=dl_result.cloud_cover,
        raw_file_path=dl_result.zip_path or None,
        raw_file_size_mb=dl_result.file_size_mb,
        download_url=dl_result.download_url,
        checksum_md5=dl_result.checksum_md5,
    )
    jc.dsmgr.upsert_scene_job_state(
        jc.job_id, pid, scene_id=scene_id, current_stage="DOWNLOAD", stage_status="COMPLETED"
    )

    dl_job_id = jc.meta.insert_processing_job(scene_id, "DOWNLOAD", parameters={"dataset_id": jc.dataset_id})
    jc.meta.start_job(dl_job_id)
    raw_vv_id = jc.meta.insert_data_product(
        scene_id=scene_id, job_id=dl_job_id, dataset_id=jc.dataset_id,
        product_tier="RAW", product_type="RAW_EXTRACTED_TIFF", band_name="VV",
        file_path=dl_result.vv_tif_path, file_name=Path(dl_result.vv_tif_path).name,
        file_size_mb=_file_size_mb(dl_result.vv_tif_path),
        data_hash_sha256=jc.lineage.compute_sha256(dl_result.vv_tif_path),
    )
    raw_vh_id = jc.meta.insert_data_product(
        scene_id=scene_id, job_id=dl_job_id, dataset_id=jc.dataset_id,
        product_tier="RAW", product_type="RAW_EXTRACTED_TIFF", band_name="VH",
        file_path=dl_result.vh_tif_path, file_name=Path(dl_result.vh_tif_path).name,
        file_size_mb=_file_size_mb(dl_result.vh_tif_path),
        data_hash_sha256=jc.lineage.compute_sha256(dl_result.vh_tif_path),
    )
    jc.meta.complete_job(dl_job_id, output_size_mb=dl_result.file_size_mb)
    produced_tiers.append("RAW")

    if "CROP" in jc.skip_stages:
        return scene_id, produced_tiers
    jc.pause_event.wait()
    if jc.cancel_event.is_set():
        return scene_id, produced_tiers

    jc.dsmgr.upsert_scene_job_state(jc.job_id, pid, current_stage="CROP", stage_status="RUNNING")
    crop_job_id = jc.meta.insert_processing_job(
        scene_id, "CROP", parameters={"bbox": list(jc.bbox_tuple), "dataset_id": jc.dataset_id}
    )
    jc.meta.start_job(crop_job_id)

    slug = _slug(pid)
    calib_dir = jc.base_dir / "calib_work" / slug
    calib_dir.mkdir(parents=True, exist_ok=True)
    calib_vv, calib_vh = calibrate_run(dl_result.zip_path, dl_result.vv_tif_path, dl_result.vh_tif_path, str(calib_dir))

    bronze_dir = jc.base_dir / "bronze" / slug
    bronze_dir.mkdir(parents=True, exist_ok=True)
    crop_vv, crop_vh = crop_run(calib_vv, calib_vh, str(bronze_dir), bbox=jc.bbox_tuple)

    shutil.rmtree(calib_dir, ignore_errors=True)

    vv_rows, vv_cols = _raster_dims(crop_vv)
    bronze_vv_id = jc.meta.insert_data_product(
        scene_id=scene_id, job_id=crop_job_id, dataset_id=jc.dataset_id,
        product_tier="BRONZE", product_type="CROPPED_TIFF", band_name="VV",
        file_path=crop_vv, file_name=Path(crop_vv).name,
        file_size_mb=_file_size_mb(crop_vv), data_hash_sha256=jc.lineage.compute_sha256(crop_vv),
        rows=vv_rows, cols=vv_cols,
    )
    vh_rows, vh_cols = _raster_dims(crop_vh)
    bronze_vh_id = jc.meta.insert_data_product(
        scene_id=scene_id, job_id=crop_job_id, dataset_id=jc.dataset_id,
        product_tier="BRONZE", product_type="CROPPED_TIFF", band_name="VH",
        file_path=crop_vh, file_name=Path(crop_vh).name,
        file_size_mb=_file_size_mb(crop_vh), data_hash_sha256=jc.lineage.compute_sha256(crop_vh),
        rows=vh_rows, cols=vh_cols,
    )
    jc.lineage.record_transformation(raw_vv_id, bronze_vv_id, "CROP", crop_job_id, {"bbox": list(jc.bbox_tuple)})
    jc.lineage.record_transformation(raw_vh_id, bronze_vh_id, "CROP", crop_job_id, {"bbox": list(jc.bbox_tuple)})
    jc.meta.complete_job(crop_job_id)
    produced_tiers.append("BRONZE")
    jc.dsmgr.upsert_scene_job_state(jc.job_id, pid, current_stage="CROP", stage_status="COMPLETED")

    if "LEE_FILTER" in jc.skip_stages:
        return scene_id, produced_tiers
    jc.pause_event.wait()
    if jc.cancel_event.is_set():
        return scene_id, produced_tiers

    jc.dsmgr.upsert_scene_job_state(jc.job_id, pid, current_stage="LEE_FILTER", stage_status="RUNNING")
    lee_job_id = jc.meta.insert_processing_job(scene_id, "LEE_FILTER", parameters={"window_size": 7, "looks": 1})
    jc.meta.start_job(lee_job_id)

    silver_dir = jc.base_dir / "silver" / slug
    silver_dir.mkdir(parents=True, exist_ok=True)
    lee_vv, lee_vh = lee_run(crop_vv, crop_vh, str(silver_dir), window_size=7, looks=1)

    silver_vv_id = jc.meta.insert_data_product(
        scene_id=scene_id, job_id=lee_job_id, dataset_id=jc.dataset_id,
        product_tier="SILVER", product_type="LEE_FILTERED", band_name="VV",
        file_path=lee_vv, file_name=Path(lee_vv).name,
        file_size_mb=_file_size_mb(lee_vv), data_hash_sha256=jc.lineage.compute_sha256(lee_vv),
    )
    silver_vh_id = jc.meta.insert_data_product(
        scene_id=scene_id, job_id=lee_job_id, dataset_id=jc.dataset_id,
        product_tier="SILVER", product_type="LEE_FILTERED", band_name="VH",
        file_path=lee_vh, file_name=Path(lee_vh).name,
        file_size_mb=_file_size_mb(lee_vh), data_hash_sha256=jc.lineage.compute_sha256(lee_vh),
    )
    jc.lineage.record_transformation(bronze_vv_id, silver_vv_id, "LEE_FILTER", lee_job_id, {"window_size": 7, "looks": 1})
    jc.lineage.record_transformation(bronze_vh_id, silver_vh_id, "LEE_FILTER", lee_job_id, {"window_size": 7, "looks": 1})
    jc.meta.complete_job(lee_job_id)
    produced_tiers.append("SILVER")
    jc.dsmgr.upsert_scene_job_state(jc.job_id, pid, current_stage="LEE_FILTER", stage_status="COMPLETED")

    if "COG_EXPORT" in jc.skip_stages:
        return scene_id, produced_tiers
    jc.pause_event.wait()
    if jc.cancel_event.is_set():
        return scene_id, produced_tiers

    jc.dsmgr.upsert_scene_job_state(jc.job_id, pid, current_stage="COG_EXPORT", stage_status="RUNNING")
    cog_job_id = jc.meta.insert_processing_job(scene_id, "COG_EXPORT", parameters={"compression": "LZW", "blocksize": 512})
    jc.meta.start_job(cog_job_id)

    gold_dir = jc.base_dir / "gold" / slug
    gold_dir.mkdir(parents=True, exist_ok=True)
    cog_vv, cog_vh = cog_run(lee_vv, lee_vh, str(gold_dir), compression="LZW", blocksize=512)

    gold_vv_id = jc.meta.insert_data_product(
        scene_id=scene_id, job_id=cog_job_id, dataset_id=jc.dataset_id,
        product_tier="GOLD", product_type="COG", band_name="VV",
        file_path=cog_vv, file_name=Path(cog_vv).name,
        file_size_mb=_file_size_mb(cog_vv), data_hash_sha256=jc.lineage.compute_sha256(cog_vv),
        file_format="COG",
    )
    gold_vh_id = jc.meta.insert_data_product(
        scene_id=scene_id, job_id=cog_job_id, dataset_id=jc.dataset_id,
        product_tier="GOLD", product_type="COG", band_name="VH",
        file_path=cog_vh, file_name=Path(cog_vh).name,
        file_size_mb=_file_size_mb(cog_vh), data_hash_sha256=jc.lineage.compute_sha256(cog_vh),
        file_format="COG",
    )
    jc.lineage.record_transformation(silver_vv_id, gold_vv_id, "COG_EXPORT", cog_job_id, {"compression": "LZW", "blocksize": 512})
    jc.lineage.record_transformation(silver_vh_id, gold_vh_id, "COG_EXPORT", cog_job_id, {"compression": "LZW", "blocksize": 512})
    jc.meta.complete_job(cog_job_id)
    produced_tiers.append("GOLD")
    jc.dsmgr.upsert_scene_job_state(jc.job_id, pid, current_stage="COG_EXPORT", stage_status="COMPLETED")

    if "QUALITY_ANALYTICS" in jc.skip_stages:
        return scene_id, produced_tiers
    jc.pause_event.wait()
    if jc.cancel_event.is_set():
        return scene_id, produced_tiers

    jc.dsmgr.upsert_scene_job_state(jc.job_id, pid, current_stage="QUALITY_ANALYTICS", stage_status="RUNNING")
    qa_job_id = jc.meta.insert_processing_job(scene_id, "QUALITY_ANALYTICS", parameters={})
    jc.meta.start_job(qa_job_id)

    for band, path, product_id in (("VV", cog_vv, gold_vv_id), ("VH", cog_vh, gold_vh_id)):
        m = compute_band_metrics(path, band, min_quality_score=jc.min_quality_score)
        jc.meta.insert_quality_metrics(
            scene_id=scene_id, product_id=product_id, band_name=band,
            total_pixels=m.total_pixels, valid_pixels=m.valid_pixels, nodata_pixels=m.nodata_pixels,
            quality_score=m.quality_score, backscatter_mean_db=m.backscatter_mean_db,
            backscatter_std_db=m.backscatter_std_db, backscatter_min_db=m.backscatter_min_db,
            backscatter_max_db=m.backscatter_max_db, radiometric_consistency=m.radiometric_consistency,
            speckle_index=m.speckle_index, quality_flag=m.quality_flag,
        )
    jc.meta.complete_job(qa_job_id)
    jc.dsmgr.upsert_scene_job_state(jc.job_id, pid, current_stage="QUALITY_ANALYTICS", stage_status="COMPLETED")

    return scene_id, produced_tiers


def _cleanup_scene_tiers(jc: _JobContext, pid: str, scene_id: int, produced_tiers: list[str]) -> None:
    tiers_to_delete = compute_tiers_to_delete(produced_tiers, jc.required_tiers)
    if not tiers_to_delete:
        return
    slug = _slug(pid)
    tier_dirs = {
        "RAW": jc.base_dir / "raw" / slug,
        "BRONZE": jc.base_dir / "bronze" / slug,
        "SILVER": jc.base_dir / "silver" / slug,
        "GOLD": jc.base_dir / "gold" / slug,
    }
    for tier in tiers_to_delete:
        tier_dir = tier_dirs.get(tier)
        if tier_dir and tier_dir.exists():
            shutil.rmtree(tier_dir, ignore_errors=True)
        jc.meta.mark_products_invalid(scene_id=scene_id, dataset_id=jc.dataset_id, tier=tier)
    logger.info("[ORCH] cleanup pid=%s tiers dihapus=%s", pid, sorted(tiers_to_delete))


def _download_worker(jc: _JobContext, scenes: list[dict], download_queue: Queue) -> None:
    for scene_meta in scenes:
        jc.pause_event.wait()
        if jc.cancel_event.is_set():
            break
        pid = scene_meta["product_identifier"]
        state = jc.dsmgr.get_scene_job_state(jc.job_id, pid)
        if state and state["stage_status"] == "COMPLETED":
            continue
        try:
            jc.dsmgr.upsert_scene_job_state(
                jc.job_id, pid, current_stage="DOWNLOAD", stage_status="RUNNING", started_at=_now()
            )
            raw_dir = jc.base_dir / "raw" / _slug(pid)
            result = download_scene(scene_meta, output_dir=str(raw_dir), keep_raw=True)
            jc.dsmgr.increment_job_counters(jc.job_id, downloaded=1)
            download_queue.put((scene_meta, result))
        except Exception as exc:
            logger.exception("[ORCH] download gagal pid=%s job_id=%d", pid, jc.job_id)
            jc.dsmgr.upsert_scene_job_state(
                jc.job_id, pid, stage_status="FAILED", last_error=str(exc)[:2000], completed_at=_now()
            )
            jc.dsmgr.increment_job_counters(jc.job_id, failed=1)
    download_queue.put(None)


def _pipeline_worker(jc: _JobContext, download_queue: Queue, cleanup_queue: Queue) -> None:
    while True:
        item = download_queue.get()
        if item is None:
            break
        jc.pause_event.wait()
        if jc.cancel_event.is_set():
            break
        scene_meta, dl_result = item
        pid = scene_meta["product_identifier"]
        _pipeline_semaphore.acquire()
        try:
            scene_id, produced_tiers = _process_scene(jc, scene_meta, dl_result)
            jc.dsmgr.increment_job_counters(jc.job_id, processed=1)
            cleanup_queue.put((pid, scene_id, produced_tiers))
        except Exception as exc:
            logger.exception("[ORCH] pipeline gagal pid=%s job_id=%d", pid, jc.job_id)
            jc.dsmgr.upsert_scene_job_state(
                jc.job_id, pid, stage_status="FAILED", last_error=str(exc)[:2000], completed_at=_now()
            )
            jc.dsmgr.increment_job_counters(jc.job_id, failed=1)
        finally:
            _pipeline_semaphore.release()
    cleanup_queue.put(None)


def _cleanup_worker(jc: _JobContext, cleanup_queue: Queue) -> None:
    while True:
        item = cleanup_queue.get()
        if item is None:
            break
        pid, scene_id, produced_tiers = item
        try:
            _cleanup_scene_tiers(jc, pid, scene_id, produced_tiers)
            jc.dsmgr.upsert_scene_job_state(
                jc.job_id, pid, current_stage="CLEANUP", stage_status="COMPLETED", completed_at=_now()
            )
            jc.dsmgr.increment_job_counters(jc.job_id, cleaned=1)
        except Exception:
            logger.exception("[ORCH] cleanup gagal pid=%s job_id=%d", pid, jc.job_id)


def run_dataset_job(db: DatabaseClient, job_id: int) -> None:
    with db.session() as sess:
        job = sess.get(DatasetJob, job_id)
        if job is None:
            logger.error("[ORCH] job_id=%d tidak ditemukan", job_id)
            return
        dataset_id = job.dataset_id
        date_range_start = job.date_range_start
        date_range_end = job.date_range_end

    dsmgr = DatasetManager(db)
    meta = MetadataManager(db)
    lineage = LineageTracker(db)

    dataset = dsmgr.get_dataset(dataset_id)
    if dataset is None:
        logger.error("[ORCH] dataset_id=%d tidak ditemukan", dataset_id)
        dsmgr.set_job_status(job_id, "FAILED", completed_at=_now())
        return

    required_tiers = dataset["required_tiers"]
    region_id = dataset["region_id"]
    bbox_wkt = dataset["bbox_wkt"]
    quality_settings = dataset["quality_settings"] or {}
    min_quality_score = float(quality_settings.get("min_quality_score") or 60.0)
    min_cloud_cover = quality_settings.get("min_cloud_cover")
    skip_stages = compute_skip_stages(required_tiers)
    bbox_tuple = _bbox_tuple_from_wkt(bbox_wkt)
    base_dir = _dataset_base_dir(dataset_id)
    base_dir.mkdir(parents=True, exist_ok=True)

    pause_event = get_pause_event(job_id)
    cancel_event = get_cancel_event(job_id)

    jc = _JobContext(
        db=db, dsmgr=dsmgr, meta=meta, lineage=lineage,
        job_id=job_id, dataset_id=dataset_id, region_id=region_id,
        bbox_wkt=bbox_wkt, bbox_tuple=bbox_tuple,
        required_tiers=required_tiers, skip_stages=skip_stages,
        min_quality_score=min_quality_score,
        base_dir=base_dir, pause_event=pause_event, cancel_event=cancel_event,
    )

    dsmgr.set_job_status(job_id, "PREPARING", started_at=_now())

    date_from = (
        datetime.combine(date_range_start, datetime.min.time(), tzinfo=timezone.utc)
        if date_range_start else _now() - timedelta(days=30)
    )
    date_to = (
        datetime.combine(date_range_end, datetime.min.time(), tzinfo=timezone.utc) + timedelta(days=1)
        if date_range_end else _now()
    )

    try:
        scenes = discover_scenes(bbox_wkt=bbox_wkt, date_from=date_from, date_to=date_to, max_results=200)
    except Exception:
        logger.exception("[ORCH] discovery gagal job_id=%d", job_id)
        dsmgr.set_job_status(job_id, "FAILED", completed_at=_now())
        return

    if not scenes:
        logger.info("[ORCH] tidak ada scene ditemukan job_id=%d", job_id)
        dsmgr.set_job_status(job_id, "COMPLETED", completed_at=_now())
        return

    if min_cloud_cover is not None:
        before = len(scenes)
        scenes = [s for s in scenes if (s.get("cloud_cover") or 0) <= min_cloud_cover]
        logger.info("[ORCH] filter cloud_cover<=%.1f job_id=%d: %d -> %d scene",
                    min_cloud_cover, job_id, before, len(scenes))
        if not scenes:
            logger.info("[ORCH] semua scene tersaring cloud_cover job_id=%d", job_id)
            dsmgr.set_job_status(job_id, "COMPLETED", completed_at=_now())
            return

    dsmgr.create_scene_job_states(job_id, [s["product_identifier"] for s in scenes])
    dsmgr.set_job_status(job_id, "DOWNLOADING")

    download_queue: Queue = Queue(maxsize=3)
    cleanup_queue: Queue = Queue()

    t_download = threading.Thread(target=_download_worker, args=(jc, scenes, download_queue), daemon=False)
    t_pipeline = threading.Thread(target=_pipeline_worker, args=(jc, download_queue, cleanup_queue), daemon=False)
    t_cleanup = threading.Thread(target=_cleanup_worker, args=(jc, cleanup_queue), daemon=False)

    t_download.start()
    t_pipeline.start()
    t_cleanup.start()
    t_download.join()
    t_pipeline.join()
    t_cleanup.join()

    total_size = _dir_size_bytes(base_dir)
    dsmgr.set_dataset_size(dataset_id, total_size)

    if cancel_event.is_set():
        dsmgr.set_job_status(job_id, "CANCELLED", completed_at=_now())
        logger.info("[ORCH] job_id=%d dibatalkan", job_id)
        return

    with db.session() as sess:
        job = sess.get(DatasetJob, job_id)
        still_paused = job.status == "PAUSED" if job else False

    if still_paused:
        logger.info("[ORCH] job_id=%d berhenti dalam status PAUSED", job_id)
        return

    dsmgr.set_job_status(job_id, "COMPLETED", completed_at=_now())
    logger.info("[ORCH] job_id=%d selesai", job_id)
