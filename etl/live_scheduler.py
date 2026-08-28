# etl/live_scheduler.py
from __future__ import annotations
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from sqlalchemy import select
from etl.constants import (
    GPM_PRODUCT_SHORT_NAME,
    GPM_SOURCE,
    GPM_TILE_ID,
    MODIS_PRODUCT_SHORT_NAME,
    MODIS_SOURCE,
    MODIS_TILE_ID,
)
from etl.database_client import DatabaseClient, Dataset, DatasetJob, LiveDatasetSource
from etl.dataset_manager import DatasetManager
from etl.lineage_tracker import LineageTracker
from etl.metadata_manager import MetadataManager
from etl.module1_download import discover_scenes
from etl.module5_orchestrator import run_dataset_job
from etl.module7_modis_download import download_modis_scene
from etl.module8_gpm_download import download_gpm_scene

logger = logging.getLogger(__name__)

_CHECK_HOUR = 2
_CHECK_MINUTE = 0
_TIMEZONE = "Asia/Jakarta"
_DEFAULT_LOOKBACK_DAYS = 7


def _now() -> datetime:
    return datetime.now(timezone.utc)


class LiveScheduler:
    def __init__(self, db: DatabaseClient) -> None:
        self._db = db
        self._dsmgr = DatasetManager(db)
        self._scheduler = None

    def start(self) -> None:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger

        self._scheduler = BackgroundScheduler(timezone=_TIMEZONE)
        self._scheduler.add_job(
            func=self.run_daily_check,
            trigger=CronTrigger(hour=_CHECK_HOUR, minute=_CHECK_MINUTE),
            id="live_dataset_daily_check",
            name="Live Dataset Daily Check",
            replace_existing=True,
        )
        self._scheduler.start()
        logger.info("[LIVE] scheduler dimulai, cek harian jam %02d:%02d %s",
                    _CHECK_HOUR, _CHECK_MINUTE, _TIMEZONE)

    def shutdown(self) -> None:
        if self._scheduler:
            self._scheduler.shutdown(wait=False)
            logger.info("[LIVE] scheduler dihentikan")

    def run_daily_check(self) -> dict:
        live = self._dsmgr.get_live_dataset()
        if live is None:
            logger.warning("[LIVE] dataset live belum ada, skip check")
            return {"checked": False, "reason": "dataset_live_not_found"}
        if not live["live_enabled"]:
            logger.info("[LIVE] live dataset nonaktif, skip check")
            return {"checked": False, "reason": "disabled"}

        with self._db.session() as sess:
            sources = sess.scalars(
                select(LiveDatasetSource).where(LiveDatasetSource.enabled == True)
            ).all()
            source_rows = [
                {
                    "source_name": s.source_name,
                    "last_check": s.last_check,
                    "last_ingest": s.last_ingest,
                    "source_config": s.source_config or {},
                }
                for s in sources
            ]

        results: dict[str, bool] = {}
        for source in source_rows:
            try:
                results[source["source_name"]] = self._check_and_ingest_source(live, source)
            except Exception:
                logger.exception("[LIVE] gagal cek sumber %s", source["source_name"])
                results[source["source_name"]] = False

        with self._db.session() as sess:
            dataset = sess.get(Dataset, live["dataset_id"])
            if dataset:
                dataset.live_last_checked_at = _now()

        if not any(results.values()):
            logger.info("[LIVE] tidak ada data baru dari semua sumber")

        return {"checked": True, "sources": results}

    def _check_and_ingest_source(self, live: dict, source: dict) -> bool:
        name = source["source_name"]
        if name == "SENTINEL1":
            return self._check_and_ingest_sentinel1(live, source)
        if name == "MODIS":
            return self._check_and_ingest_modis(live, source)
        if name == "GPM":
            return self._check_and_ingest_gpm(live, source)
        logger.warning("[LIVE] sumber tidak dikenal: %s", name)
        return False

    def _update_source_check(self, source_name: str, **fields) -> None:
        with self._db.session() as sess:
            source = sess.scalar(
                select(LiveDatasetSource).where(LiveDatasetSource.source_name == source_name)
            )
            if source is None:
                return
            for k, v in fields.items():
                setattr(source, k, v)

    def _create_live_job(self, dataset_id: int, date_start: date, date_end: date, job_type: str) -> int:
        with self._db.session() as sess:
            job = DatasetJob(
                dataset_id=dataset_id,
                job_type=job_type,
                status="QUEUED",
                date_range_start=date_start,
                date_range_end=date_end,
            )
            sess.add(job)
            sess.flush()
            return job.job_id

    def _check_and_ingest_sentinel1(self, live: dict, source: dict) -> bool:
        since = source["last_ingest"] or (_now() - timedelta(days=_DEFAULT_LOOKBACK_DAYS))
        date_to = _now()
        try:
            scenes = discover_scenes(bbox_wkt=live["bbox_wkt"], date_from=since, date_to=date_to, max_results=200)
        except Exception:
            logger.exception("[LIVE] Sentinel-1: gagal cek ketersediaan")
            self._update_source_check("SENTINEL1", last_check=_now())
            return False

        self._update_source_check("SENTINEL1", last_check=_now())

        if not scenes:
            logger.info("[LIVE] Sentinel-1: tidak ada scene baru")
            return False

        logger.info("[LIVE] Sentinel-1: %d scene baru ditemukan, memulai ingest", len(scenes))
        job_id = self._create_live_job(live["dataset_id"], since.date(), date_to.date(), "LIVE_INGEST")
        run_dataset_job(self._db, job_id)
        self._update_source_check("SENTINEL1", last_ingest=_now())
        self._build_fusion_stacks(live, scenes)
        return True

    def _build_fusion_stacks(self, live: dict, scenes: list[dict]) -> None:
        from etl.module9_fusion import create_fusion_stack

        aoi_bbox = self._bbox_tuple(live["bbox_wkt"])
        for scene in scenes:
            s1_date = scene["acquisition_datetime"].date()
            try:
                fusion_id = create_fusion_stack(
                    dataset_id=live["dataset_id"],
                    s1_date=s1_date,
                    aoi_bbox=aoi_bbox,
                    db=self._db,
                )
                logger.info("[LIVE] Fusion stack dibuat fusion_id=%d tanggal=%s", fusion_id, s1_date)
            except Exception:
                logger.exception("[LIVE] Fusion stack gagal untuk tanggal=%s", s1_date)

    def _check_and_ingest_modis(self, live: dict, source: dict) -> bool:
        dataset_id = live["dataset_id"]
        today = _now()
        since = source["last_ingest"] or (today - timedelta(days=_DEFAULT_LOOKBACK_DAYS))
        bbox_tuple = self._bbox_tuple(live["bbox_wkt"])

        self._update_source_check("MODIS", last_check=_now())

        try:
            _product_id, metadata = download_modis_scene(dataset_id, since, today, bbox_tuple)
        except NotImplementedError:
            logger.info("[LIVE] MODIS: download belum diimplementasikan, skip")
            return False
        except Exception:
            logger.exception("[LIVE] MODIS: gagal download/proses")
            return False

        meta = MetadataManager(self._db)
        lineage = LineageTracker(self._db)
        self._create_live_job(dataset_id, since.date(), today.date(), "LIVE_INGEST")
        scene_id = self._resolve_placeholder_scene(dataset_id, live["region_id"])
        processing_job_id = meta.insert_processing_job(
            scene_id, "DOWNLOAD", parameters={"dataset_id": dataset_id, "source": "MODIS"}
        )
        registered = 0
        for output in metadata["outputs"]:
            try:
                flood_tif = output["flood_path"]
                meta.insert_nasa_scene(
                    source=MODIS_SOURCE,
                    tile_id=MODIS_TILE_ID,
                    product_short_name=MODIS_PRODUCT_SHORT_NAME,
                    acquisition_date=date.fromisoformat(output["date"]),
                    region_id=live["region_id"],
                    raw_file_path=flood_tif,
                    download_url=None,
                )
                meta.insert_data_product(
                    scene_id=scene_id,
                    job_id=processing_job_id,
                    dataset_id=dataset_id,
                    product_tier="GOLD",
                    product_type="MODIS_FLOOD",
                    band_name="FLOOD",
                    file_path=flood_tif,
                    file_name=Path(flood_tif).name,
                    file_size_mb=round(Path(flood_tif).stat().st_size / (1024 ** 2), 3),
                    data_hash_sha256=lineage.compute_sha256(flood_tif),
                )
                registered += 1
            except Exception:
                logger.exception("[LIVE] MODIS: gagal registrasi produk tanggal=%s", output.get("date"))
        if registered:
            self._update_source_check("MODIS", last_ingest=_now())
        return registered > 0

    def _check_and_ingest_gpm(self, live: dict, source: dict) -> bool:
        today = _now()
        meta = MetadataManager(self._db)
        try:
            nasa_scene_id = meta.get_nasa_scene(GPM_SOURCE, GPM_TILE_ID, GPM_PRODUCT_SHORT_NAME, today.date())
        except Exception:
            nasa_scene_id = None

        self._update_source_check("GPM", last_check=_now())

        if nasa_scene_id:
            logger.info("[LIVE] GPM: data hari ini sudah ada")
            return False

        dataset_id = live["dataset_id"]
        bbox_tuple = self._bbox_tuple(live["bbox_wkt"])

        try:
            _product_ids, metadata = download_gpm_scene(dataset_id, today, bbox_tuple)
        except NotImplementedError:
            logger.info("[LIVE] GPM: download belum diimplementasikan, skip")
            return False
        except Exception:
            logger.exception("[LIVE] GPM: gagal download/proses")
            return False

        lineage = LineageTracker(self._db)
        self._create_live_job(dataset_id, today.date(), today.date(), "LIVE_INGEST")
        scene_id = self._resolve_placeholder_scene(dataset_id, live["region_id"])
        processing_job_id = meta.insert_processing_job(
            scene_id, "DOWNLOAD", parameters={"dataset_id": dataset_id, "source": "GPM"}
        )
        registered = 0
        for window_name, output in metadata["windows"].items():
            try:
                tif_path = output["path"]
                meta.insert_nasa_scene(
                    source=GPM_SOURCE,
                    tile_id=GPM_TILE_ID,
                    product_short_name=GPM_PRODUCT_SHORT_NAME,
                    acquisition_date=today.date(),
                    region_id=live["region_id"],
                    raw_file_path=tif_path,
                    download_url=None,
                )
                meta.insert_data_product(
                    scene_id=scene_id,
                    job_id=processing_job_id,
                    dataset_id=dataset_id,
                    product_tier="GOLD",
                    product_type="GPM_RAINFALL",
                    band_name=f"RAIN_{window_name.upper()}",
                    file_path=tif_path,
                    file_name=Path(tif_path).name,
                    file_size_mb=round(Path(tif_path).stat().st_size / (1024 ** 2), 3),
                    data_hash_sha256=lineage.compute_sha256(tif_path),
                )
                registered += 1
            except Exception:
                logger.exception("[LIVE] GPM: gagal registrasi produk window=%s", window_name)

        if registered:
            self._update_source_check("GPM", last_ingest=_now())
        return registered > 0

    @staticmethod
    def _bbox_tuple(bbox_wkt: str) -> tuple[float, float, float, float]:
        from shapely import wkt as shapely_wkt

        return shapely_wkt.loads(bbox_wkt).bounds

    def _resolve_placeholder_scene(self, dataset_id: int, region_id: int) -> int:
        meta = MetadataManager(self._db)
        placeholder_pid = f"NASA_AUX_DATASET_{dataset_id}"
        existing = meta.get_scene_by_pid(placeholder_pid)
        if existing:
            return existing["scene_id"]
        live = self._dsmgr.get_dataset(dataset_id)
        return meta.insert_satellite_scene(
            product_identifier=placeholder_pid,
            acquisition_datetime=_now(),
            region_id=region_id,
            bbox_wkt=live["bbox_wkt"] if live else "POLYGON((0 0,0 0,0 0,0 0,0 0))",
            orbit_direction="ASCENDING",
            polarization_vv=False,
            polarization_vh=False,
            resolution_m=250,
            instrument_mode="AUX",
        )

    def handle_backfill_request(self, date_start: date, date_end: date) -> dict:
        return self._dsmgr.trigger_live_backfill(date_start, date_end)