# etl/scheduler.py
from __future__ import annotations

import json
import logging
import signal
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs_pipeline/scheduler.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("scheduler")


def _check_apscheduler() -> bool:
    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
        return True
    except ImportError:
        logger.error("APScheduler tidak terinstall. Jalankan: pip install apscheduler")
        return False


def load_pipeline_config() -> dict:
    cfg_path = Path("config/config.json")
    if cfg_path.exists():
        with open(cfg_path) as f:
            cfg = json.load(f)
    else:
        cfg = {}

    pipeline = cfg.get("pipeline", {})
    area = cfg.get("area", {})

    return {
        "bbox_wkt": area.get(
            "bbox_wkt",
            "POLYGON((106.2948 -6.3724, 106.9734 -6.3724, 106.9734 -5.6024, 106.2948 -5.6024, 106.2948 -6.3724))",
        ),
        "orbit_direction": None,
        "days_back": area.get("days_back", 90),
        "keep_raw": pipeline.get("keep_raw", False),
        "keep_bronze": pipeline.get("keep_bronze", True),
        "keep_silver": pipeline.get("keep_silver", True),
        "output_dir": pipeline.get("output_dir", "processed"),
        "raw_dir": pipeline.get("raw_dir", "recovered_temp"),
        "lee_window_size": pipeline.get("lee_window_size", 7),
        "cog_compression": pipeline.get("cog_compression", "LZW"),
        "cog_blocksize": pipeline.get("cog_blocksize", 512),
        "min_quality_score": cfg.get("quality", {}).get("min_quality_score", 60),
        "check_interval_hours": pipeline.get("check_interval_hours", 12),
        "max_scenes_per_run": pipeline.get("max_scenes_per_run", 20),
    }


def run_pipeline_once(cfg: dict) -> dict:
    from etl.database_client import DatabaseClient
    from etl.metadata_manager import MetadataManager
    from etl.module5_orchestrator import PipelineOrchestrator, SceneContext
    import etl.module1_download as m1

    db = DatabaseClient.from_env()
    meta = MetadataManager(db)
    orch = PipelineOrchestrator(db, output_dir=cfg["output_dir"])

    date_to = datetime.now(tz=timezone.utc)
    date_from = date_to - timedelta(days=cfg["days_back"])

    logger.info("pipeline run: %s -> %s", date_from.date(), date_to.date())

    summary = {
        "started_at": date_to.isoformat(),
        "scenes_found": 0,
        "scenes_processed": 0,
        "scenes_failed": 0,
        "scenes_skipped": 0,
        "storage_freed_mb": 0.0,
    }

    keep_raw = True
    if not cfg.get("keep_raw", False):
        logger.warning(
            "keep_raw dipaksa True untuk run ini — module1b_calibrate butuh file SAFE/zip asli, "
            "cleanup manual bisa dilakukan lewat /api/storage/cleanup setelah CROP selesai."
        )

    try:
        scenes = m1.discover_scenes(
            bbox_wkt=cfg["bbox_wkt"],
            date_from=date_from,
            date_to=date_to,
            orbit_direction=cfg.get("orbit_direction"),
            max_results=cfg["max_scenes_per_run"],
        )
        summary["scenes_found"] = len(scenes)
        logger.info("ditemukan %d scene", len(scenes))

        if not scenes:
            logger.info("tidak ada scene baru")
            return summary

        for scene_meta in scenes:
            pid = scene_meta["product_identifier"]

            existing = meta.get_scene_by_pid(pid)
            if existing and existing.get("has_gold"):
                logger.info("skip %s — sudah ada GOLD product", pid[:40])
                summary["scenes_skipped"] += 1
                continue

            try:
                result = m1.download_scene(
                    scene_meta=scene_meta,
                    output_dir=cfg["raw_dir"],
                    keep_raw=keep_raw,
                )

                scene_id = meta.insert_satellite_scene(
                    product_identifier=result.product_identifier,
                    acquisition_datetime=result.acquisition_datetime,
                    region_id=1,
                    bbox_wkt=cfg["bbox_wkt"],
                    orbit_direction=result.orbit_direction,
                    orbit_number=result.orbit_number,
                    relative_orbit=result.relative_orbit,
                    cloud_cover_percent=result.cloud_cover,
                    raw_file_path=result.zip_path if result.kept_raw else None,
                    raw_file_size_mb=result.file_size_mb,
                    download_url=result.download_url,
                    checksum_md5=result.checksum_md5,
                )

                ctx = SceneContext(
                    scene_id=scene_id,
                    product_identifier=result.product_identifier,
                    region_id=1,
                    raw_file_path=result.zip_path,
                    raw_vv_path=result.vv_tif_path,
                    raw_vh_path=result.vh_tif_path,
                )
                final = orch.run(ctx)

                if final.failed_stage:
                    summary["scenes_failed"] += 1
                else:
                    summary["scenes_processed"] += 1
                    freed = cleanup_storage(result, cfg)
                    summary["storage_freed_mb"] += freed

            except Exception as exc:
                logger.error("error pada scene %s: %s", pid[:40], exc, exc_info=True)
                summary["scenes_failed"] += 1

    except Exception as exc:
        logger.error("pipeline run gagal: %s", exc, exc_info=True)
    finally:
        db.dispose()

    logger.info(
        "run selesai: found=%d processed=%d failed=%d skipped=%d freed=%.1fMB",
        summary["scenes_found"], summary["scenes_processed"],
        summary["scenes_failed"], summary["scenes_skipped"],
        summary["storage_freed_mb"],
    )
    return summary


def cleanup_storage(result, cfg: dict) -> float:
    freed_mb = 0.0
    out_dir = Path(cfg["output_dir"])

    if not cfg.get("keep_bronze", True):
        bronze_dir = out_dir / "bronze"
        for f in bronze_dir.rglob("*.tif"):
            if f.exists():
                freed_mb += f.stat().st_size / (1024 ** 2)
                f.unlink()

    if not cfg.get("keep_silver", True):
        silver_dir = out_dir / "silver"
        for f in silver_dir.rglob("*.tif"):
            if f.exists():
                freed_mb += f.stat().st_size / (1024 ** 2)
                f.unlink()

    if freed_mb > 0:
        logger.info("cleanup: %.1f MB dibebaskan", freed_mb)
    return freed_mb


def get_storage_summary(cfg: dict) -> dict:
    out = Path(cfg["output_dir"])
    raw = Path(cfg["raw_dir"])

    def dir_size_mb(d: Path) -> float:
        if not d.exists():
            return 0.0
        return sum(f.stat().st_size for f in d.rglob("*") if f.is_file()) / (1024 ** 2)

    return {
        "raw_mb": dir_size_mb(raw),
        "bronze_mb": dir_size_mb(out / "bronze"),
        "silver_mb": dir_size_mb(out / "silver"),
        "gold_mb": dir_size_mb(out / "gold"),
        "total_mb": dir_size_mb(raw) + dir_size_mb(out),
    }


class Scheduler:
    def __init__(self) -> None:
        self.cfg = load_pipeline_config()
        self._running = False

    def run_now(self) -> dict:
        return run_pipeline_once(self.cfg)

    def start(self) -> None:
        if not _check_apscheduler():
            sys.exit(1)

        from apscheduler.schedulers.blocking import BlockingScheduler
        from apscheduler.triggers.interval import IntervalTrigger

        interval_hours = self.cfg["check_interval_hours"]
        sched = BlockingScheduler(timezone="Asia/Jakarta")
        sched.add_job(
            func=lambda: run_pipeline_once(self.cfg),
            trigger=IntervalTrigger(hours=interval_hours),
            id="sentinel1_pipeline",
            name="Sentinel-1 Auto Pipeline",
            next_run_time=datetime.now(),
        )

        logger.info(
            "scheduler dimulai — interval=%dh area=%s... days_back=%d keep_raw(forced)=True",
            interval_hours, self.cfg["bbox_wkt"][:50], self.cfg["days_back"],
        )

        def _stop(signum, frame):
            logger.info("interrupt diterima, menghentikan scheduler")
            sched.shutdown(wait=False)
            sys.exit(0)

        signal.signal(signal.SIGINT, _stop)
        signal.signal(signal.SIGTERM, _stop)

        try:
            sched.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("scheduler dihentikan")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Sentinel-1 Pipeline Scheduler")
    parser.add_argument("--run-now", action="store_true")
    parser.add_argument("--storage", action="store_true")
    args = parser.parse_args()

    cfg = load_pipeline_config()

    if args.storage:
        s = get_storage_summary(cfg)
        print("\n=== Storage Summary ===")
        print(f"  RAW (ZIP/TIF asli) : {s['raw_mb']:.1f} MB")
        print(f"  BRONZE (crop)      : {s['bronze_mb']:.1f} MB")
        print(f"  SILVER (Lee filter): {s['silver_mb']:.1f} MB")
        print(f"  GOLD (COG)         : {s['gold_mb']:.1f} MB")
        print(f"  TOTAL              : {s['total_mb']:.1f} MB")
        return

    if args.run_now:
        sched = Scheduler()
        result = sched.run_now()
        print("\n=== Run Summary ===")
        for k, v in result.items():
            print(f"  {k}: {v}")
        return

    Scheduler().start()


if __name__ == "__main__":
    main()