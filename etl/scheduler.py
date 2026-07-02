# etl/scheduler.py
"""
Scheduler 24/7 untuk pipeline Sentinel-1 otomatis.
Menggunakan APScheduler untuk cek data baru setiap N jam.

Cara jalankan (terminal baru, venv aktif):
    python -m etl.scheduler

Atau sebagai service Windows:
    python -m etl.scheduler --install-service   (belum diimplementasi)

Author : Julius Marselinus (BRONTO) - NIM 00000111989
Program: Sistem Informasi - Universitas Multimedia Nusantara
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

# Setup logging sebelum import lain
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs_pipeline/scheduler.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("scheduler")


def _check_apscheduler():
    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
        return True
    except ImportError:
        logger.error(
            "APScheduler tidak terinstall.\n"
            "Jalankan: pip install apscheduler\n"
            "Lalu tambahkan 'apscheduler>=3.10.0' ke requirements.txt"
        )
        return False


def load_pipeline_config() -> dict:
    """Load konfigurasi pipeline dari config/config.json dan .env."""
    cfg_path = Path("config/config.json")
    if cfg_path.exists():
        with open(cfg_path) as f:
            cfg = json.load(f)
    else:
        cfg = {}

    # Merge dengan nilai default
    pipeline = cfg.get("pipeline", {})
    area     = cfg.get("area", {})

    return {
        # Area
        "bbox_wkt": area.get("bbox_wkt",
            "POLYGON((106.2948 -6.3724, 106.9734 -6.3724, 106.9734 -5.6024, 106.2948 -5.6024, 106.2948 -6.3724))"
        ),
        "orbit_direction": None,  # None = ambil keduanya

        # Waktu
        "days_back": area.get("days_back", 90),

        # Storage
        "keep_raw":         pipeline.get("keep_raw",         False),  # Hapus ZIP setelah extract
        "keep_bronze":      pipeline.get("keep_bronze",      True),   # Simpan hasil crop
        "keep_silver":      pipeline.get("keep_silver",      True),   # Simpan Lee filtered
        "output_dir":       pipeline.get("output_dir",       "processed"),
        "raw_dir":          pipeline.get("raw_dir",          "recovered_temp"),

        # Pipeline params
        "lee_window_size":  pipeline.get("lee_window_size",  7),
        "cog_compression":  pipeline.get("cog_compression",  "LZW"),
        "cog_blocksize":    pipeline.get("cog_blocksize",    512),

        # Quality
        "min_quality_score": cfg.get("quality", {}).get("min_quality_score", 60),

        # Scheduler
        "check_interval_hours": pipeline.get("check_interval_hours", 12),  # cek tiap 12 jam
        "max_scenes_per_run":   pipeline.get("max_scenes_per_run",   20),
    }


def run_pipeline_once(cfg: dict) -> dict:
    """
    Jalankan satu siklus penuh pipeline:
    1. Discover scene baru
    2. Download
    3. Crop → Lee Filter → COG Export
    4. Quality Analytics
    5. Cleanup storage

    Returns:
        Summary dict: {scenes_found, scenes_processed, scenes_failed, storage_freed_mb}
    """
    from etl.database_client import DatabaseClient
    from etl.metadata_manager import MetadataManager
    from etl.module5_orchestrator import PipelineOrchestrator, SceneContext
    import etl.module1_download as m1

    db   = DatabaseClient.from_env()
    meta = MetadataManager(db)
    orch = PipelineOrchestrator(db, output_dir=cfg["output_dir"])

    date_to   = datetime.now(tz=timezone.utc)
    date_from = date_to - timedelta(days=cfg["days_back"])

    logger.info("═══ Pipeline run: %s → %s ═══", date_from.date(), date_to.date())

    summary = {
        "started_at":       date_to.isoformat(),
        "scenes_found":     0,
        "scenes_processed": 0,
        "scenes_failed":    0,
        "scenes_skipped":   0,
        "storage_freed_mb": 0.0,
    }

    try:
        # ── Step 1: Discover scene baru ──────────────────────────────────
        scenes = m1.discover_scenes(
            bbox_wkt        = cfg["bbox_wkt"],
            date_from       = date_from,
            date_to         = date_to,
            orbit_direction = cfg.get("orbit_direction"),
            max_results     = cfg["max_scenes_per_run"],
        )
        summary["scenes_found"] = len(scenes)
        logger.info("[SCHED] Ditemukan %d scene.", len(scenes))

        if not scenes:
            logger.info("[SCHED] Tidak ada scene baru. Tidur sampai jadwal berikutnya.")
            return summary

        # ── Step 2–6: Per scene ──────────────────────────────────────────
        for scene_meta in scenes:
            pid = scene_meta["product_identifier"]

            # Cek apakah sudah diproses sebelumnya (idempotent)
            existing = meta.get_scene_by_pid(pid)
            if existing and existing.get("has_gold"):
                logger.info("[SCHED] Skip %s — sudah ada GOLD product.", pid[:40])
                summary["scenes_skipped"] += 1
                continue

            try:
                # Download
                result = m1.download_scene(
                    scene_meta = scene_meta,
                    output_dir = cfg["raw_dir"],
                    keep_raw   = cfg["keep_raw"],
                )

                # Register scene di DB
                scene_id = meta.insert_satellite_scene(
                    product_identifier   = result.product_identifier,
                    acquisition_datetime = result.acquisition_datetime,
                    region_id            = 1,  # default Jabodetabek
                    bbox_wkt             = cfg["bbox_wkt"],
                    orbit_direction      = result.orbit_direction,
                    orbit_number         = result.orbit_number,
                    relative_orbit       = result.relative_orbit,
                    cloud_cover_percent  = result.cloud_cover,
                    raw_file_path        = result.zip_path if result.kept_raw else None,
                    raw_file_size_mb     = result.file_size_mb,
                    download_url         = result.download_url,
                    checksum_md5         = result.checksum_md5,
                )

                # Jalankan pipeline M2–M6
                ctx = SceneContext(
                    scene_id           = scene_id,
                    product_identifier = result.product_identifier,
                    region_id          = 1,
                    raw_file_path      = result.vv_tif_path,
                    raw_vv_path        = result.vv_tif_path,
                    raw_vh_path        = result.vh_tif_path,
                )

                final = orch.run(ctx)

                if final.failed_stage:
                    summary["scenes_failed"] += 1
                else:
                    summary["scenes_processed"] += 1

                    # Cleanup storage sesuai policy
                    freed = cleanup_storage(result, cfg)
                    summary["storage_freed_mb"] += freed

            except Exception as exc:
                logger.error("[SCHED] Error pada scene %s: %s", pid[:40], exc, exc_info=True)
                summary["scenes_failed"] += 1

    except Exception as exc:
        logger.error("[SCHED] Pipeline run gagal: %s", exc, exc_info=True)
    finally:
        db.dispose()

    logger.info(
        "═══ Run selesai: found=%d processed=%d failed=%d skipped=%d freed=%.1fMB ═══",
        summary["scenes_found"], summary["scenes_processed"],
        summary["scenes_failed"], summary["scenes_skipped"],
        summary["storage_freed_mb"],
    )
    return summary


def cleanup_storage(result, cfg: dict) -> float:
    """
    Hapus file intermediate sesuai retention policy.

    Policy default:
        keep_raw    = False → hapus ZIP setelah ekstrak
        keep_bronze = True  → simpan crop result
        keep_silver = True  → simpan Lee filtered

    Returns:
        MB yang dibebaskan
    """
    freed_mb = 0.0
    out_dir  = Path(cfg["output_dir"])

    # ZIP sudah dihapus oleh module1 jika keep_raw=False
    # Bronze files (jika tidak mau disimpan)
    if not cfg.get("keep_bronze", True):
        bronze_dir = out_dir / "bronze"
        for f in bronze_dir.rglob("*.tif"):
            if f.exists():
                freed_mb += f.stat().st_size / (1024 ** 2)
                f.unlink()
                logger.debug("[CLEANUP] Hapus bronze: %s", f.name)

    # Silver files (jika tidak mau disimpan)
    if not cfg.get("keep_silver", True):
        silver_dir = out_dir / "silver"
        for f in silver_dir.rglob("*.tif"):
            if f.exists():
                freed_mb += f.stat().st_size / (1024 ** 2)
                f.unlink()
                logger.debug("[CLEANUP] Hapus silver: %s", f.name)

    if freed_mb > 0:
        logger.info("[CLEANUP] %.1f MB dibebaskan.", freed_mb)

    return freed_mb


def get_storage_summary(cfg: dict) -> dict:
    """Hitung penggunaan storage saat ini per tier."""
    out = Path(cfg["output_dir"])
    raw = Path(cfg["raw_dir"])

    def dir_size_mb(d: Path) -> float:
        if not d.exists():
            return 0.0
        return sum(f.stat().st_size for f in d.rglob("*") if f.is_file()) / (1024 ** 2)

    return {
        "raw_mb":    dir_size_mb(raw),
        "bronze_mb": dir_size_mb(out / "bronze"),
        "silver_mb": dir_size_mb(out / "silver"),
        "gold_mb":   dir_size_mb(out / "gold"),
        "total_mb":  dir_size_mb(raw) + dir_size_mb(out),
    }


class Scheduler:
    """
    24/7 pipeline scheduler.

    Menjalankan pipeline otomatis setiap N jam.
    Juga bisa trigger manual via run_now().
    """

    def __init__(self):
        self.cfg      = load_pipeline_config()
        self._running = False

    def run_now(self) -> dict:
        """Trigger pipeline run manual (untuk testing)."""
        logger.info("[SCHED] Manual trigger pipeline run.")
        return run_pipeline_once(self.cfg)

    def start(self):
        """Mulai scheduler. Blokir sampai di-interrupt (Ctrl+C)."""
        if not _check_apscheduler():
            logger.error("Install APScheduler dulu: pip install apscheduler")
            sys.exit(1)

        from apscheduler.schedulers.blocking import BlockingScheduler
        from apscheduler.triggers.interval   import IntervalTrigger

        interval_hours = self.cfg["check_interval_hours"]

        sched = BlockingScheduler(timezone="Asia/Jakarta")
        sched.add_job(
            func    = lambda: run_pipeline_once(self.cfg),
            trigger = IntervalTrigger(hours=interval_hours),
            id      = "sentinel1_pipeline",
            name    = "Sentinel-1 Auto Pipeline",
            # Jalankan sekali saat pertama start
            next_run_time = datetime.now(),
        )

        logger.info(
            "═══ Scheduler dimulai ═══\n"
            "  Interval : setiap %d jam\n"
            "  Area     : %s...\n"
            "  Hari ke belakang: %d\n"
            "  Keep raw : %s\n"
            "  Ctrl+C untuk berhenti.",
            interval_hours,
            self.cfg["bbox_wkt"][:50],
            self.cfg["days_back"],
            self.cfg["keep_raw"],
        )

        # Handle Ctrl+C dengan bersih
        def _stop(signum, frame):
            logger.info("[SCHED] Interrupt diterima. Menghentikan scheduler...")
            sched.shutdown(wait=False)
            sys.exit(0)

        signal.signal(signal.SIGINT, _stop)
        signal.signal(signal.SIGTERM, _stop)

        try:
            sched.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("[SCHED] Scheduler dihentikan.")


def main():
    """Entry point: python -m etl.scheduler"""
    import argparse
    parser = argparse.ArgumentParser(description="Sentinel-1 Pipeline Scheduler")
    parser.add_argument("--run-now",  action="store_true", help="Jalankan pipeline sekali sekarang")
    parser.add_argument("--storage",  action="store_true", help="Tampilkan ringkasan penggunaan storage")
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

    # Default: start scheduler 24/7
    Scheduler().start()


if __name__ == "__main__":
    main()