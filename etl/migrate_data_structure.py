# etl/migrate_data_structure.py
"""
Migrasi struktur folder data/datasets/{dataset_id}/... dari layout lama
({tier}/{slug}/*.tif) ke layout baru ({acquisition_date_YYYYMMDD}/{tier}/*.tif).

Sumber kebenaran untuk tanggal akuisisi tiap file adalah kolom
satellite_scenes.acquisition_datetime di database (dihubungkan lewat
data_products.scene_id), bukan nama file atau nama folder lama — supaya
migrasi tetap benar walau slug lama tidak mengandung tanggal yang bisa
diparse dengan aman.

Setiap dataset di-backup (copy penuh) ke backup/data_structure_migration/
sebelum file mana pun dipindah. Setelah dipindah, checksum SHA-256 file di
lokasi baru dibandingkan dengan data_products.data_hash_sha256 (atau dihitung
ulang dari file lama kalau kolom itu kosong) — kalau tidak cocok, file
dikembalikan ke lokasi lama dan product itu dilaporkan gagal, tidak pernah
didiamkan begitu saja.

Usage:
    python -m etl.migrate_data_structure                 # migrasi semua dataset
    python -m etl.migrate_data_structure --dataset-id 2   # migrasi 1 dataset
    python -m etl.migrate_data_structure --dry-run        # tampilkan rencana saja
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import shutil
import sys
from datetime import datetime
from pathlib import Path

from sqlalchemy import select

from etl import folder_manager as fm
from etl.database_client import DataProduct, Dataset, DatabaseClient, SatelliteScene

logger = logging.getLogger("migrate_data_structure")

BACKUP_ROOT = Path("backup") / "data_structure_migration"
LOG_DIR = Path("logs_pipeline")


def _configure_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(LOG_DIR / "migrate_data_structure.log"),
        ],
    )


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _backup_dataset(dataset_id: int) -> Path | None:
    src = fm.get_dataset_root(dataset_id)
    if not src.exists() or not any(src.rglob("*")):
        return None
    dest = BACKUP_ROOT / f"dataset_{dataset_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger.info("[BACKUP] dataset_id=%d %s -> %s", dataset_id, src, dest)
    shutil.copytree(src, dest)
    return dest


def _products_with_acquisition_date(db: DatabaseClient, dataset_id: int) -> list[dict]:
    with db.session() as sess:
        rows = sess.execute(
            select(DataProduct, SatelliteScene.acquisition_datetime)
            .join(SatelliteScene, SatelliteScene.scene_id == DataProduct.scene_id)
            .where(DataProduct.dataset_id == dataset_id)
        ).all()
        return [
            {
                "product_id": p.product_id,
                "product_tier": p.product_tier.value if hasattr(p.product_tier, "value") else str(p.product_tier),
                "file_path": p.file_path,
                "file_name": p.file_name,
                "data_hash_sha256": p.data_hash_sha256,
                "acquisition_datetime": acq_dt,
            }
            for p, acq_dt in rows
        ]


def migrate_dataset(db: DatabaseClient, dataset_id: int, dry_run: bool = False) -> dict:
    products = _products_with_acquisition_date(db, dataset_id)
    if not products:
        logger.info("[MIGRATE] dataset_id=%d tidak punya data_products, dilewati", dataset_id)
        return {"dataset_id": dataset_id, "moved": 0, "skipped": 0, "failed": 0}

    if not dry_run:
        _backup_dataset(dataset_id)

    moved = skipped = failed = 0
    with db.session() as sess:
        for row in products:
            old_path = Path(row["file_path"])
            tier = row["product_tier"].lower()
            new_dir = fm.get_dataset_path(dataset_id, row["acquisition_datetime"], tier)
            new_path = new_dir / row["file_name"]

            if Path(old_path).as_posix() == new_path.as_posix():
                skipped += 1
                continue
            if not old_path.exists():
                logger.warning(
                    "[MIGRATE] file hilang di disk, dilewati (product_id=%d): %s",
                    row["product_id"], old_path,
                )
                skipped += 1
                continue

            try:
                expected_hash = row["data_hash_sha256"] or _sha256(old_path)

                if dry_run:
                    logger.info("[DRY-RUN] product_id=%d %s -> %s", row["product_id"], old_path, new_path)
                    moved += 1
                    continue

                new_dir.mkdir(parents=True, exist_ok=True)
                shutil.move(str(old_path), str(new_path))

                actual_hash = _sha256(new_path)
                if expected_hash and actual_hash != expected_hash:
                    logger.error(
                        "[MIGRATE] checksum tidak cocok setelah pindah (product_id=%d): "
                        "%s != %s -- mengembalikan file ke lokasi lama",
                        row["product_id"], actual_hash, expected_hash,
                    )
                    shutil.move(str(new_path), str(old_path))
                    failed += 1
                    continue

                product = sess.get(DataProduct, row["product_id"])
                if product is not None:
                    product.file_path = str(new_path)
                moved += 1
                logger.info("[MIGRATE] product_id=%d %s -> %s", row["product_id"], old_path, new_path)
            except Exception:
                logger.exception(
                    "[MIGRATE] gagal migrasi product_id=%d %s", row["product_id"], old_path
                )
                failed += 1

    # Folder tier/slug lama yang sudah kosong tidak dibutuhkan lagi di layout baru.
    if not dry_run:
        _remove_empty_dirs(fm.get_dataset_root(dataset_id))

    logger.info(
        "[MIGRATE] dataset_id=%d selesai: moved=%d skipped=%d failed=%d",
        dataset_id, moved, skipped, failed,
    )
    return {"dataset_id": dataset_id, "moved": moved, "skipped": skipped, "failed": failed}


def _remove_empty_dirs(root: Path) -> None:
    if not root.exists():
        return
    for d in sorted(root.rglob("*"), reverse=True):
        if d.is_dir():
            try:
                d.rmdir()
            except OSError:
                pass  # tidak kosong, biarkan


def _all_dataset_ids(db: DatabaseClient) -> list[int]:
    with db.session() as sess:
        return list(sess.scalars(select(Dataset.dataset_id).order_by(Dataset.dataset_id)).all())


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrasi data/datasets/{id}/{tier}/{slug}/ ke data/datasets/{id}/{tanggal}/{tier}/"
    )
    parser.add_argument("--dataset-id", type=int, default=None, help="Migrasi satu dataset saja")
    parser.add_argument("--dry-run", action="store_true", help="Tampilkan rencana tanpa memindahkan file")
    args = parser.parse_args()

    _configure_logging()
    db = DatabaseClient.from_env()

    dataset_ids = [args.dataset_id] if args.dataset_id is not None else _all_dataset_ids(db)
    logger.info("[MIGRATE] mulai migrasi dataset_ids=%s dry_run=%s", dataset_ids, args.dry_run)

    totals = {"moved": 0, "skipped": 0, "failed": 0}
    for dataset_id in dataset_ids:
        result = migrate_dataset(db, dataset_id, dry_run=args.dry_run)
        for k in totals:
            totals[k] += result[k]

    logger.info("[MIGRATE] selesai semua dataset: %s", totals)
    if totals["failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
