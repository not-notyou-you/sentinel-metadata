# etl/migrate_data_structure.py
"""
Migrasi struktur folder data/datasets/... dari layout tanggal-dulu
({dataset_id}/{acquisition_date_YYYYMMDD}/{tier}/*.tif) ke layout
tier-dulu-per-nama-dataset ({dataset_id}_{slug_nama}/{tier}/{scene}/*.tif).

`{scene}` untuk produk Sentinel-1 (RAW_EXTRACTED_TIFF, CROPPED_TIFF,
LEE_FILTERED) adalah product_identifier scene tsb, diambil dari
satellite_scenes lewat data_products.scene_id -- bukan diparse dari nama
folder lama. Untuk artefak yang bukan milik satu scene S1 tertentu
(MODIS_FLOOD/GPM_RAINFALL input fusion, dan FUSION_H5 output GOLD),
`{scene}` adalah folder tanggal YYYYMMDD yang sudah ada di layout lama
(satu level di atas folder tier) -- konsisten dengan cara
module7/module8/module9 menamai scene aux di layout baru.

Setiap dataset di-backup penuh ke backup/data_structure_migration/
sebelum satu file pun dipindah. Setelah `shutil.move`, checksum SHA-256
file di lokasi baru dibandingkan dengan data_products.data_hash_sha256
(atau dihitung ulang dari file lama kalau kolom itu kosong) -- kalau tidak
cocok, file dikembalikan ke lokasi lama dan product itu dilaporkan gagal,
tidak pernah didiamkan begitu saja.

Usage:
    python -m etl.migrate_data_structure                 # migrasi semua dataset
    python -m etl.migrate_data_structure --dataset-id 2   # migrasi 1 dataset
    python -m etl.migrate_data_structure --dry-run        # tampilkan rencana saja
"""

from __future__ import annotations

import argparse
import hashlib
import json
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

# product_type yang bukan milik satu scene Sentinel-1 tertentu -- kunci
# scene-nya diturunkan dari folder tanggal lama, bukan product_identifier.
_AUX_PRODUCT_TYPES = {"MODIS_FLOOD", "GPM_RAINFALL", "FUSION_H5"}


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


def _old_dataset_root(dataset_id: int) -> Path:
    """Layout lama: data/datasets/{dataset_id}/ (tanpa slug nama)."""
    return fm.DATA_ROOT / str(dataset_id)


def _backup_dataset(dataset_id: int) -> Path | None:
    src = _old_dataset_root(dataset_id)
    if not src.exists() or not any(src.rglob("*")):
        return None
    dest = BACKUP_ROOT / f"dataset_{dataset_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger.info("[BACKUP] dataset_id=%d %s -> %s", dataset_id, src, dest)
    shutil.copytree(src, dest)
    return dest


def _products_with_scene_info(db: DatabaseClient, dataset_id: int) -> list[dict]:
    with db.session() as sess:
        rows = sess.execute(
            select(DataProduct, SatelliteScene.product_identifier)
            .join(SatelliteScene, SatelliteScene.scene_id == DataProduct.scene_id)
            .where(DataProduct.dataset_id == dataset_id)
        ).all()
        return [
            {
                "product_id": p.product_id,
                "product_tier": p.product_tier.value if hasattr(p.product_tier, "value") else str(p.product_tier),
                "product_type": p.product_type,
                "file_path": p.file_path,
                "file_name": p.file_name,
                "data_hash_sha256": p.data_hash_sha256,
                "product_identifier": pid,
            }
            for p, pid in rows
        ]


def _move_untracked(src: Path, dst: Path, dry_run: bool) -> str:
    """Pindahkan satu file yang tidak tercatat di data_products. Return
    'moved', 'skipped', atau 'failed'."""
    if src.as_posix() == dst.as_posix() or not src.exists():
        return "skipped"
    if dry_run:
        logger.info("[DRY-RUN] (untracked) %s -> %s", src, dst)
        return "moved"
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)
        if dst.exists():
            dst.unlink()
        shutil.move(str(src), str(dst))
        logger.info("[MIGRATE] (untracked) %s -> %s", src, dst)
        return "moved"
    except Exception:
        logger.exception("[MIGRATE] gagal pindah file untracked %s", src)
        return "failed"


def _migrate_untracked_files(dataset_id: int, dataset_name: str, old_root: Path, dry_run: bool) -> dict:
    """File yang tidak tercatat sebagai data_products tapi tetap perlu
    dipindah: arsip .zip SAFE mentah (raw/), sidecar metadata_qa.json
    (silver/, berisi product_identifier pemiliknya), dan sidecar
    fusion_metadata.json (gold/, milik scene tanggal itu sendiri)."""
    counts = {"moved": 0, "skipped": 0, "failed": 0}
    if not old_root.exists():
        return counts

    date_dirs = sorted(d for d in old_root.iterdir() if d.is_dir() and len(d.name) == 8 and d.name.isdigit())
    for date_dir in date_dirs:
        raw_dir = date_dir / "raw"
        if raw_dir.exists():
            for zip_path in raw_dir.glob("*.zip"):
                scene_key = zip_path.stem  # "{product_identifier}.SAFE.zip" -> "{product_identifier}.SAFE"
                new_dir = fm.get_scene_dir(dataset_id, dataset_name, "raw", scene_key)
                counts[_move_untracked(zip_path, new_dir / zip_path.name, dry_run)] += 1

        silver_qa = date_dir / "silver" / "metadata_qa.json"
        if silver_qa.exists():
            try:
                scene_key = json.loads(silver_qa.read_text())["product_identifier"]
                new_dir = fm.get_scene_dir(dataset_id, dataset_name, "silver", scene_key)
                counts[_move_untracked(silver_qa, new_dir / silver_qa.name, dry_run)] += 1
            except Exception:
                logger.exception("[MIGRATE] gagal baca metadata_qa.json: %s", silver_qa)
                counts["failed"] += 1

        gold_meta = date_dir / "gold" / "fusion_metadata.json"
        if gold_meta.exists():
            new_dir = fm.get_scene_dir(dataset_id, dataset_name, "gold", date_dir.name)
            counts[_move_untracked(gold_meta, new_dir / gold_meta.name, dry_run)] += 1

    return counts


def _scene_key_for(row: dict, old_path: Path) -> str:
    """Kunci scene di layout baru: product_identifier S1 asli untuk produk
    S1 (RAW/BRONZE/SILVER LEE_FILTERED), atau folder tanggal lama -- satu
    level di atas folder tier di layout lama -- untuk artefak fusion yang
    bukan milik satu scene S1 tertentu (MODIS_FLOOD, GPM_RAINFALL, FUSION_H5)."""
    if row["product_type"] in _AUX_PRODUCT_TYPES:
        return old_path.parent.parent.name
    return row["product_identifier"]


def migrate_dataset(db: DatabaseClient, dataset_id: int, dry_run: bool = False) -> dict:
    with db.session() as sess:
        dataset = sess.get(Dataset, dataset_id)
        if dataset is None:
            logger.warning("[MIGRATE] dataset_id=%d tidak ditemukan di database, dilewati", dataset_id)
            return {"dataset_id": dataset_id, "moved": 0, "skipped": 0, "failed": 0}
        dataset_name = dataset.name

    old_root = _old_dataset_root(dataset_id)
    new_root = fm.get_dataset_root(dataset_id, dataset_name)

    products = _products_with_scene_info(db, dataset_id)
    if not products and not old_root.exists():
        logger.info("[MIGRATE] dataset_id=%d tidak punya data di disk, dilewati", dataset_id)
        return {"dataset_id": dataset_id, "moved": 0, "skipped": 0, "failed": 0}

    if not dry_run:
        _backup_dataset(dataset_id)

    moved = skipped = failed = 0
    with db.session() as sess:
        for row in products:
            old_path = Path(row["file_path"])
            if old_path.is_relative_to(new_root):
                # Sudah dimigrasi di run sebelumnya -- jangan hitung ulang
                # scene_key dari old_path (untuk tier aux, itu diturunkan
                # dari posisi folder tanggal di layout LAMA, yang tidak lagi
                # berlaku begitu file sudah pindah ke layout baru).
                skipped += 1
                continue

            tier = row["product_tier"].lower()
            scene_key = _scene_key_for(row, old_path)
            new_dir = fm.get_scene_dir(dataset_id, dataset_name, tier, scene_key)
            new_path = new_dir / row["file_name"]

            if old_path.as_posix() == new_path.as_posix():
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

    untracked_counts = _migrate_untracked_files(dataset_id, dataset_name, old_root, dry_run)
    moved += untracked_counts["moved"]
    skipped += untracked_counts["skipped"]
    failed += untracked_counts["failed"]

    if not dry_run:
        # Cache granule mentah MODIS/GPM (file .hdf/.nc4 sebelum mosaic/crop)
        # tidak tercatat sebagai data_products -- dipindah langsung sebagai folder.
        for source in ("modis", "gpm"):
            old_aux = old_root / "raw" / source
            if old_aux.exists():
                new_aux = fm.get_aux_raw_dir(dataset_id, dataset_name, source)
                new_aux.parent.mkdir(parents=True, exist_ok=True)
                if new_aux.exists():
                    shutil.rmtree(new_aux)
                shutil.move(str(old_aux), str(new_aux))
                logger.info("[MIGRATE] aux cache dataset_id=%d %s -> %s", dataset_id, old_aux, new_aux)

        old_meta = old_root / "metadata.json"
        if old_meta.exists():
            new_meta = fm.get_dataset_metadata_path(dataset_id, dataset_name)
            new_meta.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(old_meta), str(new_meta))

        _remove_empty_dirs(old_root)
        if old_root.exists() and old_root != new_root and not any(old_root.rglob("*")):
            old_root.rmdir()

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
        description=(
            "Migrasi data/datasets/{id}/{tanggal}/{tier}/ ke "
            "data/datasets/{id}_{slug_nama}/{tier}/{scene}/"
        )
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
