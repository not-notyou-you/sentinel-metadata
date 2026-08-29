# etl/folder_manager.py
"""
Path management utility untuk struktur penyimpanan data per dataset.

Layout on-disk:
    data/datasets/{dataset_id}_{slug(dataset_name)}/
        metadata.json
        raw/
            {scene}/
        bronze/
            {scene}/
        silver/
            {scene}/
        gold/
            {scene}/

`{scene}` untuk file Sentinel-1 adalah product_identifier scene tersebut
(sudah unik termasuk jam:menit:detik). Untuk artefak yang tidak terikat ke
satu scene S1 tertentu (input fusion MODIS/GPM, dan output fusion GOLD yang
di-dedup per tanggal — lihat module9_fusion.py), `{scene}` adalah tanggal
akuisisi dalam format YYYYMMDD.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

TIERS: tuple[str, ...] = ("raw", "bronze", "silver", "gold")

DATA_ROOT = Path("data") / "datasets"


def slugify(name: str) -> str:
    """Nama dataset -> slug aman-filesystem, mis. "hakim d1" -> "hakim_d1".
    Sama persis dengan etl/pipeline_logger.py:_slug_filename supaya nama
    folder dataset & nama file log tetap konsisten."""
    slug = re.sub(r"[^A-Za-z0-9_.-]", "_", name).strip("_")
    return slug or "dataset"


def scene_slug(scene_key: str) -> str:
    """Sanitasi kunci scene (product_identifier S1 atau tanggal YYYYMMDD)
    supaya aman jadi nama folder."""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", str(scene_key))


def date_key(d: date | datetime | str) -> str:
    """Normalisasi tanggal ke format kunci scene YYYYMMDD."""
    if isinstance(d, datetime):
        d = d.date()
    if isinstance(d, date):
        return d.strftime("%Y%m%d")
    s = str(d).replace("-", "")
    if len(s) != 8 or not s.isdigit():
        raise ValueError(
            f"tanggal tidak valid: {d!r}. Gunakan objek date/datetime atau "
            "string 'YYYYMMDD'/'YYYY-MM-DD'."
        )
    return s


def dataset_dir_name(dataset_id: int, dataset_name: str) -> str:
    return f"{dataset_id}_{slugify(dataset_name)}"


def get_dataset_root(dataset_id: int, dataset_name: str) -> Path:
    """Folder root untuk sebuah dataset: data/datasets/{id}_{slug}/"""
    return DATA_ROOT / dataset_dir_name(dataset_id, dataset_name)


def get_dataset_metadata_path(dataset_id: int, dataset_name: str) -> Path:
    """Path ke metadata.json level-dataset."""
    return get_dataset_root(dataset_id, dataset_name) / "metadata.json"


def get_tier_dir(dataset_id: int, dataset_name: str, tier: str) -> Path:
    """Path folder untuk satu tier (raw/bronze/silver/gold), berisi satu
    subfolder per scene."""
    tier = tier.lower()
    if tier not in TIERS:
        raise ValueError(f"Tier tidak valid: {tier!r}. Valid: {TIERS}")
    return get_dataset_root(dataset_id, dataset_name) / tier


def get_scene_dir(dataset_id: int, dataset_name: str, tier: str, scene_key: str) -> Path:
    """
    Path folder untuk satu scene di dalam satu tier.

    get_scene_dir(2, "Hakim D1", "raw", "S1A_IW_GRDH_...")
        -> data/datasets/2_hakim_d1/raw/S1A_IW_GRDH_...
    get_scene_dir(2, "Hakim D1", "gold", "20240115")
        -> data/datasets/2_hakim_d1/gold/20240115
    """
    return get_tier_dir(dataset_id, dataset_name, tier) / scene_slug(scene_key)


def ensure_scene_dir(dataset_id: int, dataset_name: str, tier: str, scene_key: str) -> Path:
    """Buat (jika belum ada) dan kembalikan folder satu scene di satu tier."""
    p = get_scene_dir(dataset_id, dataset_name, tier, scene_key)
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_scratch_dir(dataset_id: int, dataset_name: str, scene_key: str) -> Path:
    """Folder kerja sementara (hasil kalibrasi radiometrik sebelum crop),
    dihapus otomatis setelah tahap CROP selesai — bukan bagian dari 4 tier
    resmi."""
    return get_dataset_root(dataset_id, dataset_name) / "_work" / scene_slug(scene_key)


def get_aux_raw_dir(dataset_id: int, dataset_name: str, source: str) -> Path:
    """Folder cache bersama untuk granule mentah MODIS/GPM (file .hdf/.nc4
    sebelum di-mosaic/crop) yang mencakup banyak tanggal sekaligus untuk satu
    dataset — tidak terikat ke satu scene, jadi diletakkan di bawah tier raw/
    dengan prefix "_aux_" (aman: product_identifier S1 asli tidak pernah
    diawali underscore, jadi tidak akan pernah bentrok nama)."""
    return get_tier_dir(dataset_id, dataset_name, "raw") / f"_aux_{source}"


def list_scenes(dataset_id: int, dataset_name: str, tier: str) -> list[str]:
    """List semua nama folder scene (bukan folder aux berawalan "_") yang
    ada di dalam satu tier."""
    p = get_tier_dir(dataset_id, dataset_name, tier)
    if not p.exists():
        return []
    return sorted(d.name for d in p.iterdir() if d.is_dir() and not d.name.startswith("_"))


def get_scene_files(dataset_id: int, dataset_name: str, tier: str, scene_key: str) -> list[Path]:
    """List semua file di dalam satu scene pada satu tier."""
    p = get_scene_dir(dataset_id, dataset_name, tier, scene_key)
    if not p.exists():
        return []
    return sorted(f for f in p.rglob("*") if f.is_file())


def get_tier_files(dataset_id: int, dataset_name: str, tier: str) -> list[Path]:
    """List semua file di dalam satu tier (semua scene, termasuk cache aux)."""
    p = get_tier_dir(dataset_id, dataset_name, tier)
    if not p.exists():
        return []
    return sorted(f for f in p.rglob("*") if f.is_file())


def write_dataset_metadata(dataset_id: int, dataset_name: str, metadata: dict) -> Path:
    """Tulis metadata.json level-dataset (ringkasan, bukan sumber kebenaran — DB tetap authoritative)."""
    path = get_dataset_metadata_path(dataset_id, dataset_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(metadata, f, indent=2, default=str)
    tmp.replace(path)
    return path
