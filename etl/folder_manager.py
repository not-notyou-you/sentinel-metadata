# etl/folder_manager.py
"""
Path management utility untuk struktur penyimpanan data per dataset.

Layout on-disk:
    data/datasets/{dataset_id}/
        metadata.json
        {acquisition_date_YYYYMMDD}/
            raw/
            bronze/
            silver/
            gold/

File di dalam tiap tier langsung diberi nama dari product_identifier scene
(sudah unik per scene termasuk timestamp), jadi tidak perlu sub-folder per
scene di dalam tier.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

TIERS: tuple[str, ...] = ("raw", "bronze", "silver", "gold")

DATA_ROOT = Path("data") / "datasets"


def _date_str(acquisition_date: date | datetime | str) -> str:
    """Normalisasi tanggal akuisisi ke format folder YYYYMMDD."""
    if isinstance(acquisition_date, datetime):
        acquisition_date = acquisition_date.date()
    if isinstance(acquisition_date, date):
        return acquisition_date.strftime("%Y%m%d")
    s = str(acquisition_date).replace("-", "")
    if len(s) != 8 or not s.isdigit():
        raise ValueError(
            f"acquisition_date tidak valid: {acquisition_date!r}. "
            "Gunakan objek date/datetime atau string 'YYYYMMDD'/'YYYY-MM-DD'."
        )
    return s


def get_dataset_root(dataset_id: int) -> Path:
    """Folder root untuk sebuah dataset: data/datasets/{dataset_id}/"""
    return DATA_ROOT / str(dataset_id)


def get_dataset_metadata_path(dataset_id: int) -> Path:
    """Path ke metadata.json level-dataset."""
    return get_dataset_root(dataset_id) / "metadata.json"


def get_dataset_path(
    dataset_id: int,
    acquisition_date: date | datetime | str,
    tier: str | None = None,
) -> Path:
    """
    Path folder untuk dataset pada tanggal akuisisi tertentu, opsional per tier.

    get_dataset_path(2, date(2024, 1, 15))          -> data/datasets/2/20240115
    get_dataset_path(2, date(2024, 1, 15), "gold")   -> data/datasets/2/20240115/gold
    """
    path = get_dataset_root(dataset_id) / _date_str(acquisition_date)
    if tier is not None:
        tier = tier.lower()
        if tier not in TIERS:
            raise ValueError(f"Tier tidak valid: {tier!r}. Valid: {TIERS}")
        path = path / tier
    return path


def ensure_tier_folders_exist(
    dataset_id: int,
    acquisition_date: date | datetime | str,
    tiers: tuple[str, ...] = TIERS,
) -> dict[str, Path]:
    """Buat folder tier (raw/bronze/silver/gold) untuk tanggal ini jika belum ada."""
    paths: dict[str, Path] = {}
    for tier in tiers:
        p = get_dataset_path(dataset_id, acquisition_date, tier)
        p.mkdir(parents=True, exist_ok=True)
        paths[tier] = p
    return paths


def get_tier_files(
    dataset_id: int,
    acquisition_date: date | datetime | str,
    tier: str,
) -> list[Path]:
    """List semua file di dalam satu tier pada tanggal akuisisi tertentu."""
    p = get_dataset_path(dataset_id, acquisition_date, tier)
    if not p.exists():
        return []
    return sorted(f for f in p.rglob("*") if f.is_file())


def list_acquisition_dates(dataset_id: int) -> list[str]:
    """List semua folder tanggal (YYYYMMDD) yang ada untuk sebuah dataset."""
    root = get_dataset_root(dataset_id)
    if not root.exists():
        return []
    return sorted(
        d.name for d in root.iterdir()
        if d.is_dir() and len(d.name) == 8 and d.name.isdigit()
    )


def write_dataset_metadata(dataset_id: int, metadata: dict) -> Path:
    """Tulis metadata.json level-dataset (ringkasan, bukan sumber kebenaran — DB tetap authoritative)."""
    path = get_dataset_metadata_path(dataset_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(metadata, f, indent=2, default=str)
    tmp.replace(path)
    return path
