# etl/module9_fusion.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date as date_type
from pathlib import Path

import h5py
import numpy as np
import rasterio

logger = logging.getLogger(__name__)


@dataclass
class FeatureRow:
    date: str
    s1_vv_path: str | None
    s1_vh_path: str | None
    days_since_s1: int
    modis_flood_path: str | None
    modis_cloud_cover: float | None
    gpm_rain_24h_path: str | None
    gpm_rain_72h_path: str | None
    gpm_rain_7d_path: str | None


def find_latest_s1(
    target_date: date_type,
    s1_gold_dir: str,
    max_lookback_days: int = 6,
) -> tuple[str | None, str | None, int]:
    base = Path(s1_gold_dir)
    for offset in range(0, max_lookback_days + 1):
        candidate_date = date_type.fromordinal(target_date.toordinal() - offset)
        day_dir = base / candidate_date.strftime("%Y-%m-%d")
        vv = list(day_dir.glob("*_VV_cog.tif")) if day_dir.exists() else []
        vh = list(day_dir.glob("*_VH_cog.tif")) if day_dir.exists() else []
        if vv and vh:
            return str(vv[0]), str(vh[0]), offset
    return None, None, max_lookback_days + 1


def build_feature_row(
    target_date: date_type,
    s1_gold_dir: str,
    modis_flood_path: str | None,
    gpm_paths: dict[str, str] | None,
) -> FeatureRow:
    vv, vh, days_since = find_latest_s1(target_date, s1_gold_dir)
    gpm_paths = gpm_paths or {}
    return FeatureRow(
        date=target_date.isoformat(),
        s1_vv_path=vv,
        s1_vh_path=vh,
        days_since_s1=days_since,
        modis_flood_path=modis_flood_path,
        modis_cloud_cover=None,
        gpm_rain_24h_path=gpm_paths.get("24h"),
        gpm_rain_72h_path=gpm_paths.get("72h"),
        gpm_rain_7d_path=gpm_paths.get("7d"),
    )


def _read_or_nan(path: str | None, shape: tuple[int, int]) -> np.ndarray:
    if not path or not Path(path).exists():
        return np.full(shape, np.nan, dtype=np.float32)
    with rasterio.open(path) as src:
        data = src.read(1, out_shape=shape).astype(np.float32)
    return data


def write_feature_stack(
    rows: list[FeatureRow],
    output_h5: str,
    tile_shape: tuple[int, int],
) -> str:
    Path(output_h5).parent.mkdir(parents=True, exist_ok=True)
    channels = ["s1_vv", "s1_vh", "modis_flood", "gpm_24h", "gpm_72h", "gpm_7d", "days_since_s1"]
    height, width = tile_shape
    stack = np.full((len(rows), height, width, len(channels)), np.nan, dtype=np.float32)

    for i, row in enumerate(rows):
        stack[i, :, :, 0] = _read_or_nan(row.s1_vv_path, tile_shape)
        stack[i, :, :, 1] = _read_or_nan(row.s1_vh_path, tile_shape)
        stack[i, :, :, 2] = _read_or_nan(row.modis_flood_path, tile_shape)
        stack[i, :, :, 3] = _read_or_nan(row.gpm_rain_24h_path, tile_shape)
        stack[i, :, :, 4] = _read_or_nan(row.gpm_rain_72h_path, tile_shape)
        stack[i, :, :, 5] = _read_or_nan(row.gpm_rain_7d_path, tile_shape)
        stack[i, :, :, 6] = row.days_since_s1

    with h5py.File(output_h5, "w") as f:
        f.create_dataset("features", data=stack, compression="gzip")
        f.attrs["channels"] = channels
        f.attrs["dates"] = [row.date for row in rows]

    logger.info("[M9] wrote %s shape=%s", output_h5, stack.shape)
    return output_h5