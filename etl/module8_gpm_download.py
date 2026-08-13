# etl/module8_gpm_download.py
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import rasterio

logger = logging.getLogger(__name__)

GPM_BASE = "https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGDF.07"


def _auth_headers() -> dict:
    token = os.getenv("NASA_EARTHDATA_TOKEN")
    if not token:
        raise RuntimeError("NASA_EARTHDATA_TOKEN belum diset.")
    return {"Authorization": f"Bearer {token}"}


def download_gpm_daily(date: datetime, output_dir: str) -> str:
    import requests

    fname = f"3B-DAY.MS.MRG.3IMERG.{date.strftime('%Y%m%d')}-S000000-E235959.V07B.nc4"
    url = f"{GPM_BASE}/{date.year}/{date.month:02d}/{fname}"
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    out_path = Path(output_dir) / fname
    with requests.get(url, headers=_auth_headers(), stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(chunk)
    logger.info("[M8] downloaded %s", out_path.name)
    return str(out_path)


def crop_to_bbox_netcdf(nc_path: str, bbox: tuple[float, float, float, float], output_tif: str) -> str:
    from rasterio.mask import mask
    from shapely.geometry import box, mapping

    src_path = f'NETCDF:"{nc_path}":precipitationCal'
    geom = mapping(box(*bbox))
    with rasterio.open(src_path) as src:
        out_image, out_transform = mask(src, [geom], crop=True)
        meta = src.meta.copy()
        meta.update({
            "driver": "GTiff",
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": out_transform,
        })
        with rasterio.open(output_tif, "w", **meta) as dst:
            dst.write(out_image)
    return output_tif


def aggregate_rainfall(daily_tifs: list[str]) -> np.ndarray:
    total = None
    for path in daily_tifs:
        with rasterio.open(path) as src:
            data = src.read(1).astype(np.float64)
        total = data if total is None else total + data
    return total


def run(date: datetime, output_dir: str, bbox: tuple[float, float, float, float]) -> dict:
    raw_dir = str(Path(output_dir) / "raw")
    gold_dir = str(Path(output_dir) / "gold")
    Path(gold_dir).mkdir(parents=True, exist_ok=True)

    max_days = 7
    daily_cache: dict[str, str] = {}
    for i in range(max_days):
        d = date - timedelta(days=i)
        key = d.strftime("%Y%m%d")
        tif_path = str(Path(gold_dir) / f"gpm_{key}_crop.tif")
        if not Path(tif_path).exists():
            nc_path = download_gpm_daily(d, raw_dir)
            crop_to_bbox_netcdf(nc_path, bbox, tif_path)
        else:
            logger.info("[M8] cache hit gpm_%s_crop.tif", key)
        daily_cache[key] = tif_path

    windows = {"24h": 1, "72h": 3, "7d": 7}
    ordered_keys = [(date - timedelta(days=i)).strftime("%Y%m%d") for i in range(max_days)]
    result = {}
    for label, n_days in windows.items():
        subset = [daily_cache[k] for k in ordered_keys[:n_days]]
        total = aggregate_rainfall(subset)
        out_path = str(Path(gold_dir) / f"gpm_rain_{label}_{date.strftime('%Y%m%d')}.tif")
        with rasterio.open(subset[0]) as ref:
            meta = ref.meta.copy()
        with rasterio.open(out_path, "w", **meta) as dst:
            dst.write(total.astype(meta["dtype"]), 1)
        result[label] = out_path
    return result