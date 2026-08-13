# etl/module7_modis_download.py
from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path

import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.warp import calculate_default_transform, reproject

logger = logging.getLogger(__name__)

MODIS_TILES = ["h30v08", "h31v08"]
LAADS_BASE = "https://nrt3.modaps.eosdis.nasa.gov/archive/allData/61/MCDWD_L3_F2_NRT"


def _auth_headers() -> dict:
    token = os.getenv("NASA_EARTHDATA_TOKEN")
    if not token:
        raise RuntimeError(
            "NASA_EARTHDATA_TOKEN belum diset. Generate app token di urs.earthdata.nasa.gov -> Generate Token."
        )
    return {"Authorization": f"Bearer {token}"}


def discover_modis_flood_files(date: datetime, tiles: list[str] = MODIS_TILES) -> list[dict]:
    import requests

    doy = date.timetuple().tm_yday
    year = date.year
    url = f"{LAADS_BASE}/{year}/{doy:03d}/"
    resp = requests.get(url, headers=_auth_headers(), timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"gagal listing MODIS NRT ({resp.status_code}): {url}")

    results = []
    for tile in tiles:
        matches = [line for line in resp.text.splitlines() if tile in line and ".hdf" in line]
        for line in matches:
            fname = line.split('"')[1] if '"' in line else None
            if fname:
                results.append({"tile": tile, "file_name": fname, "download_url": url + fname})
    return results


def download_modis_file(item: dict, output_dir: str) -> str:
    import requests

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    out_path = Path(output_dir) / item["file_name"]
    with requests.get(item["download_url"], headers=_auth_headers(), stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(chunk)
    logger.info("[M7] downloaded %s", out_path.name)
    return str(out_path)


def hdf_to_geotiff(
    hdf_path: str,
    subdataset: str,
    output_path: str,
    dst_crs: str = "EPSG:4326",
) -> str:
    src_path = f'HDF4_EOS:EOS_GRID:"{hdf_path}":{subdataset}'
    with rasterio.open(src_path) as src:
        transform, width, height = calculate_default_transform(
            src.crs, dst_crs, src.width, src.height, *src.bounds
        )
        kwargs = src.meta.copy()
        kwargs.update({"crs": dst_crs, "transform": transform, "width": width, "height": height})
        with rasterio.open(output_path, "w", **kwargs) as dst:
            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs=dst_crs,
                    resampling=Resampling.nearest,
                )
    logger.info("[M7] %s (%s) -> %s", Path(hdf_path).name, subdataset, Path(output_path).name)
    return output_path


def compute_ndwi(green_path: str, swir_path: str, output_path: str) -> str:
    with rasterio.open(green_path) as g, rasterio.open(swir_path) as s:
        green = g.read(1).astype(np.float32)
        swir = s.read(1).astype(np.float32)
        meta = g.meta.copy()
    denom = green + swir
    ndwi = np.where(denom != 0, (green - swir) / denom, 0.0)
    meta.update(dtype="float32", count=1)
    with rasterio.open(output_path, "w", **meta) as dst:
        dst.write(ndwi.astype(np.float32), 1)
    return output_path


def run(date: datetime, output_dir: str, tiles: list[str] = MODIS_TILES) -> list[dict]:
    raw_dir = str(Path(output_dir) / "raw")
    gold_dir = str(Path(output_dir) / "gold")
    items = discover_modis_flood_files(date, tiles)
    results = []
    for item in items:
        hdf_path = download_modis_file(item, raw_dir)
        flood_tif = str(Path(gold_dir) / f"{Path(item['file_name']).stem}_flood.tif")
        hdf_to_geotiff(hdf_path, "Flood 1-day 250m", flood_tif)
        results.append({"tile": item["tile"], "date": date.date().isoformat(), "flood_path": flood_tif})
    return results