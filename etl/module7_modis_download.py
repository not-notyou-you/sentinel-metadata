# etl/module7_modis_download.py
"""
Downloads NASA LAADS DAAC MODIS MCDWD (Flood) product, reprojects/crops it
to the dataset AOI, and writes one GeoTIFF per day for lineage tracking.
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import rasterio
from rasterio.enums import Resampling
from rasterio.merge import merge
from rasterio.warp import calculate_default_transform, reproject

logger = logging.getLogger(__name__)

MODIS_PRODUCT = "MCDWD_L3_F2_NRT"
LAADS_BASE = f"https://nrt3.modaps.eosdis.nasa.gov/archive/allData/61/{MODIS_PRODUCT}"
MODIS_TILES = ["h30v08", "h31v08"]

# Jabodetabek bounding box, WGS84 (min_lon, min_lat, max_lon, max_lat)
JABODETABEK_BBOX = (106.4, -6.7, 107.2, -5.9)

DST_CRS = "EPSG:4326"
MAX_RETRIES = 3


def _auth_headers() -> dict:
    token = os.getenv("NASA_EARTHDATA_TOKEN")
    if not token:
        raise RuntimeError(
            "NASA_EARTHDATA_TOKEN belum diset. Generate app token di "
            "urs.earthdata.nasa.gov -> Generate Token."
        )
    return {"Authorization": f"Bearer {token}"}


def _daterange(date_start: datetime, date_end: datetime):
    d = date_start
    while d.date() <= date_end.date():
        yield d
        d += timedelta(days=1)


def _md5(path: Path, chunk: int = 8 * 1024 * 1024) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def _discover_tile_files(date: datetime, tiles: list[str]) -> list[dict]:
    """List available MCDWD granules for `date` by scraping the LAADS
    directory index, one entry per requested tile."""
    import requests

    doy = date.timetuple().tm_yday
    url = f"{LAADS_BASE}/{date.year}/{doy:03d}/"
    resp = requests.get(url, headers=_auth_headers(), timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"gagal listing LAADS ({resp.status_code}): {url}")

    found = []
    for tile in tiles:
        for line in resp.text.splitlines():
            if tile not in line or ".hdf" not in line or ".hdf.xml" in line:
                continue
            fname = line.split('"')[1] if '"' in line else None
            if fname:
                found.append({"tile": tile, "file_name": fname, "download_url": url + fname})
                break
    return found


def _download_with_retry(url: str, out_path: Path) -> str:
    """Download `url` to `out_path`, retrying up to MAX_RETRIES times on
    network error or truncated transfer. Returns the file's MD5 checksum.
    Skips the download entirely if `out_path` already exists on disk."""
    import requests

    if out_path.exists() and out_path.stat().st_size > 0:
        logger.info("[M7] sudah ada di disk, lewati download: %s", out_path.name)
        return _md5(out_path)

    tmp_path = out_path.with_suffix(out_path.suffix + ".part")
    last_exc: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with requests.get(url, headers=_auth_headers(), stream=True, timeout=300) as r:
                r.raise_for_status()
                expected_size = int(r.headers.get("Content-Length", 0))
                downloaded = 0
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                        f.write(chunk)
                        downloaded += len(chunk)

            if expected_size and downloaded != expected_size:
                raise IOError(
                    f"ukuran file tidak sesuai: got {downloaded} bytes, expected {expected_size}"
                )

            tmp_path.rename(out_path)
            checksum = _md5(out_path)
            logger.info("[M7] downloaded %s (md5=%s...)", out_path.name, checksum[:12])
            return checksum

        except Exception as exc:
            last_exc = exc
            logger.warning(
                "[M7] download gagal (attempt %d/%d) %s: %s",
                attempt, MAX_RETRIES, out_path.name, exc,
            )
            tmp_path.unlink(missing_ok=True)
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)

    raise RuntimeError(f"gagal download {url} setelah {MAX_RETRIES} percobaan: {last_exc}")


def _hdf_subdataset_to_geotiff(
    hdf_path: Path,
    subdataset: str,
    output_path: Path,
    dst_crs: str = DST_CRS,
) -> Path:
    src_path = f'HDF4_EOS:EOS_GRID:"{hdf_path}":{subdataset}'
    with rasterio.open(src_path) as src:
        transform, width, height = calculate_default_transform(
            src.crs, dst_crs, src.width, src.height, *src.bounds
        )
        kwargs = src.meta.copy()
        kwargs.update({
            "driver": "GTiff",
            "crs": dst_crs,
            "transform": transform,
            "width": width,
            "height": height,
        })
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
    return output_path


def _mosaic_and_crop(
    tile_tif_paths: list[Path],
    aoi_bbox: tuple[float, float, float, float],
    output_path: Path,
) -> Path:
    """Merge per-tile GeoTIFFs (already reprojected to DST_CRS) and crop
    the mosaic to `aoi_bbox`, matching Sentinel-1 resolution/projection."""
    from rasterio.mask import mask
    from shapely.geometry import box, mapping

    srcs = [rasterio.open(p) for p in tile_tif_paths]
    try:
        mosaic, out_transform = merge(srcs)
        meta = srcs[0].meta.copy()
    finally:
        for s in srcs:
            s.close()

    meta.update({
        "driver": "GTiff",
        "height": mosaic.shape[1],
        "width": mosaic.shape[2],
        "transform": out_transform,
    })
    mosaic_path = output_path.with_name(output_path.stem + "_mosaic.tif")
    with rasterio.open(mosaic_path, "w", **meta) as dst:
        dst.write(mosaic)

    geom = mapping(box(*aoi_bbox))
    with rasterio.open(mosaic_path) as src:
        out_image, crop_transform = mask(src, [geom], crop=True)
        crop_meta = src.meta.copy()
        crop_meta.update({
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": crop_transform,
        })
        with rasterio.open(output_path, "w", **crop_meta) as dst:
            dst.write(out_image)

    mosaic_path.unlink(missing_ok=True)
    return output_path


def download_modis_scene(
    dataset_id: int,
    date_start: datetime,
    date_end: datetime,
    aoi_bbox: tuple[float, float, float, float] = JABODETABEK_BBOX,
    tiles: list[str] = MODIS_TILES,
) -> tuple[str, dict]:
    """
    Download the MODIS MCDWD flood product from NASA LAADS DAAC for every
    day in [date_start, date_end], reproject/crop each day to `aoi_bbox`,
    and write GeoTIFFs to data/datasets/{dataset_id}/gold/modis_{date}_flood.tif.

    Returns:
        (product_id, metadata_dict) — product_id identifies the source NASA
        product for lineage tracking; metadata_dict carries per-day output
        paths and MD5 checksums.
    """
    gold_dir = Path("data") / "datasets" / str(dataset_id) / "gold"
    raw_dir = Path("data") / "datasets" / str(dataset_id) / "raw" / "modis"
    gold_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)

    daily_outputs = []
    for date in _daterange(date_start, date_end):
        date_key = date.strftime("%Y%m%d")
        out_path = gold_dir / f"modis_{date_key}_flood.tif"

        if out_path.exists():
            logger.info("[M7] output sudah ada, skip: %s", out_path.name)
            daily_outputs.append({
                "date": date.date().isoformat(),
                "flood_path": str(out_path),
                "checksum_md5": _md5(out_path),
                "skipped": True,
            })
            continue

        items = _discover_tile_files(date, tiles)
        if not items:
            logger.warning("[M7] tidak ada granule MCDWD untuk tanggal %s", date.date().isoformat())
            continue

        tile_tifs = []
        source_checksums = {}
        for item in items:
            hdf_path = raw_dir / item["file_name"]
            source_checksums[item["tile"]] = _download_with_retry(item["download_url"], hdf_path)

            tile_tif = raw_dir / f"{Path(item['file_name']).stem}_flood.tif"
            _hdf_subdataset_to_geotiff(hdf_path, "Flood 1-day 250m", tile_tif)
            tile_tifs.append(tile_tif)

        _mosaic_and_crop(tile_tifs, aoi_bbox, out_path)

        daily_outputs.append({
            "date": date.date().isoformat(),
            "flood_path": str(out_path),
            "checksum_md5": _md5(out_path),
            "source_tiles": source_checksums,
            "skipped": False,
        })

    if not daily_outputs:
        raise RuntimeError(
            f"tidak ada produk MCDWD ditemukan untuk rentang "
            f"{date_start.date()}..{date_end.date()} di tiles {tiles}"
        )

    product_id = (
        f"{MODIS_PRODUCT}.{date_start.strftime('%Y%m%d')}_{date_end.strftime('%Y%m%d')}"
        ".jabodetabek"
    )
    metadata = {
        "product": MODIS_PRODUCT,
        "dataset_id": dataset_id,
        "date_start": date_start.date().isoformat(),
        "date_end": date_end.date().isoformat(),
        "aoi_bbox": aoi_bbox,
        "tiles": tiles,
        "crs": DST_CRS,
        "outputs": daily_outputs,
    }

    logger.info("[M7] selesai: %d hari diproses untuk dataset_id=%s", len(daily_outputs), dataset_id)
    return product_id, metadata
