# etl/module8_gpm_download.py
"""
Downloads NASA GES DISC GPM IMERG Final (GPM_3IMERGDF) daily rainfall product,
aggregates it into 24h/72h/7-day accumulation windows, reprojects/crops it to
the dataset AOI at the Sentinel-1 grid resolution, and writes one GeoTIFF per
window for lineage tracking.
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
from rasterio.io import MemoryFile
from rasterio.mask import mask
from rasterio.warp import calculate_default_transform, reproject
from shapely.geometry import box, mapping

logger = logging.getLogger(__name__)

IMERG_PRODUCT = "GPM_3IMERGDF"
IMERG_VERSION = "07"
GES_DISC_BASE = f"https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/{IMERG_PRODUCT}.{IMERG_VERSION}"
IMERG_SUBDATASET = "precipitation"  # mm, HDF5/NetCDF variable name

# Jabodetabek bounding box, WGS84 (min_lon, min_lat, max_lon, max_lat)
JABODETABEK_BBOX = (106.4, -6.7, 107.2, -5.9)

DST_CRS = "EPSG:4326"
S1_RESOLUTION_M = 10
S1_RESOLUTION_DEG = S1_RESOLUTION_M / 111_320.0  # meters -> degrees at the equator
MAX_RETRIES = 3
DEFAULT_NODATA = -9999.9

# window name -> number of trailing days to accumulate, ending on the target date
WINDOWS = {
    "24h": 1,
    "72h": 3,
    "7d": 7,
}


def _auth_headers() -> dict:
    token = os.getenv("NASA_EARTHDATA_TOKEN")
    if not token:
        raise RuntimeError(
            "NASA_EARTHDATA_TOKEN belum diset. Generate app token di "
            "urs.earthdata.nasa.gov -> Generate Token."
        )
    return {"Authorization": f"Bearer {token}"}


def _md5(path: Path, chunk: int = 8 * 1024 * 1024) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def _daily_granule_filename(date: datetime) -> str:
    date_str = date.strftime("%Y%m%d")
    return f"3B-DAY.MS.MRG.3IMERG.{date_str}-S000000-E235959.V{IMERG_VERSION}B.nc4"


def _daily_granule_url(date: datetime) -> str:
    return f"{GES_DISC_BASE}/{date.year}/{date.month:02d}/{_daily_granule_filename(date)}"


def _download_with_retry(url: str, out_path: Path) -> str:
    """Download `url` to `out_path`, retrying up to MAX_RETRIES times on
    network error or truncated transfer. Returns the file's MD5 checksum.
    Skips the download entirely if `out_path` already exists on disk."""
    import requests

    if out_path.exists() and out_path.stat().st_size > 0:
        logger.info("[M8] sudah ada di disk, lewati download: %s", out_path.name)
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
            logger.info("[M8] downloaded %s (md5=%s...)", out_path.name, checksum[:12])
            return checksum

        except Exception as exc:
            last_exc = exc
            logger.warning(
                "[M8] download gagal (attempt %d/%d) %s: %s",
                attempt, MAX_RETRIES, out_path.name, exc,
            )
            tmp_path.unlink(missing_ok=True)
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)

    raise RuntimeError(f"gagal download {url} setelah {MAX_RETRIES} percobaan: {last_exc}")


def _read_daily_precip(nc4_path: Path):
    """Read the daily precipitation band (mm/day) from an IMERG NetCDF granule.
    Nodata pixels are filled with 0 mm so they contribute nothing to the
    accumulation sums downstream."""
    src_path = f'NETCDF:"{nc4_path}":{IMERG_SUBDATASET}'
    with rasterio.open(src_path) as src:
        data = src.read(1).astype("float64")
        transform = src.transform
        crs = str(src.crs) if src.crs else DST_CRS
        nodata = src.nodata if src.nodata is not None else DEFAULT_NODATA
        data[data == nodata] = 0.0
    return data, transform, crs


def _fetch_daily_precip(date: datetime, raw_dir: Path) -> tuple:
    filename = _daily_granule_filename(date)
    nc4_path = raw_dir / filename
    checksum = _download_with_retry(_daily_granule_url(date), nc4_path)
    data, transform, crs = _read_daily_precip(nc4_path)
    return data, transform, crs, checksum


def _accumulate_window(end_date: datetime, num_days: int, raw_dir: Path) -> tuple:
    """Sum daily IMERG rainfall over the `num_days` ending on `end_date`
    (inclusive). All daily granules share the same fixed global grid, so the
    per-pixel sums line up without any resampling at this stage."""
    accum = None
    transform = crs = None
    source_checksums = {}

    for offset in range(num_days):
        day = end_date - timedelta(days=offset)
        data, day_transform, day_crs, checksum = _fetch_daily_precip(day, raw_dir)
        source_checksums[day.date().isoformat()] = checksum

        if accum is None:
            accum = data
            transform, crs = day_transform, day_crs
        else:
            accum = accum + data

    return accum, transform, crs, source_checksums


def _reproject_and_crop_to_s1_grid(
    accum,
    src_transform,
    src_crs: str,
    aoi_bbox: tuple[float, float, float, float],
    output_path: Path,
) -> Path:
    """Reproject the accumulated rainfall grid to the Sentinel-1 target
    resolution/CRS and crop it to the AOI, matching module7's mosaic/crop
    pattern for MODIS."""
    height, width = accum.shape

    with MemoryFile() as memfile:
        with memfile.open(
            driver="GTiff", height=height, width=width, count=1,
            dtype="float64", crs=src_crs, transform=src_transform,
            nodata=DEFAULT_NODATA,
        ) as tmp:
            tmp.write(accum, 1)

        with memfile.open() as src:
            dst_transform, dst_width, dst_height = calculate_default_transform(
                src.crs, DST_CRS, src.width, src.height, *src.bounds,
                resolution=S1_RESOLUTION_DEG,
            )
            kwargs = src.meta.copy()
            kwargs.update({
                "driver": "GTiff",
                "crs": DST_CRS,
                "transform": dst_transform,
                "width": dst_width,
                "height": dst_height,
            })
            reproj_path = output_path.with_name(output_path.stem + "_reproj.tif")
            with rasterio.open(reproj_path, "w", **kwargs) as dst:
                reproject(
                    source=rasterio.band(src, 1),
                    destination=rasterio.band(dst, 1),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=dst_transform,
                    dst_crs=DST_CRS,
                    resampling=Resampling.bilinear,
                )

    geom = mapping(box(*aoi_bbox))
    with rasterio.open(reproj_path) as src:
        out_image, crop_transform = mask(src, [geom], crop=True, nodata=DEFAULT_NODATA)
        crop_meta = src.meta.copy()
        crop_meta.update({
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": crop_transform,
        })
        with rasterio.open(output_path, "w", **crop_meta) as dst:
            dst.write(out_image)

    reproj_path.unlink(missing_ok=True)
    return output_path


def download_gpm_scene(
    dataset_id: int,
    date: datetime,
    aoi_bbox: tuple[float, float, float, float] = JABODETABEK_BBOX,
) -> tuple[list[str], dict]:
    """
    Build 24h/72h/7-day rainfall accumulation GeoTIFFs for `date` from NASA
    GES DISC GPM IMERG Final daily granules, reprojected/cropped to `aoi_bbox`
    at the Sentinel-1 grid resolution.

    Writes:
        data/datasets/{dataset_id}/gold/gpm_rain_24h_{date}.tif
        data/datasets/{dataset_id}/gold/gpm_rain_72h_{date}.tif
        data/datasets/{dataset_id}/gold/gpm_rain_7d_{date}.tif

    Returns:
        (product_ids, metadata_dict) — product_ids is [id_24h, id_72h, id_7d]
        for lineage tracking; metadata_dict carries per-window output paths,
        checksums, and the source daily granules each window was built from.
    """
    gold_dir = Path("data") / "datasets" / str(dataset_id) / "gold"
    raw_dir = Path("data") / "datasets" / str(dataset_id) / "raw" / "gpm"
    gold_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)

    date_key = date.strftime("%Y%m%d")
    product_ids = []
    window_outputs = {}

    for window_name, num_days in WINDOWS.items():
        out_path = gold_dir / f"gpm_rain_{window_name}_{date_key}.tif"
        product_id = f"{IMERG_PRODUCT}.{window_name}.{date_key}.jabodetabek"

        if out_path.exists():
            logger.info("[M8] output sudah ada, skip: %s", out_path.name)
            window_outputs[window_name] = {
                "path": str(out_path),
                "checksum_md5": _md5(out_path),
                "days_aggregated": num_days,
                "skipped": True,
            }
            product_ids.append(product_id)
            continue

        accum, transform, crs, source_checksums = _accumulate_window(date, num_days, raw_dir)
        _reproject_and_crop_to_s1_grid(accum, transform, crs, aoi_bbox, out_path)

        window_outputs[window_name] = {
            "path": str(out_path),
            "checksum_md5": _md5(out_path),
            "days_aggregated": num_days,
            "source_checksums": source_checksums,
            "skipped": False,
        }
        product_ids.append(product_id)

    metadata = {
        "product": IMERG_PRODUCT,
        "dataset_id": dataset_id,
        "date": date.date().isoformat(),
        "aoi_bbox": aoi_bbox,
        "crs": DST_CRS,
        "resolution_m": S1_RESOLUTION_M,
        "windows": window_outputs,
    }

    logger.info(
        "[M8] selesai: 3 produk rainfall dibuat untuk dataset_id=%s tanggal=%s",
        dataset_id, date.date().isoformat(),
    )
    return product_ids, metadata
