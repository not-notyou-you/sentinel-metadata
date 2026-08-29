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

from etl import folder_manager as fm
from etl.pipeline_logger import PipelineLogger

logger = logging.getLogger(__name__)

MODIS_PRODUCT = "MCDWD_L3_F2_NRT"
LAADS_BASE = f"https://nrt3.modaps.eosdis.nasa.gov/archive/allData/61/{MODIS_PRODUCT}"
MODIS_TILES = ["h30v08", "h31v08"]
MODULE = "MODULE7_MODIS_DOWNLOAD"

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


def _plog_event(
    plog: PipelineLogger | None,
    dataset_id: int | None,
    scene_id: str,
    stage: str,
    status: str,
    message: str,
    details: dict | None = None,
) -> None:
    if plog is None or dataset_id is None:
        return
    plog.log_event(dataset_id, scene_id, MODULE, stage, status, message, details or {})


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


def _download_with_retry(
    url: str,
    out_path: Path,
    *,
    plog: PipelineLogger | None = None,
    dataset_id: int | None = None,
    scene_id: str = "",
    item_label: str = "",
) -> str:
    """Download `url` to `out_path`, retrying up to MAX_RETRIES times on
    network error or truncated transfer. Returns the file's MD5 checksum.
    Skips the download entirely if `out_path` already exists on disk.

    When `plog`/`dataset_id` are given, emits a RUNNING event per attempt
    (with periodic progress ticks), a terminal FAILED event only once all
    retries are exhausted, and a COMPLETED event on success."""
    import requests

    if out_path.exists() and out_path.stat().st_size > 0:
        logger.info("[M7] sudah ada di disk, lewati download: %s", out_path.name)
        return _md5(out_path)

    tmp_path = out_path.with_suffix(out_path.suffix + ".part")
    last_exc: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        attempt_started = time.monotonic()
        _plog_event(
            plog, dataset_id, scene_id, "DOWNLOAD", "RUNNING",
            f"{item_label}: downloading (attempt {attempt}/{MAX_RETRIES})",
            {"item": item_label, "attempt": attempt, "max_retries": MAX_RETRIES, "url": url},
        )
        try:
            with requests.get(url, headers=_auth_headers(), stream=True, timeout=300) as r:
                r.raise_for_status()
                expected_size = int(r.headers.get("Content-Length", 0))
                downloaded = 0
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                        f.write(chunk)
                        downloaded += len(chunk)
                        if expected_size and downloaded % (50 * 1024 * 1024) < 8 * 1024 * 1024:
                            _plog_event(
                                plog, dataset_id, scene_id, "DOWNLOAD", "RUNNING",
                                f"{item_label}: {downloaded / 1e6:.0f}/{expected_size / 1e6:.0f} MB",
                                {
                                    "item": item_label, "attempt": attempt, "max_retries": MAX_RETRIES,
                                    "progress_percent": round(downloaded / expected_size * 100, 1),
                                },
                            )

            if expected_size and downloaded != expected_size:
                raise IOError(
                    f"ukuran file tidak sesuai: got {downloaded} bytes, expected {expected_size}"
                )

            tmp_path.rename(out_path)
            checksum = _md5(out_path)
            logger.info("[M7] downloaded %s (md5=%s...)", out_path.name, checksum[:12])
            _plog_event(
                plog, dataset_id, scene_id, "DOWNLOAD", "COMPLETED",
                f"{item_label}: downloaded",
                {
                    "item": item_label, "attempt": attempt, "file_name": out_path.name,
                    "file_size_mb": round(downloaded / (1024 ** 2), 2), "checksum_md5": checksum,
                    "duration_seconds": round(time.monotonic() - attempt_started, 3),
                },
            )
            return checksum

        except Exception as exc:
            last_exc = exc
            logger.warning(
                "[M7] download gagal (attempt %d/%d) %s: %s",
                attempt, MAX_RETRIES, out_path.name, exc,
            )
            tmp_path.unlink(missing_ok=True)
            is_final = attempt == MAX_RETRIES
            _plog_event(
                plog, dataset_id, scene_id, "DOWNLOAD", "FAILED" if is_final else "RUNNING",
                f"{item_label}: attempt {attempt}/{MAX_RETRIES} failed ({exc})",
                {
                    "item": item_label, "attempt": attempt, "max_retries": MAX_RETRIES,
                    "error_type": type(exc).__name__, "error_message": str(exc),
                    "duration_seconds": round(time.monotonic() - attempt_started, 3),
                },
            )
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
    dataset_name: str,
    date_start: datetime,
    date_end: datetime,
    aoi_bbox: tuple[float, float, float, float] = JABODETABEK_BBOX,
    tiles: list[str] = MODIS_TILES,
    plog: PipelineLogger | None = None,
) -> tuple[str, dict]:
    """
    Download the MODIS MCDWD flood product from NASA LAADS DAAC for every
    day in [date_start, date_end], reproject/crop each day to `aoi_bbox`,
    and write GeoTIFFs to
    data/datasets/{id}_{slug}/silver/{date}/modis_{date}_flood.tif
    (these are fusion *inputs*, consumed by module9_fusion.py — not a GOLD
    deliverable themselves).

    A day whose tile listing/download/mosaic fails is logged and skipped
    rather than aborting the whole date range; a day with some (not all)
    tiles missing still produces a degraded mosaic from the tiles that did
    download. Pass `plog` to also emit structured per-tile/per-day/summary
    events to the `processing_logs` table (visible in the live UI panel).

    Returns:
        (product_id, metadata_dict) — product_id identifies the source NASA
        product for lineage tracking; metadata_dict carries per-day output
        paths, MD5 checksums, and an overall `quality`/`failed_days` summary.
    """
    raw_dir = fm.get_aux_raw_dir(dataset_id, dataset_name, "modis")
    raw_dir.mkdir(parents=True, exist_ok=True)

    daily_outputs = []
    failed_days: list[dict] = []
    for date in _daterange(date_start, date_end):
        date_key = date.strftime("%Y%m%d")
        scene_label = f"MODIS_{date_key}"
        silver_dir = fm.get_scene_dir(dataset_id, dataset_name, "silver", date_key)
        silver_dir.mkdir(parents=True, exist_ok=True)
        out_path = silver_dir / f"modis_{date_key}_flood.tif"

        if out_path.exists():
            logger.info("[M7] output sudah ada, skip: %s", out_path.name)
            daily_outputs.append({
                "date": date.date().isoformat(),
                "flood_path": str(out_path),
                "checksum_md5": _md5(out_path),
                "skipped": True,
                "degraded": False,
                "failed_tiles": [],
            })
            continue

        try:
            items = _discover_tile_files(date, tiles)
        except Exception as exc:
            logger.warning("[M7] gagal listing granule tanggal %s: %s", date.date().isoformat(), exc)
            _plog_event(
                plog, dataset_id, scene_label, "DOWNLOAD", "FAILED",
                f"MODIS {date_key}: gagal listing granule LAADS ({exc})",
                {"date": date.date().isoformat(), "error_type": type(exc).__name__, "error_message": str(exc)},
            )
            failed_days.append({"date": date.date().isoformat(), "reason": f"listing failed: {exc}"})
            continue

        if not items:
            logger.warning("[M7] tidak ada granule MCDWD untuk tanggal %s", date.date().isoformat())
            continue

        tile_tifs = []
        source_checksums = {}
        failed_tiles = []
        for item in items:
            try:
                hdf_path = raw_dir / item["file_name"]
                source_checksums[item["tile"]] = _download_with_retry(
                    item["download_url"], hdf_path,
                    plog=plog, dataset_id=dataset_id, scene_id=scene_label,
                    item_label=f"tile {item['tile']}",
                )
                tile_tif = raw_dir / f"{Path(item['file_name']).stem}_flood.tif"
                _hdf_subdataset_to_geotiff(hdf_path, "Flood 1-day 250m", tile_tif)
                tile_tifs.append(tile_tif)
            except Exception as exc:
                logger.warning("[M7] tile %s gagal (tanggal %s): %s", item["tile"], date.date().isoformat(), exc)
                failed_tiles.append(item["tile"])

        if not tile_tifs:
            logger.warning("[M7] semua tile gagal untuk tanggal %s, skip hari ini", date.date().isoformat())
            _plog_event(
                plog, dataset_id, scene_label, "DOWNLOAD", "FAILED",
                f"MODIS {date_key}: semua tile gagal ({', '.join(failed_tiles)})",
                {"date": date.date().isoformat(), "failed_tiles": failed_tiles},
            )
            failed_days.append({"date": date.date().isoformat(), "reason": f"all tiles failed: {failed_tiles}"})
            continue

        try:
            _mosaic_and_crop(tile_tifs, aoi_bbox, out_path)
        except Exception as exc:
            logger.warning("[M7] mosaic gagal tanggal %s: %s", date.date().isoformat(), exc)
            _plog_event(
                plog, dataset_id, scene_label, "DOWNLOAD", "FAILED",
                f"MODIS {date_key}: mosaic/crop gagal ({exc})",
                {"date": date.date().isoformat(), "error_type": type(exc).__name__, "error_message": str(exc)},
            )
            failed_days.append({"date": date.date().isoformat(), "reason": f"mosaic failed: {exc}"})
            continue

        degraded = bool(failed_tiles)
        daily_outputs.append({
            "date": date.date().isoformat(),
            "flood_path": str(out_path),
            "checksum_md5": _md5(out_path),
            "source_tiles": source_checksums,
            "skipped": False,
            "degraded": degraded,
            "failed_tiles": failed_tiles,
        })
        _plog_event(
            plog, dataset_id, scene_label, "DOWNLOAD", "COMPLETED",
            f"MODIS {date_key}: {'selesai (degraded)' if degraded else 'selesai'} "
            f"({len(tile_tifs)}/{len(items)} tile)",
            {
                "date": date.date().isoformat(), "tiles_ok": len(tile_tifs), "tiles_total": len(items),
                "degraded": degraded, "failed_tiles": failed_tiles,
            },
        )

    if not daily_outputs:
        raise RuntimeError(
            f"tidak ada produk MCDWD ditemukan untuk rentang "
            f"{date_start.date()}..{date_end.date()} di tiles {tiles}"
            + (f" (gagal: {failed_days})" if failed_days else "")
        )

    degraded_days = sum(1 for d in daily_outputs if d.get("degraded"))
    quality = "GOOD" if not failed_days and not degraded_days else "DEGRADED"
    total_days = len(daily_outputs) + len(failed_days)
    _plog_event(
        plog, dataset_id, f"MODIS_{date_start.strftime('%Y%m%d')}_{date_end.strftime('%Y%m%d')}",
        "DOWNLOAD_SUMMARY", "COMPLETED",
        f"MODIS selesai: {len(daily_outputs)}/{total_days} hari berhasil"
        + (f", {len(failed_days)} gagal" if failed_days else "")
        + (f", {degraded_days} degraded" if degraded_days else ""),
        {
            "days_ok": len(daily_outputs), "days_degraded": degraded_days,
            "days_failed": len(failed_days), "failed_days": failed_days, "quality": quality,
        },
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
        "quality": quality,
        "failed_days": failed_days,
    }

    logger.info("[M7] selesai: %d hari diproses untuk dataset_id=%s", len(daily_outputs), dataset_id)
    return product_id, metadata
