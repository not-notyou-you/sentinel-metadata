# etl/module9_fusion.py
"""
Builds a multi-modal HDF5 feature stack (Sentinel-1 SAR + MODIS flood +
GPM rainfall) aligned to a common grid for ML training.

Sentinel-1 GOLD products (VV/VH) are looked up in the `data_products` table
and define the reference grid. MODIS/GPM GOLD GeoTIFFs are discovered on
disk under data/datasets/{dataset_id}/gold/ (written by module7/module8),
matched to the S1 acquisition time within a 24h window, reprojected onto
the S1 grid, and registered as `nasa_scenes` rows. The resulting stack is
recorded as a `fusion_products` row so `fusion_id` can be used for lineage.
"""

from __future__ import annotations

import logging
from datetime import date as date_type, datetime, timedelta, timezone
from pathlib import Path

import h5py
import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.warp import reproject
from sqlalchemy import select

from etl.database_client import (
    DatabaseClient,
    DataProduct,
    FusionProduct,
    NasaScene,
    ProductTierEnum,
    SatelliteScene,
)
from etl.lineage_tracker import LineageTracker

logger = logging.getLogger(__name__)

ALIGNMENT_WINDOW_HOURS = 24
MODIS_SOURCE = "MODIS"
MODIS_PRODUCT_SHORT_NAME = "MCDWD_L3_F2_NRT"
MODIS_TILE_ID = "MOSAIC"
GPM_SOURCE = "GPM"
GPM_PRODUCT_SHORT_NAME = "GPM_3IMERGDF"
GPM_TILE_ID = "GLOBAL"
MODIS_NODATA_U8 = 255  # uint8 can't hold NaN; 255 marks a missing/nodata pixel
HDF5_CHUNK_MAX = 256


def _find_s1_gold(db: DatabaseClient, s1_date: date_type) -> dict | None:
    """Find the Sentinel-1 GOLD VV/VH products for the scene acquired on
    `s1_date`. Returns None if no scene was acquired that day."""
    day_start = datetime.combine(s1_date, datetime.min.time(), tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)

    with db.session() as sess:
        scene = sess.scalar(
            select(SatelliteScene)
            .where(
                SatelliteScene.acquisition_datetime >= day_start,
                SatelliteScene.acquisition_datetime < day_end,
            )
            .order_by(SatelliteScene.acquisition_datetime)
        )
        if scene is None:
            return None

        def _gold_band(band: str) -> DataProduct | None:
            return sess.scalar(
                select(DataProduct).where(
                    DataProduct.scene_id == scene.scene_id,
                    DataProduct.product_tier == ProductTierEnum.GOLD,
                    DataProduct.band_name == band,
                    DataProduct.is_latest == True,
                    DataProduct.is_valid == True,
                )
            )

        vv = _gold_band("VV")
        vh = _gold_band("VH")

        return {
            "scene_id": scene.scene_id,
            "region_id": scene.region_id,
            "acquisition_datetime": scene.acquisition_datetime,
            "vv_product_id": vv.product_id if vv else None,
            "vv_path": vv.file_path if vv else None,
            "vh_product_id": vh.product_id if vh else None,
            "vh_path": vh.file_path if vh else None,
        }


def _find_nearest_daily_file(
    gold_dir: Path,
    filename_fn,
    center_dt: datetime,
    tolerance_hours: int = ALIGNMENT_WINDOW_HOURS // 2,
) -> tuple[Path, date_type] | None:
    """MODIS/GPM GOLD files are stamped one-per-day at local midnight. Return
    the (path, date) of the closest candidate day whose midnight falls within
    `tolerance_hours` of `center_dt`, or None if nothing is on disk."""
    best: tuple[Path, date_type] | None = None
    best_diff = None
    for offset in (0, -1, 1):
        candidate_date = (center_dt + timedelta(days=offset)).date()
        candidate_midnight = datetime.combine(candidate_date, datetime.min.time(),
                                              tzinfo=center_dt.tzinfo)
        diff_hours = abs((candidate_midnight - center_dt).total_seconds()) / 3600.0
        if diff_hours > tolerance_hours:
            continue
        path = gold_dir / filename_fn(candidate_date)
        if path.exists() and (best_diff is None or diff_hours < best_diff):
            best, best_diff = (path, candidate_date), diff_hours
    return best


def _read_band_or_nan(path: str | None, shape: tuple[int, int]) -> np.ndarray:
    if not path or not Path(path).exists():
        return np.full(shape, np.nan, dtype=np.float32)
    with rasterio.open(path) as src:
        data = src.read(1).astype(np.float32)
        if src.nodata is not None:
            data[data == src.nodata] = np.nan
    return data


def _reproject_to_grid(
    src_path: Path,
    ref_transform,
    ref_crs,
    ref_shape: tuple[int, int],
    resampling: Resampling,
    fill_value: float,
) -> np.ndarray:
    """Reproject a single-band raster onto the S1 reference grid, filling
    pixels outside the source extent with `fill_value`."""
    height, width = ref_shape
    dest = np.full((height, width), fill_value, dtype=np.float32)
    with rasterio.open(src_path) as src:
        reproject(
            source=rasterio.band(src, 1),
            destination=dest,
            src_transform=src.transform,
            src_crs=src.crs,
            src_nodata=src.nodata,
            dst_transform=ref_transform,
            dst_crs=ref_crs,
            dst_nodata=fill_value,
            resampling=resampling,
        )
    return dest


def _get_or_create_nasa_scene(
    db: DatabaseClient,
    source: str,
    tile_id: str,
    product_short_name: str,
    acquisition_date: date_type,
    region_id: int,
    file_path: Path,
) -> int:
    with db.session() as sess:
        existing = sess.scalar(
            select(NasaScene.nasa_scene_id).where(
                NasaScene.source == source,
                NasaScene.tile_id == tile_id,
                NasaScene.product_short_name == product_short_name,
                NasaScene.acquisition_date == acquisition_date,
            )
        )
        if existing:
            return existing

        scene = NasaScene(
            source=source,
            tile_id=tile_id,
            product_short_name=product_short_name,
            acquisition_date=acquisition_date,
            region_id=region_id,
            raw_file_path=str(file_path),
            is_available=True,
        )
        sess.add(scene)
        sess.flush()
        return scene.nasa_scene_id


def _write_fusion_h5(
    h5_path: Path,
    s1_vv: np.ndarray,
    s1_vh: np.ndarray,
    modis_flood: np.ndarray,
    gpm_24h: np.ndarray,
    gpm_72h: np.ndarray,
    acquisition_datetime: datetime,
    processing_datetime: datetime,
    aoi_bbox: tuple[float, float, float, float],
) -> None:
    height, width = s1_vv.shape
    chunks = (min(HDF5_CHUNK_MAX, height), min(HDF5_CHUNK_MAX, width))

    h5_path.parent.mkdir(parents=True, exist_ok=True)
    with h5py.File(h5_path, "w") as f:
        f.create_dataset("s1_vv", data=s1_vv, dtype="float32", chunks=chunks, compression="gzip")
        f.create_dataset("s1_vh", data=s1_vh, dtype="float32", chunks=chunks, compression="gzip")
        modis_ds = f.create_dataset("modis_flood", data=modis_flood, dtype="uint8",
                                    chunks=chunks, compression="gzip")
        modis_ds.attrs["nodata"] = MODIS_NODATA_U8
        f.create_dataset("gpm_rainfall_24h", data=gpm_24h, dtype="float32", chunks=chunks, compression="gzip")
        f.create_dataset("gpm_rainfall_72h", data=gpm_72h, dtype="float32", chunks=chunks, compression="gzip")

        f.attrs["acquisition_datetime"] = acquisition_datetime.isoformat()
        f.attrs["processing_datetime"] = processing_datetime.isoformat()
        f.attrs["aoi_bbox"] = list(aoi_bbox)

    logger.info("[M9] wrote %s shape=(%d, %d)", h5_path, height, width)


def create_fusion_stack(
    dataset_id: int,
    s1_date: date_type,
    aoi_bbox: tuple[float, float, float, float],
    db: DatabaseClient | None = None,
) -> int:
    """
    Build a fused HDF5 feature stack for the Sentinel-1 scene acquired on
    `s1_date`, aligning MODIS flood and GPM rainfall GOLD products found
    within 24h of the S1 acquisition time.

    Writes:
        data/datasets/{dataset_id}/fusion/fusion_{s1_date}.h5

    Returns:
        fusion_id (int) — primary key of the `fusion_products` row, used
        for lineage tracking.
    """
    owns_db = db is None
    db = db or DatabaseClient.from_env()
    lineage = LineageTracker(db)

    try:
        s1 = _find_s1_gold(db, s1_date)
        if s1 is None or (not s1["vv_path"] and not s1["vh_path"]):
            raise RuntimeError(f"No S1 GOLD product found for s1_date={s1_date.isoformat()}")

        ref_path = s1["vv_path"] or s1["vh_path"]
        with rasterio.open(ref_path) as ref:
            ref_transform, ref_crs = ref.transform, ref.crs
            ref_shape = (ref.height, ref.width)

        s1_vv = _read_band_or_nan(s1["vv_path"], ref_shape)
        s1_vh = _read_band_or_nan(s1["vh_path"], ref_shape)
        if s1["vv_path"] is None:
            logger.warning("[M9] S1 VV missing for scene=%s, filled with NaN", s1["scene_id"])
        if s1["vh_path"] is None:
            logger.warning("[M9] S1 VH missing for scene=%s, filled with NaN", s1["scene_id"])

        gold_dir = Path("data") / "datasets" / str(dataset_id) / "gold"
        center_dt = s1["acquisition_datetime"]

        modis_hit = _find_nearest_daily_file(
            gold_dir, lambda d: f"modis_{d.strftime('%Y%m%d')}_flood.tif", center_dt,
        )
        gpm24_hit = _find_nearest_daily_file(
            gold_dir, lambda d: f"gpm_rain_24h_{d.strftime('%Y%m%d')}.tif", center_dt,
        )
        gpm72_hit = _find_nearest_daily_file(
            gold_dir, lambda d: f"gpm_rain_72h_{d.strftime('%Y%m%d')}.tif", center_dt,
        )

        if modis_hit:
            modis_flood_f32 = _reproject_to_grid(
                modis_hit[0], ref_transform, ref_crs, ref_shape,
                resampling=Resampling.nearest, fill_value=np.nan,
            )
            modis_flood = np.where(np.isnan(modis_flood_f32), MODIS_NODATA_U8,
                                   modis_flood_f32).astype("uint8")
        else:
            logger.warning("[M9] no MODIS GOLD product within %dh of %s", ALIGNMENT_WINDOW_HOURS, center_dt)
            modis_flood = np.full(ref_shape, MODIS_NODATA_U8, dtype="uint8")

        if gpm24_hit:
            gpm_24h = _reproject_to_grid(
                gpm24_hit[0], ref_transform, ref_crs, ref_shape,
                resampling=Resampling.bilinear, fill_value=np.nan,
            )
        else:
            logger.warning("[M9] no GPM 24h GOLD product within %dh of %s", ALIGNMENT_WINDOW_HOURS, center_dt)
            gpm_24h = np.full(ref_shape, np.nan, dtype="float32")

        if gpm72_hit:
            gpm_72h = _reproject_to_grid(
                gpm72_hit[0], ref_transform, ref_crs, ref_shape,
                resampling=Resampling.bilinear, fill_value=np.nan,
            )
        else:
            logger.warning("[M9] no GPM 72h GOLD product within %dh of %s", ALIGNMENT_WINDOW_HOURS, center_dt)
            gpm_72h = np.full(ref_shape, np.nan, dtype="float32")

        out_dir = Path("data") / "datasets" / str(dataset_id) / "fusion"
        date_key = s1_date.strftime("%Y%m%d")
        h5_path = out_dir / f"fusion_{date_key}.h5"
        processing_dt = datetime.now(tz=timezone.utc)

        _write_fusion_h5(
            h5_path, s1_vv, s1_vh, modis_flood, gpm_24h, gpm_72h,
            acquisition_datetime=center_dt, processing_datetime=processing_dt,
            aoi_bbox=aoi_bbox,
        )

        modis_scene_id = None
        if modis_hit:
            modis_scene_id = _get_or_create_nasa_scene(
                db, MODIS_SOURCE, MODIS_TILE_ID, MODIS_PRODUCT_SHORT_NAME,
                modis_hit[1], s1["region_id"], modis_hit[0],
            )
        gpm_scene_id = None
        if gpm24_hit:
            gpm_scene_id = _get_or_create_nasa_scene(
                db, GPM_SOURCE, GPM_TILE_ID, GPM_PRODUCT_SHORT_NAME,
                gpm24_hit[1], s1["region_id"], gpm24_hit[0],
            )

        found_dates = [hit[1] for hit in (modis_hit, gpm24_hit, gpm72_hit) if hit]
        days_since_s1 = max((abs((d - s1_date).days) for d in found_dates), default=0)

        with db.session() as sess:
            existing = sess.scalar(
                select(FusionProduct).where(
                    FusionProduct.feature_date == s1_date,
                    FusionProduct.region_id == s1["region_id"],
                )
            )
            if existing:
                existing.s1_scene_id = s1["scene_id"]
                existing.modis_scene_id = modis_scene_id
                existing.gpm_scene_id = gpm_scene_id
                existing.days_since_s1 = days_since_s1
                existing.feature_stack_path = str(h5_path)
                sess.flush()
                fusion_id = existing.fusion_id
            else:
                fusion = FusionProduct(
                    feature_date=s1_date,
                    region_id=s1["region_id"],
                    s1_scene_id=s1["scene_id"],
                    modis_scene_id=modis_scene_id,
                    gpm_scene_id=gpm_scene_id,
                    days_since_s1=days_since_s1,
                    feature_stack_path=str(h5_path),
                )
                sess.add(fusion)
                sess.flush()
                fusion_id = fusion.fusion_id

        logger.info(
            "[M9] fusion_id=%d dataset=%s date=%s path=%s hash=%s",
            fusion_id, dataset_id, s1_date.isoformat(), h5_path,
            lineage.compute_sha256(h5_path)[:12],
        )
        return fusion_id

    finally:
        if owns_db:
            db.dispose()
