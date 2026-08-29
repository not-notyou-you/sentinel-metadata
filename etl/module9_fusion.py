# etl/module9_fusion.py
"""
Builds a multi-modal HDF5 feature stack (Sentinel-1 SAR + MODIS flood +
GPM rainfall) aligned to a common grid for ML training. This is the GOLD
tier deliverable for every dataset — the only thing a GOLD tier request
produces (no per-band GeoTIFFs; see module5_orchestrator.py's FUSION stage).

Sentinel-1 SILVER products (VV/VH, LEE-filtered) are looked up in the
`data_products` table and define the reference grid. MODIS/GPM SILVER
GeoTIFFs (fusion inputs, written by module7/module8 via
ensure_aux_inputs_for_date below) are discovered on disk under
data/datasets/{id}_{slug}/silver/{date}/, matched to the S1 acquisition
time within a 24h window, reprojected onto the S1 grid, and registered as
`nasa_scenes` rows. The resulting stack is written to
data/datasets/{id}_{slug}/gold/{date}/ as an .h5 + metadata JSON, recorded
as a `fusion_products` row (for `fusion_id` lineage) and a `data_products`
row (tier=GOLD, so it shows up in the same generic tier queries/listings as
every other tier's deliverables).
"""

from __future__ import annotations

import json
import logging
from datetime import date as date_type, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

import h5py
import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.warp import reproject
from shapely.geometry import box
from sqlalchemy import select

from etl import folder_manager as fm
from etl.database_client import (
    DatabaseClient,
    DataProduct,
    FusionProduct,
    NasaScene,
    ProductTierEnum,
    SatelliteScene,
)
from etl.constants import (
    GPM_PRODUCT_SHORT_NAME,
    GPM_SOURCE,
    GPM_TILE_ID,
    MODIS_PRODUCT_SHORT_NAME,
    MODIS_SOURCE,
    MODIS_TILE_ID,
)
from etl.lineage_tracker import LineageTracker
from etl.metadata_manager import MetadataManager
from etl.pipeline_logger import PipelineLogger

logger = logging.getLogger(__name__)

ALIGNMENT_WINDOW_HOURS = 24
MODIS_NODATA_U8 = 255  # uint8 can't hold NaN; 255 marks a missing/nodata pixel
HDF5_CHUNK_MAX = 256


def _find_s1_silver(db: DatabaseClient, dataset_id: int, scene_id: int) -> dict | None:
    """Find the Sentinel-1 SILVER (LEE-filtered) VV/VH products for
    `scene_id` within `dataset_id`. Returns None if the scene doesn't exist.

    Looked up by the exact scene_id the caller just processed, rather than
    re-derived from the acquisition date: adjacent Sentinel-1 slices from the
    same orbit pass can both land on the same UTC day over the same AOI, and
    picking "whichever scene happened that day" would silently grab the
    wrong one whenever more than one scene qualifies."""
    with db.session() as sess:
        scene = sess.get(SatelliteScene, scene_id)
        if scene is None:
            return None

        def _silver_band(band: str) -> DataProduct | None:
            return sess.scalar(
                select(DataProduct).where(
                    DataProduct.scene_id == scene.scene_id,
                    DataProduct.dataset_id == dataset_id,
                    DataProduct.product_tier == ProductTierEnum.SILVER,
                    DataProduct.product_type == "LEE_FILTERED",
                    DataProduct.band_name == band,
                    DataProduct.is_latest == True,
                    DataProduct.is_valid == True,
                )
            )

        vv = _silver_band("VV")
        vh = _silver_band("VH")

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
    dataset_id: int,
    dataset_name: str,
    filename_fn,
    center_dt: datetime,
    tolerance_hours: int = ALIGNMENT_WINDOW_HOURS // 2,
) -> tuple[Path, date_type] | None:
    """MODIS/GPM SILVER files are stamped one-per-day at local midnight, each
    under its own data/datasets/{id}_{slug}/silver/{date}/ scene folder.
    Return the (path, date) of the closest candidate day whose midnight falls
    within `tolerance_hours` of `center_dt`, or None if nothing is on disk."""
    best: tuple[Path, date_type] | None = None
    best_diff = None
    for offset in (0, -1, 1):
        candidate_date = (center_dt + timedelta(days=offset)).date()
        candidate_midnight = datetime.combine(candidate_date, datetime.min.time(),
                                              tzinfo=center_dt.tzinfo)
        diff_hours = abs((candidate_midnight - center_dt).total_seconds()) / 3600.0
        if diff_hours > tolerance_hours:
            continue
        silver_dir = fm.get_scene_dir(dataset_id, dataset_name, "silver", candidate_date.strftime("%Y%m%d"))
        path = silver_dir / filename_fn(candidate_date)
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

    logger.info("[M9] Saving fusion H5 to GOLD tier: %s shape=(%d, %d)", h5_path, height, width)


def _write_fusion_metadata_json(
    json_path: Path,
    fusion_id: int,
    dataset_id: int,
    region_id: int,
    s1_date: date_type,
    s1: dict,
    modis_hit: tuple[Path, date_type] | None,
    gpm24_hit: tuple[Path, date_type] | None,
    gpm72_hit: tuple[Path, date_type] | None,
    days_since_s1: int,
    acquisition_datetime: datetime,
    processing_datetime: datetime,
    aoi_bbox: tuple[float, float, float, float],
    h5_path: Path,
    height: int,
    width: int,
) -> None:
    metadata = {
        "fusion_id": fusion_id,
        "dataset_id": dataset_id,
        "region_id": region_id,
        "feature_date": s1_date.isoformat(),
        "acquisition_datetime": acquisition_datetime.isoformat(),
        "processing_datetime": processing_datetime.isoformat(),
        "aoi_bbox": list(aoi_bbox),
        "shape": {"height": height, "width": width},
        "layers": FUSION_LAYERS,
        "source_scenes": {
            "s1_scene_id": s1["scene_id"],
            "modis_date": modis_hit[1].isoformat() if modis_hit else None,
            "gpm_24h_date": gpm24_hit[1].isoformat() if gpm24_hit else None,
            "gpm_72h_date": gpm72_hit[1].isoformat() if gpm72_hit else None,
        },
        "days_since_s1": days_since_s1,
        "file_name": h5_path.name,
        "file_size_mb": round(h5_path.stat().st_size / (1024 ** 2), 3),
    }
    with open(json_path, "w") as f:
        json.dump(metadata, f, indent=2, default=str)


def _product_exists(db: DatabaseClient, dataset_id: int, file_path: str) -> bool:
    with db.session() as sess:
        return sess.scalar(
            select(DataProduct.product_id).where(
                DataProduct.dataset_id == dataset_id,
                DataProduct.file_path == file_path,
                DataProduct.is_latest == True,
            )
        ) is not None


def _resolve_aux_scene(db: DatabaseClient, dataset_id: int, region_id: int, bbox_wkt: str) -> int:
    """Placeholder SatelliteScene used to attach MODIS/GPM aux data_products
    (which aren't tied to any single Sentinel-1 scene) to a valid scene_id."""
    meta = MetadataManager(db)
    pid = f"NASA_AUX_DATASET_{dataset_id}"
    existing = meta.get_scene_by_pid(pid)
    if existing:
        return existing["scene_id"]
    return meta.insert_satellite_scene(
        product_identifier=pid,
        acquisition_datetime=datetime.now(tz=timezone.utc),
        region_id=region_id,
        bbox_wkt=bbox_wkt,
        orbit_direction="ASCENDING",
        polarization_vv=False,
        polarization_vh=False,
        resolution_m=250,
        instrument_mode="AUX",
    )


def ensure_aux_inputs_for_date(
    db: DatabaseClient,
    dataset_id: int,
    dataset_name: str,
    region_id: int,
    aoi_bbox: tuple[float, float, float, float],
    target_date: date_type,
    plog: PipelineLogger | None = None,
) -> None:
    """
    Download (if not already on disk) and register as SILVER data_products
    the MODIS flood + GPM rainfall inputs needed to fuse an S1 scene acquired
    on `target_date`. Idempotent: module7/module8 skip files already written
    to disk, and a dedup check by file_path skips re-registering a
    data_products row for a file already tracked for this dataset.

    Download failures are logged and swallowed here — create_fusion_stack
    already tolerates missing MODIS/GPM inputs by filling NaN/nodata, so a
    NASA server hiccup shouldn't fail the whole scene pipeline (only a
    missing S1 SILVER reference is fatal, checked in create_fusion_stack).
    """
    from etl.module7_modis_download import download_modis_scene
    from etl.module8_gpm_download import download_gpm_scene

    meta = MetadataManager(db)
    lineage = LineageTracker(db)
    bbox_wkt = box(*aoi_bbox).wkt
    aux_scene_id = _resolve_aux_scene(db, dataset_id, region_id, bbox_wkt)
    target_dt = datetime.combine(target_date, datetime.min.time(), tzinfo=timezone.utc)

    try:
        _, modis_meta = download_modis_scene(dataset_id, dataset_name, target_dt, target_dt, aoi_bbox, plog=plog)
        job_id = meta.insert_processing_job(
            aux_scene_id, "DOWNLOAD", parameters={"dataset_id": dataset_id, "source": "MODIS"}
        )
        for output in modis_meta["outputs"]:
            path = output["flood_path"]
            if _product_exists(db, dataset_id, path):
                continue
            meta.insert_nasa_scene(
                source=MODIS_SOURCE, tile_id=MODIS_TILE_ID,
                product_short_name=MODIS_PRODUCT_SHORT_NAME,
                acquisition_date=date_type.fromisoformat(output["date"]),
                region_id=region_id, raw_file_path=path,
            )
            meta.insert_data_product(
                scene_id=aux_scene_id, job_id=job_id, dataset_id=dataset_id,
                product_tier="SILVER", product_type="MODIS_FLOOD", band_name="FLOOD",
                file_path=path, file_name=Path(path).name,
                file_size_mb=round(Path(path).stat().st_size / (1024 ** 2), 3),
                data_hash_sha256=lineage.compute_sha256(path),
            )
    except Exception:
        logger.exception("[M9] gagal siapkan input MODIS dataset=%s tanggal=%s", dataset_id, target_date)

    try:
        _, gpm_meta = download_gpm_scene(dataset_id, dataset_name, target_dt, aoi_bbox, plog=plog)
        job_id = meta.insert_processing_job(
            aux_scene_id, "DOWNLOAD", parameters={"dataset_id": dataset_id, "source": "GPM"}
        )
        for window_name, output in gpm_meta["windows"].items():
            path = output["path"]
            if _product_exists(db, dataset_id, path):
                continue
            meta.insert_nasa_scene(
                source=GPM_SOURCE, tile_id=GPM_TILE_ID,
                product_short_name=GPM_PRODUCT_SHORT_NAME,
                acquisition_date=target_date, region_id=region_id, raw_file_path=path,
            )
            meta.insert_data_product(
                scene_id=aux_scene_id, job_id=job_id, dataset_id=dataset_id,
                product_tier="SILVER", product_type="GPM_RAINFALL",
                band_name=f"RAIN_{window_name.upper()}",
                file_path=path, file_name=Path(path).name,
                file_size_mb=round(Path(path).stat().st_size / (1024 ** 2), 3),
                data_hash_sha256=lineage.compute_sha256(path),
            )
    except Exception:
        logger.exception("[M9] gagal siapkan input GPM dataset=%s tanggal=%s", dataset_id, target_date)


FUSION_LAYERS = ["s1_vv", "s1_vh", "modis_flood", "gpm_rainfall_24h", "gpm_rainfall_72h"]


def create_fusion_stack(
    dataset_id: int,
    dataset_name: str,
    s1_date: date_type,
    aoi_bbox: tuple[float, float, float, float],
    scene_id: int,
    db: DatabaseClient | None = None,
    progress_cb: Callable[[str, int, int], None] | None = None,
) -> int:
    """
    Build a fused HDF5 feature stack for Sentinel-1 `scene_id` (acquired on
    `s1_date`), aligning MODIS flood and GPM rainfall SILVER products found
    within 24h of the S1 acquisition time. This is the GOLD tier deliverable.

    Writes:
        data/datasets/{id}_{slug}/gold/{s1_date}/fusion_{s1_date}.h5
        data/datasets/{id}_{slug}/gold/{s1_date}/fusion_metadata.json

    Returns:
        fusion_id (int) — primary key of the `fusion_products` row, used
        for lineage tracking.
    """
    owns_db = db is None
    db = db or DatabaseClient.from_env()
    lineage = LineageTracker(db)
    meta = MetadataManager(db)

    try:
        s1 = _find_s1_silver(db, dataset_id, scene_id)
        if s1 is None or (not s1["vv_path"] and not s1["vh_path"]):
            raise RuntimeError(f"No S1 SILVER product found for scene_id={scene_id} s1_date={s1_date.isoformat()}")

        ref_path = s1["vv_path"] or s1["vh_path"]
        with rasterio.open(ref_path) as ref:
            ref_transform, ref_crs = ref.transform, ref.crs
            ref_shape = (ref.height, ref.width)

        s1_vv = _read_band_or_nan(s1["vv_path"], ref_shape)
        if s1["vv_path"] is None:
            logger.warning("[M9] S1 VV missing for scene=%s, filled with NaN", s1["scene_id"])
        if progress_cb:
            progress_cb("s1_vv", 1, len(FUSION_LAYERS))

        s1_vh = _read_band_or_nan(s1["vh_path"], ref_shape)
        if s1["vh_path"] is None:
            logger.warning("[M9] S1 VH missing for scene=%s, filled with NaN", s1["scene_id"])
        if progress_cb:
            progress_cb("s1_vh", 2, len(FUSION_LAYERS))

        center_dt = s1["acquisition_datetime"]

        modis_hit = _find_nearest_daily_file(
            dataset_id, dataset_name, lambda d: f"modis_{d.strftime('%Y%m%d')}_flood.tif", center_dt,
        )
        gpm24_hit = _find_nearest_daily_file(
            dataset_id, dataset_name, lambda d: f"gpm_rain_24h_{d.strftime('%Y%m%d')}.tif", center_dt,
        )
        gpm72_hit = _find_nearest_daily_file(
            dataset_id, dataset_name, lambda d: f"gpm_rain_72h_{d.strftime('%Y%m%d')}.tif", center_dt,
        )

        if modis_hit:
            modis_flood_f32 = _reproject_to_grid(
                modis_hit[0], ref_transform, ref_crs, ref_shape,
                resampling=Resampling.nearest, fill_value=np.nan,
            )
            modis_flood = np.where(np.isnan(modis_flood_f32), MODIS_NODATA_U8,
                                   modis_flood_f32).astype("uint8")
        else:
            logger.warning("[M9] no MODIS SILVER product within %dh of %s", ALIGNMENT_WINDOW_HOURS, center_dt)
            modis_flood = np.full(ref_shape, MODIS_NODATA_U8, dtype="uint8")
        if progress_cb:
            progress_cb("modis_flood", 3, len(FUSION_LAYERS))

        if gpm24_hit:
            gpm_24h = _reproject_to_grid(
                gpm24_hit[0], ref_transform, ref_crs, ref_shape,
                resampling=Resampling.bilinear, fill_value=np.nan,
            )
        else:
            logger.warning("[M9] no GPM 24h SILVER product within %dh of %s", ALIGNMENT_WINDOW_HOURS, center_dt)
            gpm_24h = np.full(ref_shape, np.nan, dtype="float32")
        if progress_cb:
            progress_cb("gpm_rainfall_24h", 4, len(FUSION_LAYERS))

        if gpm72_hit:
            gpm_72h = _reproject_to_grid(
                gpm72_hit[0], ref_transform, ref_crs, ref_shape,
                resampling=Resampling.bilinear, fill_value=np.nan,
            )
        else:
            logger.warning("[M9] no GPM 72h SILVER product within %dh of %s", ALIGNMENT_WINDOW_HOURS, center_dt)
            gpm_72h = np.full(ref_shape, np.nan, dtype="float32")
        if progress_cb:
            progress_cb("gpm_rainfall_72h", 5, len(FUSION_LAYERS))

        date_key = s1_date.strftime("%Y%m%d")
        out_dir = fm.get_scene_dir(dataset_id, dataset_name, "gold", date_key)
        h5_path = out_dir / f"fusion_{date_key}.h5"
        json_path = out_dir / "fusion_metadata.json"
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

        height, width = ref_shape
        _write_fusion_metadata_json(
            json_path, fusion_id, dataset_id, s1["region_id"], s1_date, s1,
            modis_hit, gpm24_hit, gpm72_hit, days_since_s1,
            center_dt, processing_dt, aoi_bbox, h5_path, height, width,
        )

        fusion_job_id = meta.insert_processing_job(
            s1["scene_id"], "FUSION", parameters={"dataset_id": dataset_id, "s1_date": s1_date.isoformat()},
        )
        meta.start_job(fusion_job_id)
        gold_product_id = meta.insert_data_product(
            scene_id=s1["scene_id"], job_id=fusion_job_id, dataset_id=dataset_id,
            product_tier="GOLD", product_type="FUSION_H5", band_name="FUSION",
            file_path=str(h5_path), file_name=h5_path.name,
            file_size_mb=round(h5_path.stat().st_size / (1024 ** 2), 3),
            data_hash_sha256=lineage.compute_sha256(h5_path),
            file_format="HDF5", rows=height, cols=width,
        )
        if s1["vv_product_id"]:
            lineage.record_transformation(
                s1["vv_product_id"], gold_product_id, "FUSION", fusion_job_id, {"aoi_bbox": list(aoi_bbox)}
            )
        if s1["vh_product_id"]:
            lineage.record_transformation(
                s1["vh_product_id"], gold_product_id, "FUSION", fusion_job_id, {"aoi_bbox": list(aoi_bbox)}
            )
        meta.complete_job(fusion_job_id)

        logger.info(
            "[M9] fusion_id=%d dataset=%s date=%s path=%s hash=%s",
            fusion_id, dataset_id, s1_date.isoformat(), h5_path,
            lineage.compute_sha256(h5_path)[:12],
        )
        return fusion_id

    finally:
        if owns_db:
            db.dispose()
