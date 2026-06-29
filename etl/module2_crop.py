# etl/module2_crop.py
"""
Module 2: Spatial Subsetting to Region of Interest (Jabodetabek bbox).

Responsibilities:
    - Crop raw Sentinel-1 GeoTIFF to Jabodetabek bounding box
    - Reproject if necessary (target: EPSG:4326)
    - Output BRONZE-tier product

Author : Julius Marselinus (BRONTO) - NIM 00000111989
Program: Sistem Informasi - Universitas Multimedia Nusantara

Integration hook (called by module5_orchestrator.py):
    from etl.module2_crop import run
    vv_crop, vh_crop = run(vv_path, vh_path, bbox, output_dir)
"""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Jabodetabek bbox (WGS84): min_lon, min_lat, max_lon, max_lat
JABODETABEK_BBOX = (106.4, -6.7, 107.2, -5.9)


def crop_to_bbox(
    input_path: str,
    bbox: tuple[float, float, float, float],
    output_path: str,
    target_crs: str = "EPSG:4326",
    resampling: str = "bilinear",
) -> str:
    """
    Crop a GeoTIFF to a bounding box and reproject if needed.

    Args:
        input_path  : Path to input GeoTIFF
        bbox        : (min_lon, min_lat, max_lon, max_lat) WGS84
        output_path : Path for output cropped GeoTIFF
        target_crs  : Target coordinate reference system
        resampling  : Resampling method for reprojection

    Returns:
        output_path of the cropped file

    TODO: Implement with rasterio.mask.mask() + rasterio.warp.reproject()
    """
    logger.info("[M2] Cropping %s → bbox=%s", Path(input_path).name, bbox)
    # --- IMPLEMENT ---
    # import rasterio
    # from rasterio.mask import mask
    # from shapely.geometry import box
    # import geopandas as gpd
    # geom = box(*bbox)
    # with rasterio.open(input_path) as src:
    #     out_image, out_transform = mask(src, [geom.__geo_interface__], crop=True)
    #     out_meta = src.meta.copy()
    #     out_meta.update({"driver": "GTiff", "height": out_image.shape[1],
    #                      "width": out_image.shape[2], "transform": out_transform})
    #     with rasterio.open(output_path, "w", **out_meta) as dst:
    #         dst.write(out_image)
    raise NotImplementedError("Module 2 crop not yet implemented")


def run(
    vv_path: str,
    vh_path: str,
    output_dir: str,
    bbox: tuple[float, float, float, float] = JABODETABEK_BBOX,
) -> tuple[str, str]:
    """
    Main entry point for Module 2. Crops both VV and VH bands.

    Args:
        vv_path    : Input VV band GeoTIFF (from Module 1)
        vh_path    : Input VH band GeoTIFF (from Module 1)
        output_dir : Output directory for cropped files
        bbox       : Bounding box to crop to

    Returns:
        (vv_crop_path, vh_crop_path) BRONZE-tier outputs
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    stem = Path(vv_path).stem.replace("_VV", "")

    vv_out = str(Path(output_dir) / f"{stem}_VV_crop.tif")
    vh_out = str(Path(output_dir) / f"{stem}_VH_crop.tif")

    crop_to_bbox(vv_path, bbox, vv_out)
    crop_to_bbox(vh_path, bbox, vh_out)

    logger.info("[M2] Complete: VV=%s VH=%s", vv_out, vh_out)
    return vv_out, vh_out
