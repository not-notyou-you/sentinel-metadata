# etl/module3_lee_filter.py
"""
Module 3: SAR Speckle Reduction using Lee Adaptive Filter.

Responsibilities:
    - Apply Lee adaptive filter to reduce SAR speckle noise
    - Preserve edge information while smoothing homogeneous areas
    - Output SILVER-tier product

Author : Julius Marselinus (BRONTO) - NIM 00000111989
Program: Sistem Informasi - Universitas Multimedia Nusantara

Integration hook (called by module5_orchestrator.py):
    from etl.module3_lee_filter import run
    vv_lee, vh_lee = run(vv_crop, vh_crop, output_dir)
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)


def lee_filter(img: np.ndarray, window_size: int = 7, looks: int = 1) -> np.ndarray:
    """
    Apply Lee adaptive filter to a 2D SAR intensity array.

    The Lee filter estimates the local mean and variance in a sliding window,
    then applies a weighted combination of the local mean and original pixel
    value based on the noise model.

    Args:
        img         : 2D numpy array of SAR intensity values (linear scale)
        window_size : NxN sliding window size (default 7)
        looks       : Number of looks (affects ENL estimation)

    Returns:
        Filtered 2D numpy array

    Reference:
        Lee, J.S. (1980). Digital image enhancement and noise filtering by use
        of local statistics. IEEE TPAMI, 2(2), 165-168.
    """
    from scipy.ndimage import uniform_filter

    img = img.astype(np.float64)
    img_mean    = uniform_filter(img,   size=window_size)
    img_sqr     = uniform_filter(img**2, size=window_size)
    img_var     = img_sqr - img_mean**2

    # Equivalent Number of Looks noise variance
    noise_var   = img_mean**2 / looks
    weight      = img_var / (img_var + noise_var)
    weight      = np.clip(weight, 0, 1)

    filtered    = img_mean + weight * (img - img_mean)
    return filtered.astype(img.dtype)


def apply_lee_to_tiff(
    input_path: str,
    output_path: str,
    window_size: int = 7,
    looks: int = 1,
) -> str:
    """
    Read a GeoTIFF, apply Lee filter, and write the result.

    Args:
        input_path  : Path to input GeoTIFF (BRONZE tier)
        output_path : Path for filtered output GeoTIFF (SILVER tier)
        window_size : Lee filter window size
        looks       : Number of looks

    Returns:
        output_path of the filtered file

    TODO: Wire rasterio read/write around the lee_filter() function above
    """
    logger.info("[M3] Lee filtering %s (window=%d looks=%d)",
                Path(input_path).name, window_size, looks)
    # --- IMPLEMENT ---
    # import rasterio
    # with rasterio.open(input_path) as src:
    #     meta = src.meta.copy()
    #     band = src.read(1)
    # filtered = lee_filter(band, window_size, looks)
    # with rasterio.open(output_path, 'w', **meta) as dst:
    #     dst.write(filtered, 1)
    raise NotImplementedError("Module 3 Lee filter not yet implemented")


def run(
    vv_path: str,
    vh_path: str,
    output_dir: str,
    window_size: int = 7,
    looks: int = 1,
) -> tuple[str, str]:
    """
    Main entry point for Module 3. Filters both VV and VH bands.

    Args:
        vv_path     : Input VV band (BRONZE tier, from Module 2)
        vh_path     : Input VH band (BRONZE tier, from Module 2)
        output_dir  : Output directory for filtered files
        window_size : Lee filter window size
        looks       : Number of looks

    Returns:
        (vv_lee_path, vh_lee_path) SILVER-tier outputs
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    stem = Path(vv_path).stem.replace("_VV_crop", "")

    vv_out = str(Path(output_dir) / f"{stem}_VV_lee.tif")
    vh_out = str(Path(output_dir) / f"{stem}_VH_lee.tif")

    apply_lee_to_tiff(vv_path, vv_out, window_size, looks)
    apply_lee_to_tiff(vh_path, vh_out, window_size, looks)

    logger.info("[M3] Complete: VV=%s VH=%s", vv_out, vh_out)
    return vv_out, vh_out
