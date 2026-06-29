# etl/module6_analytics.py
"""
Module 6: Quality Analytics & Visualization.

Responsibilities:
    - Compute radiometric statistics per band (mean, std, min, max in dB)
    - Calculate quality score (0-100) from nodata %, speckle index, radiometric range
    - Generate quality report plots (histogram, spatial map)
    - Write results to database via MetadataManager

Author : Julius Marselinus (BRONTO) - NIM 00000111989
Program: Sistem Informasi - Universitas Multimedia Nusantara

Integration hook (called by module5_orchestrator.py):
    from etl.module6_analytics import run
    metrics = run(scene_id, gold_products, db)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)

# Valid backscatter range for Sentinel-1 IW GRD (dB)
VALID_BACKSCATTER_MIN = -35.0
VALID_BACKSCATTER_MAX =   5.0


@dataclass
class BandMetrics:
    """Quality metrics for a single raster band."""
    band_name:               str
    total_pixels:            int
    valid_pixels:            int
    nodata_pixels:           int
    nodata_percent:          float
    backscatter_mean_db:     float
    backscatter_std_db:      float
    backscatter_min_db:      float
    backscatter_max_db:      float
    speckle_index:           float   # CV = std / |mean|
    radiometric_consistency: bool
    quality_score:           float   # 0-100
    quality_flag:            str     # PASS / FAIL / WARNING


def compute_band_metrics(
    file_path: str,
    band_name: str,
    nodata_value: float = -9999.0,
    cloud_threshold: float = 20.0,
    min_quality_score: float = 60.0,
) -> BandMetrics:
    """
    Compute radiometric quality metrics for a single raster band.

    Args:
        file_path         : Path to GOLD-tier COG GeoTIFF
        band_name         : 'VV' or 'VH'
        nodata_value      : Pixel value indicating missing data
        cloud_threshold   : Max nodata% before flagging as cloud contaminated
        min_quality_score : Threshold below which quality_flag = 'FAIL'

    Returns:
        BandMetrics dataclass with all computed statistics

    TODO: Replace stub logic with rasterio-based computation
    """
    logger.info("[M6] Computing metrics for %s band=%s",
                Path(file_path).name, band_name)

    # --- IMPLEMENT ---
    # import rasterio
    # with rasterio.open(file_path) as src:
    #     data = src.read(1).astype(np.float64)
    #     nodata = src.nodata or nodata_value
    #
    # mask = data != nodata
    # valid = data[mask]
    # total = data.size
    # nodata_count = total - valid.size
    # ...compute stats on valid...
    raise NotImplementedError("Module 6 analytics not yet implemented")


def generate_quality_plot(
    metrics: list[BandMetrics],
    output_path: str,
    scene_id: int,
) -> str:
    """
    Generate a quality summary plot (histogram + stats table).

    Args:
        metrics     : List of BandMetrics (one per band)
        output_path : Output path for PNG plot
        scene_id    : Scene ID for plot title

    Returns:
        output_path of the saved plot

    TODO: Implement with matplotlib
    """
    logger.info("[M6] Generating quality plot for scene=%d", scene_id)
    # --- IMPLEMENT ---
    # import matplotlib.pyplot as plt
    # fig, axes = plt.subplots(1, len(metrics), figsize=(12, 5))
    # ...
    raise NotImplementedError("Module 6 quality plot not yet implemented")


def compute_quality_score(
    nodata_percent: float,
    speckle_index: float,
    radiometric_ok: bool,
) -> float:
    """
    Compute composite quality score (0-100).

    Scoring breakdown:
        50 pts  : nodata penalty (50 * (1 - nodata_percent/100))
        30 pts  : speckle penalty (30 * max(0, 1 - speckle_index))
        20 pts  : radiometric consistency (20 if True, 0 if False)

    Args:
        nodata_percent  : Percentage of nodata pixels (0-100)
        speckle_index   : Coefficient of variation of backscatter
        radiometric_ok  : Whether backscatter range is within valid bounds

    Returns:
        Float quality score in range [0, 100]
    """
    nodata_component      = 50.0 * (1.0 - min(nodata_percent / 100.0, 1.0))
    speckle_component     = 30.0 * max(0.0, 1.0 - speckle_index)
    radiometric_component = 20.0 if radiometric_ok else 0.0
    return round(min(100.0, nodata_component + speckle_component + radiometric_component), 2)


def run(
    scene_id: int,
    gold_products: dict[str, str],  # {'VV': vv_cog_path, 'VH': vh_cog_path}
    db,                             # DatabaseClient instance
    analytics_dir: str = "analytics",
) -> list[BandMetrics]:
    """
    Main entry point for Module 6. Computes metrics and writes to DB.

    Args:
        scene_id      : Scene ID for DB insertion
        gold_products : Dict mapping band name → COG file path
        db            : DatabaseClient instance
        analytics_dir : Directory to save quality plots

    Returns:
        List of BandMetrics (one per band)
    """
    from etl.metadata_manager import MetadataManager
    meta    = MetadataManager(db)
    results = []

    for band_name, file_path in gold_products.items():
        m = compute_band_metrics(file_path, band_name)
        results.append(m)

        # Get product_id for this band's GOLD product
        products = meta.get_products_by_scene(scene_id, tier="GOLD")
        product_id = next(
            (p["product_id"] for p in products if p["band_name"] == band_name), None
        )
        if not product_id:
            logger.warning("[M6] No GOLD product found for scene=%d band=%s", scene_id, band_name)
            continue

        meta.insert_quality_metrics(
            scene_id                = scene_id,
            product_id              = product_id,
            band_name               = m.band_name,
            total_pixels            = m.total_pixels,
            valid_pixels            = m.valid_pixels,
            nodata_pixels           = m.nodata_pixels,
            quality_score           = m.quality_score,
            backscatter_mean_db     = m.backscatter_mean_db,
            backscatter_std_db      = m.backscatter_std_db,
            backscatter_min_db      = m.backscatter_min_db,
            backscatter_max_db      = m.backscatter_max_db,
            radiometric_consistency = m.radiometric_consistency,
            speckle_index           = m.speckle_index,
            quality_flag            = m.quality_flag,
        )

    logger.info("[M6] Complete for scene=%d: %d bands processed", scene_id, len(results))
    return results
