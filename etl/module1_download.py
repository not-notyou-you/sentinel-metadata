# etl/module1_download.py
"""
Module 1: Sentinel-1 Scene Discovery & Georeferencing Recovery.

Responsibilities:
    - Query Copernicus Open Access Hub for new Sentinel-1 IW scenes over Jabodetabek
    - Download and verify SAFE/ZIP archives
    - Recover missing georeferencing metadata (GCPs, projection info)
    - Register scene in database via MetadataManager

Author : Julius Marselinus (BRONTO) - NIM 00000111989
Program: Sistem Informasi - Universitas Multimedia Nusantara

Integration hook (called by module5_orchestrator.py):
    from etl.module1_download import run
    raw_vv_path, raw_vh_path = run(scene_metadata, ctx)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class SceneMetadata:
    """Sentinel-1 scene metadata returned by Copernicus Hub search."""
    product_identifier:   str
    acquisition_datetime: datetime
    orbit_direction:      str        = "ASCENDING"
    orbit_number:         int | None = None
    relative_orbit:       int | None = None
    cloud_cover_percent:  float | None = None
    incidence_angle_near: float | None = None
    incidence_angle_far:  float | None = None
    resolution_m:         int        = 10
    download_url:         str | None = None
    instrument_mode:      str        = "IW"


def discover_scenes(
    bbox: tuple[float, float, float, float],
    date_from: datetime,
    date_to: datetime,
    max_results: int = 10,
) -> list[SceneMetadata]:
    """
    Query Copernicus Hub for Sentinel-1 scenes over a bounding box.

    Args:
        bbox       : (min_lon, min_lat, max_lon, max_lat) in WGS84
        date_from  : Start of acquisition window
        date_to    : End of acquisition window
        max_results: Max number of scenes to return

    Returns:
        List of SceneMetadata objects

    TODO: Implement using sentinelsat or direct OpenSearch API
    """
    logger.info("[M1] Discovering scenes over bbox=%s from=%s to=%s",
                bbox, date_from.date(), date_to.date())
    # --- IMPLEMENT: sentinelsat / Copernicus Hub API query ---
    raise NotImplementedError("Module 1 discovery not yet implemented")


def download_scene(metadata: SceneMetadata, output_dir: str) -> str:
    """
    Download a Sentinel-1 SAFE archive from Copernicus Hub.

    Args:
        metadata   : Scene metadata containing download_url and product_identifier
        output_dir : Local directory to save the archive

    Returns:
        Local file path of the downloaded archive

    TODO: Implement with requests/sentinelsat + progress bar + MD5 verification
    """
    logger.info("[M1] Downloading scene %s", metadata.product_identifier)
    # --- IMPLEMENT: HTTP download with retry ---
    raise NotImplementedError("Module 1 download not yet implemented")


def recover_georeferencing(zip_path: str, output_dir: str) -> tuple[str, str]:
    """
    Extract VV and VH bands from SAFE archive and recover missing GCPs.
    Some Sentinel-1 products have incomplete georeferencing — this step
    applies GCP-based correction using orbit state vectors.

    Args:
        zip_path   : Path to downloaded .SAFE.zip archive
        output_dir : Directory for extracted/corrected TIFF outputs

    Returns:
        (vv_tif_path, vh_tif_path) tuple of corrected GeoTIFF paths

    TODO: Implement with rasterio + GDAL GCP-based warping
    """
    logger.info("[M1] Recovering georeferencing for %s", zip_path)
    # --- IMPLEMENT: rasterio extract + GCP recovery ---
    raise NotImplementedError("Module 1 georeferencing recovery not yet implemented")


def run(metadata: SceneMetadata, output_dir: str = "recovered_temp") -> tuple[str, str]:
    """
    Main entry point for Module 1. Called by module5_orchestrator.

    Args:
        metadata   : Scene metadata from discover_scenes()
        output_dir : Base output directory

    Returns:
        (vv_tif_path, vh_tif_path) ready for Module 2
    """
    zip_path = download_scene(metadata, output_dir)
    vv_path, vh_path = recover_georeferencing(zip_path, output_dir)
    logger.info("[M1] Complete: VV=%s VH=%s", vv_path, vh_path)
    return vv_path, vh_path
