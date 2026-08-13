# etl/module4_cog_export.py
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import rasterio
from rasterio.enums import Resampling

logger = logging.getLogger(__name__)

DEFAULT_COMPRESSION = "LZW"
DEFAULT_BLOCKSIZE = 512
DEFAULT_OVERVIEWS = [2, 4, 8, 16]
DEFAULT_BIGTIFF = "IF_SAFER"


def to_db(linear_array: np.ndarray) -> np.ndarray:
    return 10 * np.log10(np.where(linear_array > 0, linear_array, 1e-10))


def export_cog(
    input_path: str,
    output_path: str,
    compression: str = DEFAULT_COMPRESSION,
    blocksize: int = DEFAULT_BLOCKSIZE,
    overview_levels: list[int] | None = None,
    convert_to_db: bool = False,
) -> str:
    if overview_levels is None:
        overview_levels = DEFAULT_OVERVIEWS
    with rasterio.open(input_path) as src:
        data = src.read(1)
        profile = src.profile.copy()
        if convert_to_db:
            data = to_db(data).astype("float32")
            profile.update({"dtype": "float32"})
        profile.update({
            "driver": "GTiff",
            "compress": compression,
            "tiled": True,
            "blockxsize": blocksize,
            "blockysize": blocksize,
            "bigtiff": DEFAULT_BIGTIFF,
        })
        with rasterio.open(output_path, "w", **profile) as dst:
            dst.write(data, 1)
            dst.build_overviews(overview_levels, Resampling.average)
            dst.update_tags(ns="rio_overview", resampling="average")
    logger.info("[M4] %s -> %s compression=%s blocksize=%d",
                Path(input_path).name, Path(output_path).name, compression, blocksize)
    return output_path


def validate_cog(file_path: str) -> bool:
    with rasterio.open(file_path) as src:
        if not src.profile.get("tiled", False):
            return False
        if not src.overviews(1):
            return False
    return True


def run(
    vv_path: str,
    vh_path: str,
    output_dir: str,
    compression: str = DEFAULT_COMPRESSION,
    blocksize: int = DEFAULT_BLOCKSIZE,
    convert_to_db: bool = False,
) -> tuple[str, str]:
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    stem = Path(vv_path).stem.replace("_VV_lee", "")
    vv_out = str(Path(output_dir) / f"{stem}_VV_cog.tif")
    vh_out = str(Path(output_dir) / f"{stem}_VH_cog.tif")
    export_cog(vv_path, vv_out, compression, blocksize, convert_to_db=convert_to_db)
    export_cog(vh_path, vh_out, compression, blocksize, convert_to_db=convert_to_db)
    return vv_out, vh_out