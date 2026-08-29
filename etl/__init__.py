# etl/__init__.py
import os

# This machine has three mutually-incompatible PROJ installations fighting over
# the same env vars: PostgreSQL/PostGIS sets PROJ_LIB/GDAL_DATA system-wide
# (PROJ 8.2.1, DB layout 1.2), a stray .env sets PROJ_LIB/PROJ_DATA to a path
# that doesn't even exist on disk, and the venv's rasterio (PROJ 9.5.0, layout
# 1.4) and pyproj (PROJ 9.4.1, layout 1.3) wheels each bundle their own
# matching proj.db. Whichever of PROJ_LIB/PROJ_DATA/GDAL_DATA is already set
# in the process env wins over each wheel's own bundled data, which is how you
# get "Cannot find proj.db" / "CRSError: unknown EPSG code" during CALIBRATE,
# CROP, etc. Every rasterio/pyproj-touching module in this codebase imports
# through the `etl` package, so stripping these here — before any of them are
# imported — forces rasterio and pyproj to fall back to their own bundled,
# version-matched PROJ data instead of an external, incompatible one.
for _var in ("PROJ_LIB", "PROJ_DATA", "GDAL_DATA"):
    os.environ.pop(_var, None)
del _var
