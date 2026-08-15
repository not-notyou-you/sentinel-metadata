# etl/location_resolver.py
from __future__ import annotations
import logging
from geoalchemy2.shape import to_shape
from sqlalchemy import select
from etl.database_client import DatabaseClient, RegionOfInterest

logger = logging.getLogger(__name__)

_NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
_USER_AGENT = "sentinel-sentinel-flood-pipeline/1.0"


def _match_known_region(db: DatabaseClient, location: str) -> tuple[str, int, str] | None:
    normalized = location.strip().lower()
    with db.session() as sess:
        rows = sess.scalars(
            select(RegionOfInterest).where(RegionOfInterest.is_active == True)
        ).all()
        for r in rows:
            if r.name.strip().lower() == normalized or r.region_code.strip().lower() == normalized:
                bbox_wkt = to_shape(r.bbox).wkt
                return bbox_wkt, r.region_id, r.name
    return None


def _geocode_nominatim(location: str) -> tuple[str, str]:
    import requests

    params = {
        "q": location,
        "format": "json",
        "limit": 1,
        "polygon_geojson": 0,
        "countrycodes": "id",
    }
    headers = {"User-Agent": _USER_AGENT}
    resp = requests.get(_NOMINATIM_URL, params=params, headers=headers, timeout=15)
    if resp.status_code != 200:
        raise RuntimeError(f"Geocoding gagal ({resp.status_code}) untuk lokasi: {location}")
    results = resp.json()
    if not results:
        raise ValueError(f"Lokasi tidak ditemukan: {location}")
    item = results[0]
    south, north, west, east = [float(x) for x in item["boundingbox"]]
    bbox_wkt = (
        f"POLYGON(({west} {south}, {east} {south}, {east} {north}, "
        f"{west} {north}, {west} {south}))"
    )
    label = item.get("display_name", location)
    return bbox_wkt, label


def resolve_location(db: DatabaseClient, location: str) -> tuple[str, int | None, str]:
    known = _match_known_region(db, location)
    if known:
        bbox_wkt, region_id, label = known
        logger.info("[LOCATION] '%s' matched known region_id=%d", location, region_id)
        return bbox_wkt, region_id, label
    bbox_wkt, label = _geocode_nominatim(location)
    logger.info("[LOCATION] '%s' geocoded via Nominatim -> %s", location, label)
    return bbox_wkt, None, label