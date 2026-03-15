"""MassGIS parcel → centroid lookup.

Loads data/nantucket_parcels.geojson once at import time and provides
a lookup from (map_number, parcel_number) to (latitude, longitude).

The GeoJSON MAP_PAR_ID field uses space-separated format: "21 80".
Our DB stores map_number="21", parcel_number="80" separately.
"""

import json
import logging
import os
from pathlib import Path
from typing import Optional

from shapely.geometry import shape

logger = logging.getLogger(__name__)

_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"
_GEOJSON_PATH = _DATA_DIR / "nantucket_parcels.geojson"

# Parcel index: MAP_PAR_ID → (lat, lng)
_parcel_centroids: dict[str, tuple[float, float]] = {}
_loaded = False


def _load():
    global _parcel_centroids, _loaded
    if _loaded:
        return

    logger.info("Loading parcel GeoJSON from %s", _GEOJSON_PATH)
    with open(_GEOJSON_PATH) as f:
        gj = json.load(f)

    for feat in gj["features"]:
        par_id = feat["properties"].get("MAP_PAR_ID")
        if not par_id:
            continue
        geom = shape(feat["geometry"])
        centroid = geom.centroid
        _parcel_centroids[par_id] = (centroid.y, centroid.x)  # (lat, lng)

    _loaded = True
    logger.info("Loaded %d parcel centroids", len(_parcel_centroids))


def lookup_parcel(map_number: str, parcel_number: str) -> Optional[tuple[float, float]]:
    """Return (latitude, longitude) for a parcel, or None if not found."""
    _load()

    # Primary key: "21 80"
    key = f"{map_number} {parcel_number}"
    result = _parcel_centroids.get(key)
    if result:
        return result

    # Compound parcels like "37 & 122": try first number only
    if "&" in parcel_number:
        first_parcel = parcel_number.split("&")[0].strip()
        fallback_key = f"{map_number} {first_parcel}"
        result = _parcel_centroids.get(fallback_key)
        if result:
            logger.info("Compound parcel %s resolved via first parcel %s", key, fallback_key)
            return result

    logger.warning("Parcel not found: %s", key)
    return None
