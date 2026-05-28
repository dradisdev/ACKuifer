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
_STREET_INDEX_PATH = _DATA_DIR / "parcel_streets.json"

# Parcel indices: MAP_PAR_ID → (lat, lng), and MAP_PAR_ID → street name.
# The street-name index is precomputed by scripts/build_parcel_street_index.py
# via Nominatim reverse-geocoding of each parcel centroid.
_parcel_centroids: dict[str, tuple[float, float]] = {}
_parcel_streets: dict[str, str] = {}
_loaded = False


def _load():
    global _parcel_centroids, _parcel_streets, _loaded
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

    logger.info("Loaded %d parcel centroids", len(_parcel_centroids))

    # Load the precomputed street-name sidecar if present. Built by
    # scripts/build_parcel_street_index.py via Nominatim reverse-geocoding.
    # Used as a fallback when a Laserfiche PDF has no embedded sample address
    # (third-party-submitter reports like Eurofins for Island Water Filtration).
    # Missing file → parcel_street_name() returns None and the popup falls
    # back to "Unknown location" — same as pre-feature behavior.
    if _STREET_INDEX_PATH.exists():
        with open(_STREET_INDEX_PATH) as f:
            _parcel_streets = json.load(f)
        logger.info(
            "Loaded %d parcel street names from %s",
            len(_parcel_streets),
            _STREET_INDEX_PATH.name,
        )
    else:
        logger.warning(
            "Parcel street index not found at %s — parcel_street_name() will "
            "return None for all parcels. Run "
            "scripts/build_parcel_street_index.py to generate.",
            _STREET_INDEX_PATH,
        )

    _loaded = True


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


def parcel_street_name(map_number: str, parcel_number: str) -> Optional[str]:
    """Return the display street name for a parcel, or None if not indexed.

    Used as a fallback when a Laserfiche PDF has no embedded sample address —
    e.g. third-party-submitter reports (Eurofins for Island Water Filtration)
    that test water on behalf of an unnamed homeowner. The Town parcel
    placement of the Laserfiche folder identifies the property even when the
    PDF doesn't, so the same map_number / parcel_number that produces the pin
    coordinates also names the street.

    Backed by data/parcel_streets.json, precomputed by
    scripts/build_parcel_street_index.py via Nominatim reverse-geocoding of
    each parcel centroid. Empty-string entries (parcel had no nearby road —
    water, wetland, etc.) are treated as misses.

    Callers should pass the same (map_number, parcel_number) pair that
    successfully resolved coordinates via lookup_parcel(). Mirrors
    lookup_parcel's internal "&" fallback so a result like ("21", "37 & 122")
    — which lookup_parcel resolves via the first parcel — also resolves to
    a street name.
    """
    _load()
    key = f"{map_number} {parcel_number}"
    result = _parcel_streets.get(key)
    if result:  # non-empty string
        return result
    if "&" in parcel_number:
        first_parcel = parcel_number.split("&")[0].strip()
        result = _parcel_streets.get(f"{map_number} {first_parcel}")
        if result:
            return result
    return None
