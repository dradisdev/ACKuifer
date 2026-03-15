"""Centroid → neighborhood lookup.

Loads data/nantucket_neighborhoods.geojson once at import time and provides
a lookup from (latitude, longitude) to neighborhood name.

The file contains a mix of Point and Polygon features. Lookup checks
polygon containment first, then falls back to nearest-point assignment.
"""

import json
import logging
from pathlib import Path

from shapely.geometry import Point, shape

logger = logging.getLogger(__name__)

_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"
_GEOJSON_PATH = _DATA_DIR / "nantucket_neighborhoods.geojson"

FALLBACK_NEIGHBORHOOD = "Nantucket (Island-wide)"

# Neighborhood reference points: [(name, Point), ...]
_neighborhood_points: list[tuple[str, Point]] = []
# Neighborhood polygons: [(name, polygon), ...]
_neighborhood_polygons: list[tuple[str, object]] = []
_loaded = False


def _load():
    global _neighborhood_points, _neighborhood_polygons, _loaded
    if _loaded:
        return

    logger.info("Loading neighborhoods GeoJSON from %s", _GEOJSON_PATH)
    with open(_GEOJSON_PATH) as f:
        gj = json.load(f)

    for feat in gj["features"]:
        name = feat["properties"].get("name")
        if not name:
            continue
        geom_type = feat["geometry"]["type"]
        if geom_type == "Point":
            coords = feat["geometry"]["coordinates"]  # [lng, lat]
            _neighborhood_points.append((name, Point(coords[0], coords[1])))
        elif geom_type in ("Polygon", "MultiPolygon"):
            _neighborhood_polygons.append((name, shape(feat["geometry"])))

    _loaded = True
    all_names = sorted(
        set(n for n, _ in _neighborhood_points)
        | set(n for n, _ in _neighborhood_polygons)
    )
    logger.info(
        "Loaded %d neighborhood points + %d polygons: %s",
        len(_neighborhood_points), len(_neighborhood_polygons), all_names,
    )


def get_all_neighborhoods() -> list[str]:
    """Return sorted list of all neighborhood names in the reference data."""
    _load()
    return sorted(
        set(n for n, _ in _neighborhood_points)
        | set(n for n, _ in _neighborhood_polygons)
    )


def lookup_neighborhood(lat: float, lng: float, max_distance_deg: float = 0.05) -> str:
    """Return the neighborhood name for a coordinate.

    Checks polygon containment first, then nearest-point assignment.

    Args:
        lat: Latitude
        lng: Longitude
        max_distance_deg: Max distance in degrees (~5.5 km) before falling back.

    Returns:
        Neighborhood name, or FALLBACK_NEIGHBORHOOD if no match.
    """
    _load()

    pt = Point(lng, lat)  # Shapely uses (x, y) = (lng, lat)

    # Check polygon containment first
    for name, polygon in _neighborhood_polygons:
        if polygon.contains(pt):
            return name

    # Fall back to nearest point
    best_name = FALLBACK_NEIGHBORHOOD
    best_dist = float("inf")

    for name, npt in _neighborhood_points:
        dist = pt.distance(npt)
        if dist < best_dist:
            best_dist = dist
            best_name = name

    if best_dist > max_distance_deg:
        logger.warning(
            "Point (%.6f, %.6f) is %.4f deg from nearest neighborhood %s; using fallback",
            lat, lng, best_dist, best_name,
        )
        return FALLBACK_NEIGHBORHOOD

    return best_name
