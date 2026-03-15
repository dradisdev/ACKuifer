"""Geo pipeline: parcel ID → centroid → neighborhood."""

import logging
from typing import Optional

from app.geo.parcel_lookup import lookup_parcel
from app.geo.neighborhood import lookup_neighborhood, FALLBACK_NEIGHBORHOOD

logger = logging.getLogger(__name__)


def resolve_location(
    map_number: str, parcel_number: str
) -> Optional[dict]:
    """Resolve a parcel to coordinates and neighborhood.

    Returns:
        Dict with keys: lat, lng, neighborhood, geocode_method
        None if parcel not found in MassGIS data.
    """
    coords = lookup_parcel(map_number, parcel_number)
    if coords is None:
        logger.warning(
            "Cannot resolve location: parcel %s %s not found",
            map_number, parcel_number,
        )
        return None

    lat, lng = coords
    neighborhood = lookup_neighborhood(lat, lng)

    return {
        "lat": lat,
        "lng": lng,
        "neighborhood": neighborhood,
        "geocode_method": "massgis_parcel",
    }
