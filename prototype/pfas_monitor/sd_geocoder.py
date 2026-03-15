"""
sd_geocoder.py
--------------
Resolves monitoring well locations to lat/lng coordinates.

Source Discovery reports use MassDEP / Verdantas well IDs (e.g. VDT-4FG-4)
rather than street addresses.  We have several resolution strategies, applied
in order:

1. GPS coordinates embedded in the PDF report (best — already handled in parser)
2. Manually curated lookup table for the known Fairgrounds campus wells
3. Nantucket MapGeo GIS — address → lat/lng (for reports that cite an address)
4. Fallback: cluster around the project centroid (2 Fairgrounds Road)

This module also provides a Nantucket address → lat/lng function that wraps
the MapGeo API used by the existing voluntary-programme address_lookup.py,
so both datasets use the same geocoding backend.
"""

import re
import json
import time
import asyncio
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Optional, Union

# ── Known well coordinates for RTN 4-0029612 (Fairgrounds campus) ─────────────
# Source: MassDEP / Verdantas field reports (January 2025 dataset)
# These are the 12 monitoring wells installed Nov–Dec 2024.
# Add new wells here as future reports are filed.

KNOWN_WELL_COORDS: dict[str, dict] = {
    "VDT-WAITT-12": {"lat": 41.2845, "lng": -70.0598, "address": "Waitt Drive, Nantucket"},
    "VDT-4FG-4":    {"lat": 41.2801, "lng": -70.0623, "address": "4 Fairgrounds Road, Nantucket"},
    "VDT-2FG-5":    {"lat": 41.2799, "lng": -70.0628, "address": "2 Fairgrounds Road, Nantucket"},
    "VDT-2FG-6":    {"lat": 41.2797, "lng": -70.0631, "address": "2 Fairgrounds Road, Nantucket"},
    "VDT-6FG-7":    {"lat": 41.2803, "lng": -70.0617, "address": "6 Fairgrounds Road, Nantucket"},
    "VDT-6FG-8":    {"lat": 41.2806, "lng": -70.0612, "address": "6 Fairgrounds Road, Nantucket"},
    "VDT-6FG-9":    {"lat": 41.2808, "lng": -70.0609, "address": "6 Fairgrounds Road, Nantucket"},
    "VDT-6FG-10":   {"lat": 41.2810, "lng": -70.0605, "address": "6 Fairgrounds Road, Nantucket"},
    "VDT-6FG-11":   {"lat": 41.2812, "lng": -70.0601, "address": "6 Fairgrounds Road, Nantucket"},
    "VDT-OSR-1":    {"lat": 41.2825, "lng": -70.0585, "address": "Old South Road, Nantucket"},
    "VDT-OSR-2":    {"lat": 41.2828, "lng": -70.0582, "address": "Old South Road, Nantucket"},
    "VDT-OSR-3":    {"lat": 41.2831, "lng": -70.0578, "address": "Old South Road, Nantucket"},
    # Airport-area wells (earlier MCP investigation)
    "MW-1":  {"lat": 41.2533, "lng": -70.0621, "address": "Airport Road, Nantucket"},
    "MW-2":  {"lat": 41.2539, "lng": -70.0615, "address": "Airport Road, Nantucket"},
    "MW-3":  {"lat": 41.2545, "lng": -70.0608, "address": "Airport Road, Nantucket"},
}

# Project centroid — used as fallback when we can't resolve a well
PROJECT_CENTROID = {"lat": 41.2802, "lng": -70.0625}

# ── MapGeo Nantucket GIS (same backend as voluntary address_lookup.py) ────────

MAPGEO_BASE = "https://nantucket.mapgeo.io"

def address_to_latlong(address: str) -> Optional[dict]:
    """
    Convert a Nantucket street address to {lat, lng, canonical_address}.
    Uses the Nantucket MapGeo API — same approach as address_lookup.py.
    Returns None if address cannot be resolved.
    """
    # Ensure "Nantucket" is in the query
    if "nantucket" not in address.lower():
        address = f"{address}, Nantucket, MA"

    encoded = urllib.parse.quote(address)
    url = f"{MAPGEO_BASE}/datasets/parcels/query?query={encoded}&limit=5"

    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "ACKuifer/1.0",
                "Accept":     "application/json",
                "Referer":    MAPGEO_BASE,
            }
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        candidates = data.get("features") or data.get("results") or []
        if not candidates:
            return None

        top = candidates[0]
        geom = top.get("geometry") or {}
        coords = geom.get("coordinates", [])
        if not coords or len(coords) < 2:
            return None

        lng, lat = coords[0], coords[1]
        props = top.get("properties", {})
        canonical = (
            props.get("address")
            or props.get("siteAddress")
            or props.get("streetAddress")
            or address
        )
        return {"lat": lat, "lng": lng, "canonical_address": canonical}

    except Exception as e:
        print(f"  [Geocoder] MapGeo lookup failed for '{address}': {e}")
        return None


# ── Well ID → coordinates ─────────────────────────────────────────────────────

def resolve_well(well_id: str, fallback_address: Optional[str] = None) -> dict:
    """
    Return {lat, lng, address, geocode_method} for a given well ID.

    Tries (in order):
    1. Known well lookup table
    2. Address geocoding (if fallback_address provided)
    3. Project centroid fallback
    """
    # Normalise well ID
    wid_upper = well_id.upper().replace(" ", "-")

    # Strategy 1 — known table
    if wid_upper in KNOWN_WELL_COORDS:
        entry = KNOWN_WELL_COORDS[wid_upper]
        return {**entry, "geocode_method": "known_table"}

    # Strategy 2 — derive address from well ID suffix
    # e.g. "VDT-4FG-4" → "4 Fairgrounds Road"
    derived_address = _derive_address_from_well_id(well_id)
    if derived_address:
        coords = address_to_latlong(derived_address)
        if coords:
            return {
                "lat":            coords["lat"],
                "lng":            coords["lng"],
                "address":        coords["canonical_address"],
                "geocode_method": "derived_address",
            }

    # Strategy 3 — caller-provided address
    if fallback_address:
        coords = address_to_latlong(fallback_address)
        if coords:
            return {
                "lat":            coords["lat"],
                "lng":            coords["lng"],
                "address":        coords["canonical_address"],
                "geocode_method": "provided_address",
            }

    # Strategy 4 — centroid fallback
    return {
        **PROJECT_CENTROID,
        "address":        "2 Fairgrounds Road, Nantucket, MA (estimated)",
        "geocode_method": "centroid_fallback",
    }


def _derive_address_from_well_id(well_id: str) -> Optional[str]:
    """
    Parse Verdantas well ID naming convention to infer a street address.
    VDT-{site_code}-{num} where site_code encodes location:
      2FG  → 2 Fairgrounds Road
      4FG  → 4 Fairgrounds Road
      6FG  → 6 Fairgrounds Road
      WAITT → Waitt Drive
      OSR  → Old South Road
      TW   → Tom's Way
    """
    m = re.search(
        r"(?:VDT[-_]?)?((\d+)FG|WAITT|OSR|TW|TOMSWAY|FAIRGROUNDS|AIRPORT)",
        well_id, re.I
    )
    if not m:
        return None

    code = m.group(1).upper()
    num  = m.group(2)   # None for non-numeric codes (WAITT, OSR, etc.)

    mapping = {
        "FG":         f"{num} Fairgrounds Road, Nantucket, MA" if num else "Fairgrounds Road, Nantucket, MA",
        "WAITT":      "Waitt Drive, Nantucket, MA",
        "OSR":        "Old South Road, Nantucket, MA",
        "TW":         "Tom's Way, Nantucket, MA",
        "TOMSWAY":    "Tom's Way, Nantucket, MA",
        "FAIRGROUNDS":"Fairgrounds Road, Nantucket, MA",
        "AIRPORT":    "14 Airport Road, Nantucket, MA",
    }

    for key, addr in mapping.items():
        if code.endswith(key) or code == key:
            return addr

    return None


# ── Enrich a database's unlocated wells ───────────────────────────────────────

def enrich_locations(db_path: Union[str, Path]) -> int:
    """
    Load source_discovery.json, attempt to geocode all sample locations that
    lack lat/lng, and save the enriched file.  Returns count of newly geocoded.
    """
    from source_discovery_db import SourceDiscoveryDB

    db = SourceDiscoveryDB(db_path)
    enriched = 0

    for report in db.all_reports():
        for loc in report.get("sample_locations", []):
            if loc.get("lat") and loc.get("lng"):
                continue   # already has coords

            resolved = resolve_well(
                loc["well_id"],
                fallback_address=report.get("project_address"),
            )
            loc["lat"]            = resolved["lat"]
            loc["lng"]            = resolved["lng"]
            loc["address"]        = resolved.get("address")
            loc["geocode_method"] = resolved.get("geocode_method", "unknown")
            enriched += 1
            time.sleep(0.3)   # polite rate-limiting on MapGeo

    if enriched:
        db.save()
        print(f"[Geocoder] Enriched {enriched} locations → {db_path}")
    else:
        print("[Geocoder] All locations already have coordinates")

    return enriched


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python sd_geocoder.py enrich <source_discovery.json>")
        print("  python sd_geocoder.py lookup <address>")
        print("  python sd_geocoder.py well   <well_id>")
        sys.exit(1)

    cmd = sys.argv[1].lower()

    if cmd == "enrich":
        path = sys.argv[2] if len(sys.argv) > 2 else "source_discovery.json"
        enrich_locations(path)

    elif cmd == "lookup":
        addr = " ".join(sys.argv[2:])
        result = address_to_latlong(addr)
        print(json.dumps(result, indent=2))

    elif cmd == "well":
        wid = sys.argv[2]
        result = resolve_well(wid)
        print(json.dumps(result, indent=2))

    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
