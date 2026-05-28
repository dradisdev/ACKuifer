"""Precompute (map+parcel) -> street name index via Nominatim reverse-geocoding.

Reads data/nantucket_parcels.geojson, walks every parcel feature, computes the
centroid, and asks Nominatim what road that point sits on. Writes the result
to data/parcel_streets.json as a flat dict: {"MAP_PAR_ID": "Street Name", ...}.

Used as a fallback for the map popup / table when a Laserfiche PDF has no
embedded sample address (third-party-submitter reports like Eurofins prepared
for Island Water Filtration). The parcel centroid is already known with full
accuracy — this script just labels each parcel with the road it sits on.

Idempotent: skips parcels already in the index, so the script can be killed
and restarted without losing work. Saves progress every 25 new entries.

Polite to Nominatim's usage policy (1 req/sec, identifying User-Agent).
Nantucket has on the order of 10,000 parcels; a full run takes ~3 hours.
Run overnight.

Usage:
    python scripts/build_parcel_street_index.py
    python scripts/build_parcel_street_index.py --limit 50      # quick test
    python scripts/build_parcel_street_index.py --force         # re-geocode all

After running, commit data/parcel_streets.json and redeploy. The app picks
up the new file at the next process start.
"""
import argparse
import json
import logging
import sys
import time
from pathlib import Path

import requests
from shapely.geometry import shape

logger = logging.getLogger(__name__)

NOMINATIM_URL = "https://nominatim.openstreetmap.org/reverse"
USER_AGENT = "ACKuifer parcel index builder (https://github.com/dradisdev/ACKuifer)"
RATE_LIMIT_SEC = 1.1   # slightly above 1/sec to stay polite
SAVE_EVERY = 25        # checkpoint frequency (entries since last save)

# Fields on the Nominatim response we'll accept as "the road this parcel
# is on", in order of preference. Most parcels resolve to `road`. Smaller
# Nantucket parcels along private drives may resolve to one of the others.
_ROAD_FIELDS = ("road", "pedestrian", "footway", "path", "track")


def _reverse_geocode(lat: float, lng: float) -> str:
    """Return the road name for (lat, lng), or '' if Nominatim has none."""
    params = {
        "lat": f"{lat:.6f}",
        "lon": f"{lng:.6f}",
        "format": "jsonv2",
        "zoom": 18,
        "addressdetails": 1,
    }
    resp = requests.get(
        NOMINATIM_URL,
        params=params,
        headers={"User-Agent": USER_AGENT},
        timeout=20,
    )
    resp.raise_for_status()
    data = resp.json()
    addr = data.get("address") or {}
    for fld in _ROAD_FIELDS:
        val = addr.get(fld)
        if val:
            return val
    return ""


def _save(index_path: Path, index: dict) -> None:
    """Atomic-ish save: write to temp then rename."""
    tmp = index_path.with_suffix(index_path.suffix + ".tmp")
    with open(tmp, "w") as f:
        json.dump(index, f, indent=2, sort_keys=True)
    tmp.replace(index_path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=None,
                        help="Stop after N new lookups (handy for testing)")
    parser.add_argument("--force", action="store_true",
                        help="Re-geocode parcels already in the index")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(message)s",
        datefmt="%H:%M:%S",
    )

    repo_root = Path(__file__).resolve().parent.parent
    geojson_path = repo_root / "data" / "nantucket_parcels.geojson"
    index_path = repo_root / "data" / "parcel_streets.json"

    if not geojson_path.exists():
        logger.error("GeoJSON not found at %s", geojson_path)
        return 1

    logger.info("Loading parcels from %s", geojson_path)
    with open(geojson_path) as f:
        gj = json.load(f)

    index: dict[str, str] = {}
    if index_path.exists() and not args.force:
        with open(index_path) as f:
            index = json.load(f)
        logger.info("Resuming with %d existing entries", len(index))

    new_count = 0
    skip_count = 0
    fail_count = 0
    null_count = 0
    total_parcels = sum(1 for f in gj["features"]
                        if f["properties"].get("MAP_PAR_ID"))
    logger.info("Found %d parcels in GeoJSON", total_parcels)

    try:
        for feat in gj["features"]:
            par_id = feat["properties"].get("MAP_PAR_ID")
            if not par_id:
                continue

            if par_id in index and not args.force:
                skip_count += 1
                continue

            if args.limit is not None and new_count >= args.limit:
                logger.info("Hit --limit %d, stopping", args.limit)
                break

            geom = shape(feat["geometry"])
            c = geom.centroid

            try:
                road = _reverse_geocode(c.y, c.x)
            except Exception as e:
                logger.warning("Reverse-geocode failed for %s: %s", par_id, e)
                fail_count += 1
                time.sleep(RATE_LIMIT_SEC)
                continue

            index[par_id] = road
            new_count += 1
            if road:
                logger.info("  %s -> %s", par_id, road)
            else:
                null_count += 1
                logger.info("  %s -> (no road)", par_id)

            if new_count % SAVE_EVERY == 0:
                _save(index_path, index)
                logger.info("Checkpoint: %d entries total saved", len(index))

            time.sleep(RATE_LIMIT_SEC)
    except KeyboardInterrupt:
        logger.warning("Interrupted; saving progress before exit")

    _save(index_path, index)
    logger.info(
        "Done. new=%d skipped=%d failed=%d (no-road)=%d total=%d",
        new_count, skip_count, fail_count, null_count, len(index),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())