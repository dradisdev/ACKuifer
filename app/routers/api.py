"""JSON API endpoints for the map frontend."""

import re
from collections import defaultdict
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.config import RETEST_WINDOW_DAYS
from app.database import get_db, SessionLocal
from app.models.results import PfasResult, SourceDiscoveryResult
from app.models.site_config import SiteConfig
from app.geo.parcel_lookup import lookup_parcel

router = APIRouter(prefix="/api", tags=["api"])


# --- Street-name expansion table (subset from massdep.py for display) ---
_STREET_EXPANSIONS = {
    "FULLING MILL": "Fulling Mill Road", "HAMMOCK POND": "Hammock Pond Road",
    "HAMMOCK": "Hammock Pond Road", "POLPIS": "Polpis Road",
    "MILESTONE": "Milestone Road", "MIACOMET": "Miacomet Road",
    "MADEQUECHAM": "Madequecham Valley Road", "SURFSIDE": "Surfside Road",
    "HUMMOCK POND": "Hummock Pond Road", "HUMMOCK": "Hummock Pond Road",
    "FAIRGROUNDS": "Fairgrounds Road", "OLD SOUTH": "Old South Road",
    "TOMS WAY": "Toms Way", "TOM": "Toms Way", "TOMS": "Toms Way",
    "QUIDNET": "Quidnet Road", "MONOMOY": "Monomoy Road",
    "WAUWINET": "Wauwinet Road", "SIASCONSET": "Siasconset",
    "SANKATY": "Sankaty Road", "LOW BEACH": "Low Beach Road",
    "POCOMO": "Pocomo Road", "AMES": "Ames Avenue", "SQUAM": "Squam Road",
    "CLIFF": "Cliff Road", "UPPER TAWPAWSHAW": "Upper Tawpawshaw Road",
    "TAWPAWSHAW": "Tawpawshaw Road", "PLAINFIELD": "Plainfield Road",
    "LONG POND": "Long Pond Drive", "NORWICH": "Norwich Way",
    "NAUSHON": "Naushon Way", "MADAKET": "Madaket Road",
    "TICCOMA": "Ticcoma Way",
}


def _well_id_to_street(well_id: str) -> str:
    """Convert a monitoring well ID to a human-readable street name.

    Uses the Verdantas VDT naming convention from sd_geocoder.py:
        VDT-4FG-4  -> Fairgrounds Road
        VDT-WAITT-12 -> Waitt Drive
        VDT-OSR-1  -> Old South Road
        VDT-TIC-11 -> Ticcoma Way
        VDT-OS-8   -> Old South Road
        MW-1       -> Monitoring Well
        FRB-ACK-4  -> Monitoring Well
    """
    upper = well_id.upper()

    # VDT naming convention: VDT-{site_code}-{num}
    if "FG" in upper:
        return "Fairgrounds Road"
    if "WAITT" in upper:
        return "Waitt Drive"
    if "OSR" in upper or "OS-" in upper:
        return "Old South Road"
    if "TW" in upper or "TOMSWAY" in upper:
        return "Toms Way"
    if "TIC" in upper:
        return "Ticcoma Way"

    # FRB with location hints
    if "MADUK" in upper or "MADUKET" in upper:
        return "Madaket Road"

    return "Monitoring Well"


def _clean_sd_street_name(sample_location: str) -> str:
    """Strip house number and medium suffix from a Source Discovery sample_location.

    Per PRD Section 13: house numbers must never be displayed publicly.
    Medium suffixes like (groundwater)/(drinking_water) are internal data.

    Examples:
        "4 TOMS WAY-3 (drinking_water)" -> "Toms Way"
        "VDT-4FG-4 (groundwater)"       -> "VDT-4FG-4"
        "Old South Road, Nantucket, MA (groundwater)" -> "Old South Road"
    """
    s = sample_location.strip()

    # 1. Strip medium suffix
    s = re.sub(r"\s*\((groundwater|drinking_water|soil)\)\s*$", "", s, flags=re.I)

    # 2. Strip depth brackets like [0.5-0.75'] or (26-28')
    s = re.sub(r"\s*[\[\(][\d.\-']*[\]\)]?\s*$", "", s).strip()

    # 3. If it's a well ID, expand to a readable street name
    if re.match(r"^(VDT|MW|SB|GW|EB|FRB)", s, re.I):
        return _well_id_to_street(s)

    # 4. If it's already a full address like "Old South Road, Nantucket, MA",
    #    return the street name portion only (strip city/state and house number)
    if "," in s:
        s = s.split(",")[0].strip()

    # 5. Strip _INF/_EFF/_PD/_PS/_2 suffixes
    s = re.sub(r"_(INF|EFF|PD|PS|2)$", "", s, flags=re.I).strip()

    # 6. Strip well-unit suffixes like -3, -C-3, -R-3, -M-3
    s = re.sub(r"[-_][A-Z]?[-_]?\d+$", "", s).strip()

    # 7. Strip house number prefix (e.g. "4 FULLING MILL" -> "FULLING MILL")
    m = re.match(r"^\d+[A-Z]?\s+(.+)$", s)
    if m:
        street_abbrev = m.group(1).strip()
    else:
        street_abbrev = s

    # 8. Expand abbreviated street name to proper case
    key = street_abbrev.upper()
    for abbrev in sorted(_STREET_EXPANSIONS.keys(), key=len, reverse=True):
        if abbrev in key:
            return _STREET_EXPANSIONS[abbrev]

    # 9. Title-case if still all-caps, fix apostrophe casing
    if street_abbrev.isupper():
        titled = street_abbrev.title()
        # Fix "'S " from title() (e.g. "Scott'S Way" -> "Scott's Way")
        titled = re.sub(r"'S\b", "'s", titled)
        return titled

    return street_abbrev


@router.get("/results")
def get_results(
    neighborhood: Optional[str] = None,
    status: Optional[str] = None,
    days: Optional[int] = None,
    db: Session = Depends(get_db),
):
    """Return all geocoded PFAS results from both data sources."""
    status_list = [s.strip() for s in status.split(",")] if status else None
    cutoff = date.today() - timedelta(days=days) if days else None

    results = []

    # --- Laserfiche / Board of Health results ---
    lf_query = db.query(PfasResult)
    if neighborhood:
        lf_query = lf_query.filter(PfasResult.neighborhood == neighborhood)
    if status_list:
        lf_query = lf_query.filter(PfasResult.result_status.in_(status_list))
    if cutoff:
        lf_query = lf_query.filter(PfasResult.sample_date >= cutoff)

    for r in lf_query.all():
        coords = lookup_parcel(r.map_number, r.parcel_number)
        if coords is None:
            continue
        lat, lng = coords
        doc_url = (
            f"https://portal.laserfiche.com/Portal/DocView.aspx"
            f"?id={r.laserfiche_doc_id}&repo=r-ec7bdbfe"
        )
        results.append({
            "id": str(r.id),
            "source": "laserfiche",
            "lat": lat,
            "lng": lng,
            "neighborhood": r.neighborhood,
            "street_name": r.street_name,
            "map_number": r.map_number,
            "parcel_number": r.parcel_number,
            "pfas6_sum": float(r.pfas6_sum) if r.pfas6_sum is not None else None,
            "result_status": r.result_status,
            "sample_date": r.sample_date.isoformat() if r.sample_date else None,
            "source_doc_url": doc_url,
            "is_retest": False,
            "retest_group_id": None,
        })

    # --- MassDEP Source Discovery results ---
    sd_query = db.query(SourceDiscoveryResult).filter(
        SourceDiscoveryResult.latitude.isnot(None),
        SourceDiscoveryResult.longitude.isnot(None),
        SourceDiscoveryResult.geocode_review_needed == False,
    )
    if neighborhood:
        sd_query = sd_query.filter(SourceDiscoveryResult.neighborhood == neighborhood)
    if status_list:
        sd_query = sd_query.filter(SourceDiscoveryResult.result_status.in_(status_list))
    if cutoff:
        sd_query = sd_query.filter(SourceDiscoveryResult.sample_date >= cutoff)

    for r in sd_query.all():
        # Extract base doc URL (strip the fragment we appended for uniqueness)
        base_url = r.source_doc_url.split("#")[0] if r.source_doc_url else None
        results.append({
            "id": str(r.id),
            "source": "massdep",
            "lat": float(r.latitude),
            "lng": float(r.longitude),
            "neighborhood": r.neighborhood,
            "street_name": _clean_sd_street_name(r.sample_location),
            "pfas6_sum": float(r.pfas6_sum) if r.pfas6_sum is not None else None,
            "result_status": r.result_status,
            "sample_date": r.sample_date.isoformat() if r.sample_date else None,
            "source_doc_url": base_url,
            "is_retest": False,
            "retest_group_id": None,
        })

    # --- Flag laserfiche retests ---
    retest_window = _get_retest_window_days(db)
    parcels: dict[str, list[dict]] = defaultdict(list)
    for r in results:
        if r["source"] == "laserfiche" and r.get("map_number") and r.get("parcel_number"):
            key = f"{r['map_number']}-{r['parcel_number']}"
            parcels[key].append(r)
    for key, group in parcels.items():
        if len(group) < 2:
            continue
        group.sort(key=lambda x: x["sample_date"] or "")
        has_retest = False
        for i in range(1, len(group)):
            prev_date = group[i - 1]["sample_date"]
            curr_date = group[i]["sample_date"]
            if prev_date and curr_date:
                days_apart = (date.fromisoformat(curr_date) - date.fromisoformat(prev_date)).days
                if days_apart <= retest_window:
                    group[i]["is_retest"] = True
                    has_retest = True
        if has_retest:
            gid = group[0]["id"]
            for r in group:
                r["retest_group_id"] = gid

    return results


def _get_retest_window_days(db: Session) -> int:
    """Read retest_window_days from site_config, falling back to config.py default."""
    try:
        row = db.query(SiteConfig).filter(SiteConfig.key == "retest_window_days").first()
        if row and row.value:
            return int(row.value)
    except Exception:
        pass
    return RETEST_WINDOW_DAYS
