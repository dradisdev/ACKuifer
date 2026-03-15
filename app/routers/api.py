"""JSON API endpoints for the map frontend."""

from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.results import PfasResult, SourceDiscoveryResult
from app.geo.parcel_lookup import lookup_parcel

router = APIRouter(prefix="/api", tags=["api"])


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
            "pfas6_sum": float(r.pfas6_sum) if r.pfas6_sum is not None else None,
            "result_status": r.result_status,
            "sample_date": r.sample_date.isoformat() if r.sample_date else None,
            "source_doc_url": doc_url,
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
            "street_name": r.sample_location,
            "pfas6_sum": float(r.pfas6_sum) if r.pfas6_sum is not None else None,
            "result_status": r.result_status,
            "sample_date": r.sample_date.isoformat() if r.sample_date else None,
            "source_doc_url": base_url,
        })

    return results
