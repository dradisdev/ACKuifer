"""Map and landing page routes."""

import logging
import time
import urllib.parse

import httpx
from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.config import settings
from app.geo.neighborhood import lookup_neighborhood

logger = logging.getLogger(__name__)

router = APIRouter(tags=["pages"])
templates = Jinja2Templates(directory="app/templates")

# Nantucket bounding box for Nominatim viewbox bias
_NANTUCKET_VIEWBOX = "-70.30,41.22,-69.93,41.34"


def _nominatim_search(address: str) -> list:
    """Call Nominatim search API with retry on 429."""
    resp = httpx.get(
        "https://nominatim.openstreetmap.org/search",
        params={
            "q": address,
            "format": "json",
            "limit": 1,
            "viewbox": _NANTUCKET_VIEWBOX,
            "bounded": 1,
        },
        headers={"User-Agent": "ACKuifer/1.0 (ackuifer.org)"},
        timeout=10.0,
    )
    if resp.status_code == 429:
        logger.warning("Nominatim rate limited (429), retrying in 60s")
        time.sleep(60)
        resp = httpx.get(
            "https://nominatim.openstreetmap.org/search",
            params={
                "q": address,
                "format": "json",
                "limit": 1,
                "viewbox": _NANTUCKET_VIEWBOX,
                "bounded": 1,
            },
            headers={"User-Agent": "ACKuifer/1.0 (ackuifer.org)"},
            timeout=10.0,
        )
    resp.raise_for_status()
    return resp.json()


@router.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@router.post("/search")
def search(address: str = Form(...)):
    """Geocode address via Nominatim, resolve neighborhood, redirect to map."""
    address = address.strip()
    if not address:
        return RedirectResponse(url="/map", status_code=303)

    # Append "Nantucket, MA" if not already present
    addr_lower = address.lower()
    if "nantucket" not in addr_lower:
        address = f"{address}, Nantucket, MA"

    try:
        results = _nominatim_search(address)
    except Exception:
        logger.exception("Nominatim geocode failed for: %s", address)
        error = urllib.parse.urlencode({"search_error": "Address search is temporarily unavailable — please try again in a few minutes."})
        return RedirectResponse(url=f"/map?{error}", status_code=303)

    if not results:
        logger.info("No geocode results for: %s", address)
        error = urllib.parse.urlencode({"search_error": f"No results found for \"{address}\". Try a street address like \"10 Surfside Road\"."})
        return RedirectResponse(url=f"/map?{error}", status_code=303)

    lat = float(results[0]["lat"])
    lng = float(results[0]["lon"])
    neighborhood = lookup_neighborhood(lat, lng)

    params = urllib.parse.urlencode({
        "lat": f"{lat:.6f}",
        "lng": f"{lng:.6f}",
        "neighborhood": neighborhood,
        "search_address": address,
    })
    return RedirectResponse(url=f"/map?{params}", status_code=303)


@router.get("/map", response_class=HTMLResponse)
def map_page(request: Request, search_address: str = ""):
    return templates.TemplateResponse("map.html", {
        "request": request,
        "mapbox_token": settings.mapbox_public_token,
        "search_address": search_address,
    })
