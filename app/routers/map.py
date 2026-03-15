"""Map and landing page routes."""

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.config import settings

router = APIRouter(tags=["pages"])
templates = Jinja2Templates(directory="app/templates")


@router.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@router.post("/search")
def search(request: Request):
    # Stub: redirect to map. Future: geocode address, resolve neighborhood.
    return RedirectResponse(url="/map", status_code=303)


@router.get("/map", response_class=HTMLResponse)
def map_page(request: Request):
    return templates.TemplateResponse("map.html", {
        "request": request,
        "mapbox_token": settings.mapbox_public_token,
    })
