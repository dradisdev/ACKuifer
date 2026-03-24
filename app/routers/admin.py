"""Admin interface — password-protected dashboard for operator."""

import hashlib
import logging
import secrets
import threading
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.config import settings
from app.database import SessionLocal, get_db
from app.models.results import PfasResult, SourceDiscoveryResult
from app.models.scraper import ScrapeRun, SeenDocument
from app.models.users import Subscription, User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["admin"])
templates = Jinja2Templates(directory="app/templates")

# Simple session store: token -> expiry timestamp
_sessions: dict[str, datetime] = {}
_SESSION_COOKIE = "ackuifer_admin"
_SESSION_MAX_AGE = 8 * 60 * 60  # 8 hours


def _is_authenticated(request: Request) -> bool:
    """Check if the request has a valid admin session cookie."""
    token = request.cookies.get(_SESSION_COOKIE)
    if not token:
        return False
    expiry = _sessions.get(token)
    if not expiry:
        return False
    if datetime.now(timezone.utc) > expiry:
        _sessions.pop(token, None)
        return False
    return True


def _require_auth(request: Request):
    """Dependency that redirects to login if not authenticated."""
    if not _is_authenticated(request):
        return None
    return True


# --- Login / Logout ---

@router.get("/login", response_class=HTMLResponse)
def admin_login_form(request: Request, error: str = Query("")):
    if _is_authenticated(request):
        return RedirectResponse(url="/admin", status_code=303)
    return templates.TemplateResponse("admin_login.html", context={
        "request": request,
        "error": error,
    })


@router.post("/login")
def admin_login(request: Request, password: str = Form(...)):
    if not secrets.compare_digest(password, settings.admin_password):
        return RedirectResponse(url="/admin/login?error=Invalid+password", status_code=303)

    token = secrets.token_urlsafe(32)
    _sessions[token] = datetime.now(timezone.utc) + timedelta(seconds=_SESSION_MAX_AGE)

    response = RedirectResponse(url="/admin", status_code=303)
    response.set_cookie(
        key=_SESSION_COOKIE,
        value=token,
        max_age=_SESSION_MAX_AGE,
        httponly=True,
        samesite="lax",
    )
    return response


@router.get("/logout")
def admin_logout(request: Request):
    token = request.cookies.get(_SESSION_COOKIE)
    if token:
        _sessions.pop(token, None)
    response = RedirectResponse(url="/admin/login", status_code=303)
    response.delete_cookie(_SESSION_COOKIE)
    return response


# --- Dashboard ---

@router.get("", response_class=HTMLResponse)
def admin_dashboard(request: Request, db: Session = Depends(get_db)):
    if not _is_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    now = datetime.now(timezone.utc)
    deadmans_cutoff = now - timedelta(days=settings.deadmans_window_days)

    # --- Scraper status ---
    def _scraper_status(source: str) -> dict:
        last_success = (
            db.query(ScrapeRun)
            .filter(ScrapeRun.source == source, ScrapeRun.status == "success")
            .order_by(ScrapeRun.completed_at.desc())
            .first()
        )
        last_run = (
            db.query(ScrapeRun)
            .filter(ScrapeRun.source == source)
            .order_by(ScrapeRun.started_at.desc())
            .first()
        )
        currently_running = (
            db.query(ScrapeRun)
            .filter(ScrapeRun.source == source, ScrapeRun.status == "running")
            .first()
        )
        healthy = bool(
            last_success
            and last_success.completed_at
            and last_success.completed_at >= deadmans_cutoff
        )
        return {
            "last_success": last_success,
            "last_run": last_run,
            "currently_running": currently_running,
            "healthy": healthy,
        }

    lf_status = _scraper_status("laserfiche")
    sd_status = _scraper_status("massdep")

    # --- Dead man's switch email ---
    _check_deadmans_alerts(lf_status, sd_status, now)

    # --- Parse error queue ---
    lf_errors = (
        db.query(SeenDocument)
        .filter(SeenDocument.source == "laserfiche", SeenDocument.parse_status == "error")
        .order_by(SeenDocument.discovered_at.desc())
        .all()
    )
    sd_errors = (
        db.query(SeenDocument)
        .filter(SeenDocument.source == "massdep", SeenDocument.parse_status == "error")
        .order_by(SeenDocument.discovered_at.desc())
        .all()
    )

    # --- Geocode review queue ---
    geocode_queue = (
        db.query(SourceDiscoveryResult)
        .filter(SourceDiscoveryResult.geocode_review_needed.is_(True))
        .order_by(SourceDiscoveryResult.sample_location)
        .all()
    )

    # --- Subscriber summary ---
    total_confirmed = (
        db.query(func.count(User.id))
        .filter(User.confirmed_at.isnot(None), User.unsubscribed_at.is_(None))
        .scalar()
    )
    with_mobile = (
        db.query(func.count(User.id))
        .filter(
            User.confirmed_at.isnot(None),
            User.unsubscribed_at.is_(None),
            User.mobile.isnot(None),
        )
        .scalar()
    )
    hood_counts = (
        db.query(Subscription.neighborhood, func.count(Subscription.id))
        .join(User)
        .filter(User.confirmed_at.isnot(None), User.unsubscribed_at.is_(None))
        .group_by(Subscription.neighborhood)
        .order_by(func.count(Subscription.id).desc())
        .all()
    )

    # --- Editable content (site_config) ---
    from app.models.site_config import SiteConfig
    configs = db.query(SiteConfig).all()
    config_map = {c.key: c.value for c in configs}

    return templates.TemplateResponse("admin.html", context={
        "request": request,
        "lf_status": lf_status,
        "sd_status": sd_status,
        "lf_errors": lf_errors,
        "sd_errors": sd_errors,
        "geocode_queue": geocode_queue,
        "total_confirmed": total_confirmed,
        "with_mobile": with_mobile,
        "hood_counts": hood_counts,
        "config_map": config_map,
        "deadmans_window": settings.deadmans_window_days,
    })


# --- Geocode review: update coordinates ---

@router.post("/geocode-resolve", response_class=HTMLResponse)
def geocode_resolve(
    request: Request,
    result_id: int = Form(...),
    latitude: float = Form(...),
    longitude: float = Form(...),
    db: Session = Depends(get_db),
):
    if not _is_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    result = db.query(SourceDiscoveryResult).filter(SourceDiscoveryResult.id == result_id).first()
    if result:
        result.latitude = latitude
        result.longitude = longitude
        result.geocode_review_needed = False
        # Re-resolve neighborhood
        from app.geo.neighborhood import lookup_neighborhood
        result.neighborhood = lookup_neighborhood(float(latitude), float(longitude))
        db.commit()
        logger.info("Geocode resolved: SD result %d → (%s, %s) → %s",
                     result_id, latitude, longitude, result.neighborhood)

    return RedirectResponse(url="/admin#geocode-queue", status_code=303)


# --- Manual scraper triggers ---

@router.post("/run-laserfiche")
def run_laserfiche_trigger(request: Request):
    if not _is_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=303)
    _run_scraper_background("laserfiche")
    return RedirectResponse(url="/admin", status_code=303)


@router.post("/run-massdep")
def run_massdep_trigger(request: Request):
    if not _is_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=303)
    _run_scraper_background("massdep")
    return RedirectResponse(url="/admin", status_code=303)


def _run_scraper_background(source: str):
    """Launch a scraper in a background thread."""
    def _run():
        try:
            if source == "laserfiche":
                from app.scrapers.laserfiche import run_laserfiche_scraper
                run_laserfiche_scraper()
            else:
                from app.scrapers.massdep import run_massdep_scraper
                run_massdep_scraper()
        except Exception:
            logger.exception("Background %s scraper failed", source)

    thread = threading.Thread(target=_run, daemon=True, name=f"scraper-{source}")
    thread.start()
    logger.info("Started background %s scraper", source)


# --- Editable content save ---

@router.post("/save-config")
def save_config(request: Request, db: Session = Depends(get_db)):
    if not _is_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    import asyncio
    # We need the form data; FastAPI Form() params are clunky for dynamic keys,
    # so we parse the raw form body.
    # Use a sync workaround to get form data.
    loop = asyncio.new_event_loop()
    form_data = loop.run_until_complete(request.form())
    loop.close()

    from app.models.site_config import SiteConfig

    now = datetime.now(timezone.utc)
    for key, value in form_data.items():
        existing = db.query(SiteConfig).filter(SiteConfig.key == key).first()
        if existing:
            existing.value = str(value)
            existing.updated_at = now
        else:
            db.add(SiteConfig(key=key, value=str(value), updated_at=now))
    db.commit()

    return RedirectResponse(url="/admin#editable-content", status_code=303)


# --- Dead man's switch alert logic ---

_last_deadmans_alert: dict[str, datetime] = {}  # source -> last alert sent time


def _check_deadmans_alerts(lf_status: dict, sd_status: dict, now: datetime):
    """Send dead man's switch alerts if either scraper is overdue."""
    if not settings.operator_email:
        return

    for source, status in [("laserfiche", lf_status), ("massdep", sd_status)]:
        if status["healthy"]:
            continue
        # Don't send more than once per day
        last_sent = _last_deadmans_alert.get(source)
        if last_sent and (now - last_sent) < timedelta(hours=24):
            continue

        last_run = status["last_success"]
        last_run_at = last_run.completed_at if last_run else None

        try:
            from app.notifications.email import send_deadmans_alert
            send_deadmans_alert(source, last_run_at)
            _last_deadmans_alert[source] = now
        except Exception:
            logger.exception("Failed to send dead man's switch alert for %s", source)
