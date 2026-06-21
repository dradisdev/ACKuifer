"""Admin interface — password-protected dashboard for operator."""

import hashlib
import logging
import secrets
import threading
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.config import (
    DISCREPANCY_TOLERANCE_PPT,
    MCL,
    classify_result_status,
    settings,
)
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
    return templates.TemplateResponse("admin_login.html", {
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

    # --- Scraper status (shared logic: see app/monitoring/health.py) ---
    # Both this dashboard and the independent monitor cron call the SAME
    # functions, so the page-load path and the scheduled path can never drift,
    # and they share one durable cooldown so they cannot double-send.
    from app.monitoring.health import check_and_alert, compute_scraper_status

    lf_status = compute_scraper_status(db, "laserfiche", now=now)
    sd_status = compute_scraper_status(db, "massdep", now=now)

    # --- Dead man's switch email (durable, cross-process dedup) ---
    check_and_alert(db, {"laserfiche": lf_status, "massdep": sd_status}, now=now)

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

    # --- PFAS6 discrepancy review queue (query-time; no migration) ---
    # Lab-math class: stored pfas6_sum (the lab's certified total) disagrees
    # with the raw sum of the six component columns by more than tolerance.
    # Hidden records are excluded, so the HIDE action removes the row.
    _component_cols = ("pfos", "pfoa", "pfhxs", "pfna", "pfhpa", "pfda")
    _tol = Decimal(str(DISCREPANCY_TOLERANCE_PPT))
    _disc_candidates = (
        db.query(PfasResult)
        .filter(
            PfasResult.pfas6_sum.isnot(None),
            PfasResult.pfas6_sum > 0,
            PfasResult.hidden == False,  # noqa: E712
        )
        .all()
    )
    discrepancy_queue = []
    for _r in _disc_candidates:
        _comp_sum = sum(
            (getattr(_r, _c) if getattr(_r, _c) is not None else Decimal(0))
            for _c in _component_cols
        )
        _diff = _r.pfas6_sum - _comp_sum
        if abs(_diff) > _tol:
            discrepancy_queue.append({"r": _r, "comp_sum": _comp_sum, "diff": _diff})
    discrepancy_queue.sort(key=lambda d: abs(d["diff"]), reverse=True)

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

    return templates.TemplateResponse("admin.html", {
        "request": request,
        "lf_status": lf_status,
        "sd_status": sd_status,
        "lf_errors": lf_errors,
        "sd_errors": sd_errors,
        "geocode_queue": geocode_queue,
        "discrepancy_queue": discrepancy_queue,
        "discrepancy_tol": DISCREPANCY_TOLERANCE_PPT,
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


# --- Hide / Unhide results ---

@router.post("/hide-result")
def hide_result(
    request: Request,
    result_id: str = Form(...),
    source: str = Form(...),
    redirect_anchor: str = Form("hide-unhide"),
    db: Session = Depends(get_db),
):
    if not _is_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    if source == "laserfiche":
        record = db.query(PfasResult).filter(PfasResult.id == result_id).first()
    else:
        record = db.query(SourceDiscoveryResult).filter(SourceDiscoveryResult.id == int(result_id)).first()

    if record:
        record.hidden = True
        db.commit()
        logger.info("Hidden %s result %s", source, result_id)

    return RedirectResponse(url=f"/admin#{redirect_anchor}", status_code=303)


@router.post("/unhide-result")
def unhide_result(
    request: Request,
    result_id: str = Form(...),
    source: str = Form(...),
    db: Session = Depends(get_db),
):
    if not _is_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    if source == "laserfiche":
        record = db.query(PfasResult).filter(PfasResult.id == result_id).first()
    else:
        record = db.query(SourceDiscoveryResult).filter(SourceDiscoveryResult.id == int(result_id)).first()

    if record:
        record.hidden = False
        db.commit()
        logger.info("Unhidden %s result %s", source, result_id)

    return RedirectResponse(url="/admin#hide-unhide", status_code=303)


# --- PFAS6 override (discrepancy review) ---

@router.post("/override-pfas6")
def override_pfas6(
    request: Request,
    result_id: str = Form(...),
    new_value: float = Form(...),
    db: Session = Depends(get_db),
):
    """Set the stored pfas6_sum to an admin-verified value (from the PDF).

    Recomputes result_status via the same classifier the parser uses, and
    pass_fail from the MCL. Deliberately does NOT touch notified_at and does
    NOT enqueue any notification: admin corrections are silent.
    """
    if not _is_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    record = db.query(PfasResult).filter(PfasResult.id == result_id).first()
    if record and new_value >= 0:
        old_sum, old_status = record.pfas6_sum, record.result_status
        record.pfas6_sum = Decimal(str(new_value))
        record.result_status = classify_result_status(new_value)
        record.pass_fail = "PASS" if new_value <= MCL else "FAIL"
        db.commit()
        logger.info(
            "PFAS6 override: doc %s pfas6 %s -> %s, status %s -> %s",
            record.laserfiche_doc_id, old_sum, new_value,
            old_status, record.result_status,
        )

    return RedirectResponse(url="/admin#discrepancy-queue", status_code=303)


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
    from app.monitoring.health import is_reserved_config_key

    now = datetime.now(timezone.utc)
    for key, value in form_data.items():
        # Internal bookkeeping keys (e.g. deadmans dedup timestamps) share this
        # table but must never be editable via the operator content form.
        if is_reserved_config_key(key):
            logger.warning("Refused to overwrite reserved site_config key via form: %s", key)
            continue
        existing = db.query(SiteConfig).filter(SiteConfig.key == key).first()
        if existing:
            existing.value = str(value)
            existing.updated_at = now
        else:
            db.add(SiteConfig(key=key, value=str(value), updated_at=now))
    db.commit()

    return RedirectResponse(url="/admin#editable-content", status_code=303)
