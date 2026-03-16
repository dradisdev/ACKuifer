"""Signup, confirmation, unsubscribe, and subscription management routes."""

import logging
import re
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.geo.neighborhood import get_all_neighborhoods
from app.models.users import Subscription, User
from app.notifications.tokens import (
    generate_confirm_token,
    generate_manage_token,
    generate_unsubscribe_token,
    verify_confirm_token,
    verify_manage_token,
    verify_unsubscribe_token,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["signup"])
templates = Jinja2Templates(directory="app/templates")

_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")


def _validate_email(email: str) -> bool:
    return bool(_EMAIL_RE.match(email.strip()))


# --- GET /pfas-info ---
@router.get("/pfas-info", response_class=HTMLResponse)
def pfas_info(request: Request):
    return templates.TemplateResponse("pfas_info.html", {"request": request})


# --- GET /signup ---
@router.get("/signup", response_class=HTMLResponse)
def signup_form(
    request: Request,
    neighborhood: Optional[str] = None,
    db: Session = Depends(get_db),
):
    neighborhoods = get_all_neighborhoods()
    return templates.TemplateResponse("signup.html", {
        "request": request,
        "neighborhoods": neighborhoods,
        "pre_neighborhood": neighborhood,
        "error": None,
    })


# --- POST /signup ---
@router.post("/signup", response_class=HTMLResponse)
def signup_submit(
    request: Request,
    email: str = Form(...),
    mobile: str = Form(""),
    neighborhoods: list = Form(..., alias="neighborhoods"),
    db: Session = Depends(get_db),
):
    all_neighborhoods = get_all_neighborhoods()
    email = email.strip().lower()
    mobile = mobile.strip() or None

    # Validate email
    if not _validate_email(email):
        return templates.TemplateResponse("signup.html", {
            "request": request,
            "neighborhoods": all_neighborhoods,
            "pre_neighborhood": None,
            "error": "Please enter a valid email address.",
        })

    # Validate at least one neighborhood selected
    if not neighborhoods:
        return templates.TemplateResponse("signup.html", {
            "request": request,
            "neighborhoods": all_neighborhoods,
            "pre_neighborhood": None,
            "error": "Please select at least one neighborhood.",
        })

    # Check for existing user
    existing = db.query(User).filter(User.email == email).first()
    if existing:
        if existing.unsubscribed_at:
            # Resubscribing — reactivate
            existing.unsubscribed_at = None
            existing.confirmed_at = None
            existing.mobile = mobile
            # Remove old subscriptions, add new ones
            for sub in existing.subscriptions:
                db.delete(sub)
            for i, hood in enumerate(neighborhoods):
                db.add(Subscription(
                    user_id=existing.id,
                    neighborhood=hood,
                    is_primary=(i == 0),
                ))
            db.commit()
            _send_confirmation(existing, db)
            return templates.TemplateResponse("signup_success.html", {
                "request": request,
                "email": email,
            })
        else:
            # Already subscribed and active
            return templates.TemplateResponse("signup.html", {
                "request": request,
                "neighborhoods": all_neighborhoods,
                "pre_neighborhood": None,
                "error": f"The email {email} is already subscribed. Check your inbox for a manage link, or contact us if you need help.",
            })

    # Create new user (unconfirmed)
    user = User(email=email, mobile=mobile)
    db.add(user)
    db.flush()

    for i, hood in enumerate(neighborhoods):
        db.add(Subscription(
            user_id=user.id,
            neighborhood=hood,
            is_primary=(i == 0),
        ))
    db.commit()
    db.refresh(user)

    _send_confirmation(user, db)

    return templates.TemplateResponse("signup_success.html", {
        "request": request,
        "email": email,
    })


def _send_confirmation(user: User, db: Session):
    """Send confirmation email (imported lazily to avoid circular imports)."""
    try:
        from app.notifications.email import send_confirmation_email
        send_confirmation_email(user, db)
    except Exception:
        logger.exception("Failed to send confirmation email to %s", user.email)


# --- GET /confirm/{token} ---
@router.get("/confirm/{token}", response_class=HTMLResponse)
def confirm_subscription(request: Request, token: str, db: Session = Depends(get_db)):
    user_id = verify_confirm_token(token)
    if not user_id:
        return templates.TemplateResponse("confirm_success.html", {
            "request": request,
            "success": False,
            "error": "Invalid or corrupted confirmation link.",
        })

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        return templates.TemplateResponse("confirm_success.html", {
            "request": request,
            "success": False,
            "error": "Account not found.",
        })

    if not user.confirmed_at:
        user.confirmed_at = datetime.now(timezone.utc)
    user.last_active_at = datetime.now(timezone.utc)
    db.commit()

    hood_names = [s.neighborhood for s in user.subscriptions]

    return templates.TemplateResponse("confirm_success.html", {
        "request": request,
        "success": True,
        "neighborhoods": hood_names,
        "error": None,
    })


# --- GET /unsubscribe/{token} ---
@router.get("/unsubscribe/{token}", response_class=HTMLResponse)
def unsubscribe(request: Request, token: str, db: Session = Depends(get_db)):
    user_id = verify_unsubscribe_token(token)
    if not user_id:
        return templates.TemplateResponse("unsubscribe_success.html", {
            "request": request,
            "success": False,
            "error": "Invalid unsubscribe link.",
        })

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        return templates.TemplateResponse("unsubscribe_success.html", {
            "request": request,
            "success": False,
            "error": "Account not found.",
        })

    if not user.unsubscribed_at:
        user.unsubscribed_at = datetime.now(timezone.utc)
    user.last_active_at = datetime.now(timezone.utc)
    db.commit()

    return templates.TemplateResponse("unsubscribe_success.html", {
        "request": request,
        "success": True,
        "error": None,
    })


# --- GET /unsubscribe (query param version for email links) ---
@router.get("/unsubscribe", response_class=HTMLResponse)
def unsubscribe_query(request: Request, token: str = Query(""), db: Session = Depends(get_db)):
    if not token:
        return templates.TemplateResponse("unsubscribe_success.html", {
            "request": request,
            "success": False,
            "error": "Missing unsubscribe token.",
        })
    return unsubscribe(request, token, db)


# --- GET /manage/{token} ---
@router.get("/manage/{token}", response_class=HTMLResponse)
def manage_subscriptions(request: Request, token: str, db: Session = Depends(get_db)):
    user_id = verify_manage_token(token)
    if not user_id:
        return templates.TemplateResponse("manage.html", {
            "request": request,
            "expired": True,
            "user": None,
            "current_neighborhoods": [],
            "all_neighborhoods": [],
            "token": token,
            "success": None,
        })

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        return templates.TemplateResponse("manage.html", {
            "request": request,
            "expired": False,
            "user": None,
            "current_neighborhoods": [],
            "all_neighborhoods": [],
            "token": token,
            "success": None,
        })

    user.last_active_at = datetime.now(timezone.utc)
    db.commit()

    current = [s.neighborhood for s in user.subscriptions]

    return templates.TemplateResponse("manage.html", {
        "request": request,
        "expired": False,
        "user": user,
        "current_neighborhoods": current,
        "all_neighborhoods": get_all_neighborhoods(),
        "token": token,
        "success": None,
    })


# --- GET /manage (query param version for email links) ---
@router.get("/manage", response_class=HTMLResponse)
def manage_query(request: Request, token: str = Query(""), db: Session = Depends(get_db)):
    if not token:
        return templates.TemplateResponse("manage.html", {
            "request": request,
            "expired": True,
            "user": None,
            "current_neighborhoods": [],
            "all_neighborhoods": [],
            "token": "",
            "success": None,
        })
    return manage_subscriptions(request, token, db)


# --- POST /manage/{token} ---
@router.post("/manage/{token}", response_class=HTMLResponse)
def manage_save(
    request: Request,
    token: str,
    neighborhoods: list = Form(..., alias="neighborhoods"),
    db: Session = Depends(get_db),
):
    user_id = verify_manage_token(token)
    if not user_id:
        return templates.TemplateResponse("manage.html", {
            "request": request,
            "expired": True,
            "user": None,
            "current_neighborhoods": [],
            "all_neighborhoods": [],
            "token": token,
            "success": None,
        })

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        return RedirectResponse(url="/signup", status_code=303)

    # Remove old subscriptions
    for sub in user.subscriptions:
        db.delete(sub)
    db.flush()

    # Add new subscriptions
    for i, hood in enumerate(neighborhoods):
        db.add(Subscription(
            user_id=user.id,
            neighborhood=hood,
            is_primary=(i == 0),
        ))

    user.last_active_at = datetime.now(timezone.utc)
    db.commit()

    current = [s.neighborhood for s in user.subscriptions]

    return templates.TemplateResponse("manage.html", {
        "request": request,
        "expired": False,
        "user": user,
        "current_neighborhoods": current,
        "all_neighborhoods": get_all_neighborhoods(),
        "token": token,
        "success": "Subscriptions updated successfully.",
    })
