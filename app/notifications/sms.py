"""SMS alerts — auto-activates when Twilio credentials are configured.

Sends a per-result SMS for every result with pfas6_sum >= SMS_THRESHOLD (16 ppt).
If Twilio credentials are not set, logs the would-be message and returns gracefully.
"""

import logging

from app.config import MCL, SMS_THRESHOLD, settings
from app.routers.api import _clean_sd_street_name

logger = logging.getLogger(__name__)


def _is_twilio_configured() -> bool:
    """Check if Twilio credentials are present and not placeholder values."""
    return bool(
        settings.twilio_account_sid
        and settings.twilio_auth_token
        and settings.twilio_from_number
        and settings.twilio_account_sid != "not_configured"
        and settings.twilio_auth_token != "not_configured"
        and settings.twilio_from_number != "not_configured"
    )


def _build_sms_message(neighborhood: str, pfas6_sum: float, street_name: str,
                        sample_date, source_doc_url: str) -> str:
    """Build SMS message per PRD Section 6.3 (fit within 160 chars).

    Format: 'ACKuifer: PFAS detect in [Neighborhood] -- [X.X] ppt on [Street Name].
             Sample date [MM/DD/YY]. See: [short URL]'
    Prepend 'ALERT: ' if pfas6_sum >= MCL (20.0 ppt).
    """
    prefix = "ALERT: " if pfas6_sum >= MCL else ""
    date_str = sample_date.strftime("%m/%d/%y") if sample_date else "N/A"
    url_part = f" See: {source_doc_url}" if source_doc_url else ""

    msg = (
        f"{prefix}ACKuifer: PFAS detect in {neighborhood} -- "
        f"{pfas6_sum:.1f} ppt on {street_name}. "
        f"Sample date {date_str}.{url_part}"
    )

    # Truncate to 160 chars if needed
    if len(msg) > 160:
        msg = msg[:157] + "..."

    return msg


def send_sms_alert(user, result, source: str) -> bool:
    """Send an SMS alert for a single result to a single user.

    Args:
        user: User model instance (must have .mobile).
        result: PfasResult or SourceDiscoveryResult instance.
        source: 'laserfiche' or 'massdep'.

    Returns:
        True if SMS was sent (or would have been sent), False on error.
    """
    if not user.mobile:
        return False

    pfas6 = float(result.pfas6_sum) if result.pfas6_sum is not None else 0.0
    if pfas6 < SMS_THRESHOLD:
        return False

    neighborhood = result.neighborhood or "Nantucket"

    if source == "laserfiche":
        street = result.street_name or "Unknown location"
        doc_url = (
            f"https://portal.laserfiche.com/Portal/DocView.aspx"
            f"?id={result.laserfiche_doc_id}&repo=r-ec7bdbfe"
        )
    else:
        street = _clean_sd_street_name(result.sample_location)
        doc_url = result.source_doc_url.split("#")[0] if result.source_doc_url else None

    message = _build_sms_message(neighborhood, pfas6, street, result.sample_date, doc_url)

    if not _is_twilio_configured():
        logger.info(
            "SMS stub (Twilio not configured) — would send to %s: %s",
            user.mobile, message,
        )
        return True

    # Twilio is configured — send real SMS
    try:
        from twilio.rest import Client

        client = Client(settings.twilio_account_sid, settings.twilio_auth_token)
        client.messages.create(
            body=message,
            from_=settings.twilio_from_number,
            to=user.mobile,
        )
        logger.info("SMS sent to %s for %s", user.mobile, neighborhood)
        return True
    except Exception:
        logger.exception("Failed to send SMS to %s", user.mobile)
        return False
