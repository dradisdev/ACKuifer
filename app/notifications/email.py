"""Email notifications — neighborhood digest and reconfirmation emails."""

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import resend
from sqlalchemy.orm import Session

from app.config import MCL, RETEST_WINDOW_DAYS, SMS_THRESHOLD, settings
from app.models.results import PfasResult, SourceDiscoveryResult
from app.models.users import Subscription, User

logger = logging.getLogger(__name__)

# Reuse the street-name cleaning logic from api.py for Source Discovery display
from app.routers.api import _clean_sd_street_name


def _format_date(d) -> str:
    """Format a date for email display."""
    if not d:
        return "Unknown"
    return d.strftime("%b %d, %Y")


def _format_value(pfas6_sum) -> str:
    """Format PFAS6 value for email display."""
    if pfas6_sum is None or pfas6_sum == 0:
        return "Non-detect"
    return f"{float(pfas6_sum):.1f} ppt"


def _result_to_email_row(result, source: str) -> dict:
    """Normalize a PfasResult or SourceDiscoveryResult into a dict for the email template."""
    if source == "laserfiche":
        street = result.street_name or "Unknown location"
        doc_url = (
            f"https://portal.laserfiche.com/Portal/DocView.aspx"
            f"?id={result.laserfiche_doc_id}&repo=r-ec7bdbfe"
        )
        j_qualified = bool(result.j_qualifier_present)
    else:
        street = _clean_sd_street_name(result.sample_location)
        doc_url = result.source_doc_url.split("#")[0] if result.source_doc_url else None
        j_qualified = False

    pfas6 = float(result.pfas6_sum) if result.pfas6_sum is not None else 0.0

    return {
        "street_name": street,
        "pfas6_sum": pfas6,
        "value_display": _format_value(result.pfas6_sum),
        "sample_date": _format_date(result.sample_date),
        "sample_date_raw": result.sample_date,
        "source_doc_url": doc_url,
        "result_status": result.result_status,
        "j_qualified": j_qualified,
        "above_sms_threshold": pfas6 >= SMS_THRESHOLD,
        "above_mcl": pfas6 > MCL,
        "source_label": "Board of Health" if source == "laserfiche" else "MassDEP Source Discovery",
        "retest": False,
        "retest_arrow": "",
        "retest_prev_value": "",
    }


def _annotate_retests(rows: list) -> None:
    """Mark rows that are retests of a previous result at the same street.

    Only applies to Board of Health (Laserfiche) results. Source Discovery
    results are never flagged as retests.

    Groups by street_name. Within each group, sorts by date ascending.
    If two consecutive results are within RETEST_WINDOW_DAYS, the newer
    one is marked as a retest with a directional arrow.
    """
    # Filter to Laserfiche rows only
    lf_rows = [r for r in rows if r["source_label"] == "Board of Health"]
    groups = defaultdict(list)
    for r in lf_rows:
        groups[r["street_name"]].append(r)

    for group_rows in groups.values():
        if len(group_rows) < 2:
            continue

        # Sort by date ascending (oldest first)
        group_rows.sort(key=lambda r: r["sample_date_raw"] or datetime.min.date())

        for i in range(1, len(group_rows)):
            prev = group_rows[i - 1]
            curr = group_rows[i]

            if not prev["sample_date_raw"] or not curr["sample_date_raw"]:
                continue

            days_apart = (curr["sample_date_raw"] - prev["sample_date_raw"]).days
            if days_apart > RETEST_WINDOW_DAYS:
                continue

            # This is a retest — determine direction
            prev_val = prev["pfas6_sum"]
            curr_val = curr["pfas6_sum"]

            # Within 10% = essentially unchanged
            if prev_val == 0 and curr_val == 0:
                arrow = "="
            elif prev_val == 0:
                arrow = "\u2191"  # ↑ up from zero
            elif abs(curr_val - prev_val) / prev_val <= 0.10:
                arrow = "="
            elif curr_val > prev_val:
                arrow = "\u2191"  # ↑
            else:
                arrow = "\u2193"  # ↓

            curr["retest"] = True
            curr["retest_arrow"] = arrow
            curr["retest_prev_value"] = _format_value(prev_val if prev_val else None)


def _sort_with_retest_groups(rows: list) -> list:
    """Sort rows newest-first, keeping retest groups together.

    Retest groups (LF results at the same street within RETEST_WINDOW_DAYS)
    are placed by their most recent result's date. Within a group, newest first.
    Non-grouped results are interleaved by date.
    """
    from datetime import date as date_type

    min_date = date_type.min

    # Identify retest groups: a group is a street_name where any row has retest=True
    retest_streets = set()
    for r in rows:
        if r["retest"]:
            retest_streets.add(r["street_name"])

    # Build groups for retest streets, leave others as singletons
    groups = []  # list of (sort_key_date, [rows newest-first])
    grouped_indices = set()

    for street in retest_streets:
        group = [r for r in rows if r["street_name"] == street and r["source_label"] == "Board of Health"]
        if not group:
            continue
        # Sort newest first within group
        group.sort(key=lambda r: r["sample_date_raw"] or min_date, reverse=True)
        anchor_date = group[0]["sample_date_raw"] or min_date
        groups.append((anchor_date, group))
        for r in group:
            grouped_indices.add(id(r))

    # Add ungrouped rows as singletons
    for r in rows:
        if id(r) not in grouped_indices:
            groups.append((r["sample_date_raw"] or min_date, [r]))

    # Sort all groups newest-first by anchor date
    groups.sort(key=lambda g: g[0], reverse=True)

    # Flatten
    result = []
    for _, group in groups:
        result.extend(group)
    return result


def _build_digest_html(neighborhood: str, rows: list, unsubscribe_url: str, manage_url: str) -> str:
    """Build the HTML digest email body."""
    _annotate_retests(rows)

    detections = [r for r in rows if r["result_status"] != "NON-DETECT"]
    non_detects = [r for r in rows if r["result_status"] == "NON-DETECT"]

    # Sort detections newest-first, but keep retest groups together.
    # Group by street_name for LF results that have retests, then sort
    # each group newest-first internally, and sort groups by most recent date.
    detections = _sort_with_retest_groups(detections)
    non_detects = _sort_with_retest_groups(non_detects)

    any_j_qualified = any(r["j_qualified"] for r in rows)

    # Build result rows HTML
    def _row_html(r: dict) -> str:
        highlight = r["above_sms_threshold"]
        bg = "#FFF5F5" if highlight else "#FFFFFF"
        bold = "font-weight:700;" if highlight else ""
        value_color = "color:#E53E3E;" if r["above_mcl"] else ""
        mcl_badge = ' <span style="color:#E53E3E;font-weight:700;font-size:12px;">Above MCL</span>' if r["above_mcl"] else ""
        doc_link = f'<a href="{r["source_doc_url"]}" style="color:#2B6CB0;text-decoration:none;">View report</a>' if r["source_doc_url"] else "\u2014"

        # Retest indicator
        retest_badge = ""
        if r["retest"]:
            arrow = r["retest_arrow"]
            prev = r["retest_prev_value"]
            if arrow == "\u2191":
                arrow_color = "#E53E3E"  # red for increase
            elif arrow == "\u2193":
                arrow_color = "#38A169"  # green for decrease
            else:
                arrow_color = "#718096"  # gray for unchanged
            retest_badge = (
                f' <span style="font-size:11px;color:{arrow_color};font-weight:600;">'
                f'Retest {arrow} from {prev}</span>'
            )

        street_display = f'{r["street_name"]}{retest_badge}'

        return f"""<tr style="background:{bg};">
  <td style="padding:10px 12px;border-bottom:1px solid #EDF2F7;{bold}">{street_display}</td>
  <td style="padding:10px 12px;border-bottom:1px solid #EDF2F7;{bold}{value_color}">{r["value_display"]}{mcl_badge}</td>
  <td style="padding:10px 12px;border-bottom:1px solid #EDF2F7;">{r["sample_date"]}</td>
  <td style="padding:10px 12px;border-bottom:1px solid #EDF2F7;">{r["source_label"]}</td>
  <td style="padding:10px 12px;border-bottom:1px solid #EDF2F7;">{doc_link}</td>
</tr>"""

    detection_rows = "\n".join(_row_html(r) for r in detections)
    non_detect_rows = "\n".join(_row_html(r) for r in non_detects)

    detection_section = ""
    if detections:
        detection_section = f"""
<h3 style="color:#1A3A5C;font-size:16px;margin:20px 0 8px 0;">Detections ({len(detections)})</h3>
<table style="width:100%;border-collapse:collapse;font-size:14px;">
  <thead>
    <tr style="background:#F7FAFC;">
      <th style="padding:8px 12px;text-align:left;border-bottom:2px solid #E2E8F0;color:#4A5568;">Street</th>
      <th style="padding:8px 12px;text-align:left;border-bottom:2px solid #E2E8F0;color:#4A5568;">PFAS6</th>
      <th style="padding:8px 12px;text-align:left;border-bottom:2px solid #E2E8F0;color:#4A5568;">Date</th>
      <th style="padding:8px 12px;text-align:left;border-bottom:2px solid #E2E8F0;color:#4A5568;">Source</th>
      <th style="padding:8px 12px;text-align:left;border-bottom:2px solid #E2E8F0;color:#4A5568;">Report</th>
    </tr>
  </thead>
  <tbody>
    {detection_rows}
  </tbody>
</table>"""

    non_detect_section = ""
    if non_detects:
        non_detect_section = f"""
<h3 style="color:#1A3A5C;font-size:16px;margin:20px 0 8px 0;">Non-detects ({len(non_detects)})</h3>
<table style="width:100%;border-collapse:collapse;font-size:14px;">
  <thead>
    <tr style="background:#F7FAFC;">
      <th style="padding:8px 12px;text-align:left;border-bottom:2px solid #E2E8F0;color:#4A5568;">Street</th>
      <th style="padding:8px 12px;text-align:left;border-bottom:2px solid #E2E8F0;color:#4A5568;">PFAS6</th>
      <th style="padding:8px 12px;text-align:left;border-bottom:2px solid #E2E8F0;color:#4A5568;">Date</th>
      <th style="padding:8px 12px;text-align:left;border-bottom:2px solid #E2E8F0;color:#4A5568;">Source</th>
      <th style="padding:8px 12px;text-align:left;border-bottom:2px solid #E2E8F0;color:#4A5568;">Report</th>
    </tr>
  </thead>
  <tbody>
    {non_detect_rows}
  </tbody>
</table>"""

    j_footnote = ""
    if any_j_qualified:
        j_footnote = """
<p style="font-size:12px;color:#718096;margin-top:16px;font-style:italic;">
  * One or more values are J-qualified (estimated). See source report for details.
</p>"""

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#F7FAFC;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
<div style="max-width:640px;margin:0 auto;padding:20px;">

  <div style="background:#1A3A5C;padding:16px 20px;border-radius:8px 8px 0 0;">
    <h1 style="color:#FFFFFF;font-size:18px;margin:0;">
      <a href="https://ackuifer.org" style="color:#FFFFFF;text-decoration:none;">ACKuifer</a>
    </h1>
  </div>

  <div style="background:#FFFFFF;padding:20px 20px 24px 20px;border-radius:0 0 8px 8px;border:1px solid #E2E8F0;border-top:none;">
    <h2 style="color:#1A3A5C;font-size:18px;margin:0 0 4px 0;">
      {len(rows)} new PFAS result{"s" if len(rows) != 1 else ""} in {neighborhood}
    </h2>
    <p style="color:#718096;font-size:14px;margin:0 0 16px 0;">
      New well test results have been posted for your subscribed neighborhood.
    </p>

    {detection_section}
    {non_detect_section}
    {j_footnote}

    <div style="margin-top:24px;padding-top:16px;border-top:1px solid #EDF2F7;">
      <p style="font-size:13px;color:#718096;margin:0;">
        <a href="https://ackuifer.org/map" style="color:#2B6CB0;text-decoration:none;">View map</a> &middot;
        <a href="{manage_url}" style="color:#2B6CB0;text-decoration:none;">Manage subscriptions</a> &middot;
        <a href="{unsubscribe_url}" style="color:#2B6CB0;text-decoration:none;">Unsubscribe</a>
      </p>
      <p style="font-size:12px;color:#A0AEC0;margin:8px 0 0 0;">
        ACKuifer is an independent public-interest service. Your email is never shared.
        <a href="https://ackuifer.org" style="color:#A0AEC0;">ackuifer.org</a>
      </p>
    </div>
  </div>

</div>
</body>
</html>"""


def _build_reconfirmation_html(user_email: str, reconfirm_url: str, unsubscribe_url: str) -> str:
    """Build the HTML for an inactivity re-confirmation email."""
    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#F7FAFC;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
<div style="max-width:640px;margin:0 auto;padding:20px;">

  <div style="background:#1A3A5C;padding:16px 20px;border-radius:8px 8px 0 0;">
    <h1 style="color:#FFFFFF;font-size:18px;margin:0;">
      <a href="https://ackuifer.org" style="color:#FFFFFF;text-decoration:none;">ACKuifer</a>
    </h1>
  </div>

  <div style="background:#FFFFFF;padding:20px 20px 24px 20px;border-radius:0 0 8px 8px;border:1px solid #E2E8F0;border-top:none;">
    <h2 style="color:#1A3A5C;font-size:18px;margin:0 0 12px 0;">Still want PFAS alerts?</h2>
    <p style="color:#4A5568;font-size:14px;line-height:1.6;margin:0 0 16px 0;">
      It's been a while since you've interacted with your ACKuifer alerts.
      To keep your subscription active, please confirm below. If we don't
      hear from you within 30 days, your subscription will be automatically cancelled.
    </p>

    <div style="text-align:center;margin:24px 0;">
      <a href="{reconfirm_url}" style="display:inline-block;background:#2B6CB0;color:#FFFFFF;padding:12px 32px;border-radius:6px;text-decoration:none;font-weight:600;font-size:14px;">
        Keep my subscription active
      </a>
    </div>

    <div style="margin-top:24px;padding-top:16px;border-top:1px solid #EDF2F7;">
      <p style="font-size:13px;color:#718096;margin:0;">
        <a href="{unsubscribe_url}" style="color:#2B6CB0;text-decoration:none;">Unsubscribe instead</a>
      </p>
      <p style="font-size:12px;color:#A0AEC0;margin:8px 0 0 0;">
        ACKuifer is an independent public-interest service. Your email is never shared.
        <a href="https://ackuifer.org" style="color:#A0AEC0;">ackuifer.org</a>
      </p>
    </div>
  </div>

</div>
</body>
</html>"""


def send_neighborhood_digest(
    neighborhood: str,
    new_lf_results: list,
    new_sd_results: list,
    db: Session,
) -> int:
    """Send a digest email to all subscribers of a neighborhood.

    Args:
        neighborhood: Neighborhood name.
        new_lf_results: List of new PfasResult objects.
        new_sd_results: List of new SourceDiscoveryResult objects.
        db: Database session.

    Returns:
        Number of emails sent.
    """
    if not settings.resend_api_key:
        logger.warning("RESEND_API_KEY not configured — skipping email digest")
        return 0

    resend.api_key = settings.resend_api_key

    # Build normalized rows for template
    rows = []
    for r in new_lf_results:
        rows.append(_result_to_email_row(r, "laserfiche"))
    for r in new_sd_results:
        rows.append(_result_to_email_row(r, "massdep"))

    if not rows:
        return 0

    # Query active subscribers for this neighborhood
    subscribers = (
        db.query(User)
        .join(Subscription)
        .filter(
            Subscription.neighborhood == neighborhood,
            User.unsubscribed_at.is_(None),
        )
        .all()
    )

    if not subscribers:
        logger.info("No subscribers for %s — skipping digest", neighborhood)
        return 0

    count = len(rows)
    subject = f"ACKuifer: {count} new PFAS result{'s' if count != 1 else ''} in {neighborhood}"

    sent = 0
    for user in subscribers:
        # Import here to avoid circular imports at module level
        from app.notifications.tokens import generate_manage_token, generate_unsubscribe_token

        unsub_token = generate_unsubscribe_token(str(user.id))
        manage_token = generate_manage_token(str(user.id))
        unsubscribe_url = f"https://ackuifer.org/unsubscribe?token={unsub_token}"
        manage_url = f"https://ackuifer.org/manage?token={manage_token}"

        html = _build_digest_html(neighborhood, rows, unsubscribe_url, manage_url)

        try:
            resend.Emails.send({
                "from": settings.email_from,
                "to": [user.email],
                "subject": subject,
                "html": html,
            })
            sent += 1
            logger.info("Digest email sent to %s for %s", user.email, neighborhood)
        except Exception:
            logger.exception("Failed to send digest to %s", user.email)

    # Mark results as notified
    now = datetime.now(timezone.utc)
    for r in new_lf_results:
        r.notified_at = now
    for r in new_sd_results:
        r.notified_at = now
    db.commit()

    return sent


def send_reconfirmation_email(user: User, db: Session) -> bool:
    """Send an inactivity re-confirmation email.

    Returns True if sent successfully.
    """
    if not settings.resend_api_key:
        logger.warning("RESEND_API_KEY not configured — skipping reconfirmation email")
        return False

    resend.api_key = settings.resend_api_key

    from app.notifications.tokens import generate_manage_token, generate_unsubscribe_token

    unsub_token = generate_unsubscribe_token(str(user.id))
    manage_token = generate_manage_token(str(user.id))
    unsubscribe_url = f"https://ackuifer.org/unsubscribe?token={unsub_token}"
    reconfirm_url = f"https://ackuifer.org/reconfirm?token={manage_token}"

    html = _build_reconfirmation_html(user.email, reconfirm_url, unsubscribe_url)

    try:
        resend.Emails.send({
            "from": settings.email_from,
            "to": [user.email],
            "subject": "ACKuifer: Are you still there?",
            "html": html,
        })
        user.reconfirm_sent_at = datetime.now(timezone.utc)
        db.commit()
        logger.info("Reconfirmation email sent to %s", user.email)
        return True
    except Exception:
        logger.exception("Failed to send reconfirmation to %s", user.email)
        return False
