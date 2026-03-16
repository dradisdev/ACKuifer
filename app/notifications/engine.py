"""Notification engine — orchestrates email digests and SMS alerts after scraper runs."""

import logging
from collections import defaultdict

from sqlalchemy.orm import Session

from app.config import SMS_THRESHOLD
from app.models.results import PfasResult, SourceDiscoveryResult
from app.models.scraper import ScrapeRun
from app.models.users import Subscription, User
from app.notifications.email import send_neighborhood_digest
from app.notifications.sms import send_sms_alert

logger = logging.getLogger(__name__)


def run_notifications(scrape_run_id: str, db: Session) -> dict:
    """Run all notifications for a completed scrape run.

    Queries un-notified results, groups by neighborhood, sends digests and SMS.

    Args:
        scrape_run_id: UUID of the completed ScrapeRun.
        db: Database session.

    Returns:
        Summary dict with counts.
    """
    # Verify the scrape run completed successfully
    run = db.query(ScrapeRun).filter(ScrapeRun.id == scrape_run_id).first()
    if not run:
        logger.error("Scrape run %s not found", scrape_run_id)
        return {"status": "error", "reason": "scrape_run_not_found"}

    if run.status != "success":
        logger.info(
            "Scrape run %s status is '%s' — skipping notifications",
            scrape_run_id, run.status,
        )
        return {"status": "skipped", "reason": f"run_status_{run.status}"}

    source = run.source  # 'laserfiche' or 'massdep'

    # Query un-notified results from this source
    lf_results = []
    sd_results = []

    if source == "laserfiche":
        lf_results = (
            db.query(PfasResult)
            .filter(PfasResult.notified_at.is_(None))
            .all()
        )
    elif source == "massdep":
        sd_results = (
            db.query(SourceDiscoveryResult)
            .filter(
                SourceDiscoveryResult.notified_at.is_(None),
                SourceDiscoveryResult.latitude.isnot(None),
                SourceDiscoveryResult.longitude.isnot(None),
                SourceDiscoveryResult.geocode_review_needed == False,
            )
            .all()
        )

    total_results = len(lf_results) + len(sd_results)
    if total_results == 0:
        logger.info("No un-notified results for %s — nothing to send", source)
        return {"status": "ok", "results": 0, "emails": 0, "sms_sent": 0, "sms_skipped": 0}

    # Group results by neighborhood
    lf_by_hood = defaultdict(list)
    sd_by_hood = defaultdict(list)

    for r in lf_results:
        hood = r.neighborhood or "Nantucket (Island-wide)"
        lf_by_hood[hood].append(r)

    for r in sd_results:
        hood = r.neighborhood or "Nantucket (Island-wide)"
        sd_by_hood[hood].append(r)

    all_neighborhoods = set(lf_by_hood.keys()) | set(sd_by_hood.keys())

    # Send digests per neighborhood
    total_emails = 0
    for hood in sorted(all_neighborhoods):
        hood_lf = lf_by_hood.get(hood, [])
        hood_sd = sd_by_hood.get(hood, [])
        sent = send_neighborhood_digest(hood, hood_lf, hood_sd, db)
        total_emails += sent

    # Send SMS alerts for results at or above SMS_THRESHOLD
    sms_sent = 0
    sms_skipped = 0

    sms_candidates = []
    for r in lf_results:
        pfas6 = float(r.pfas6_sum) if r.pfas6_sum is not None else 0.0
        if pfas6 >= SMS_THRESHOLD:
            sms_candidates.append((r, "laserfiche"))
    for r in sd_results:
        pfas6 = float(r.pfas6_sum) if r.pfas6_sum is not None else 0.0
        if pfas6 >= SMS_THRESHOLD:
            sms_candidates.append((r, "massdep"))

    if sms_candidates:
        # Get all users with mobile numbers subscribed to affected neighborhoods
        sms_neighborhoods = set()
        for r, src in sms_candidates:
            sms_neighborhoods.add(r.neighborhood or "Nantucket (Island-wide)")

        sms_users = (
            db.query(User)
            .join(Subscription)
            .filter(
                Subscription.neighborhood.in_(sms_neighborhoods),
                User.unsubscribed_at.is_(None),
                User.mobile.isnot(None),
                User.mobile != "",
            )
            .all()
        )

        # Build user->neighborhoods lookup
        user_hoods = defaultdict(set)
        for user in sms_users:
            for sub in user.subscriptions:
                if sub.neighborhood in sms_neighborhoods:
                    user_hoods[user.id].add(sub.neighborhood)

        for result, src in sms_candidates:
            result_hood = result.neighborhood or "Nantucket (Island-wide)"
            for user in sms_users:
                if result_hood in user_hoods.get(user.id, set()):
                    if send_sms_alert(user, result, src):
                        sms_sent += 1
                    else:
                        sms_skipped += 1

    summary = {
        "status": "ok",
        "source": source,
        "results": total_results,
        "neighborhoods": len(all_neighborhoods),
        "emails": total_emails,
        "sms_sent": sms_sent,
        "sms_skipped": sms_skipped,
    }

    logger.info(
        "Notification summary for %s run %s: %d results, %d neighborhoods, "
        "%d emails, %d SMS sent, %d SMS skipped",
        source, scrape_run_id, total_results, len(all_neighborhoods),
        total_emails, sms_sent, sms_skipped,
    )

    return summary
