"""Scraper health + dead man's switch — shared logic.

This module is the single source of truth for:
  - computing scraper health (last success within the dead man's window), and
  - deciding whether to send a dead man's switch alert, with DURABLE,
    cross-process dedup stored in site_config.

It is deliberately free of any FastAPI / web-request dependency so it can be
imported and run headless from a Railway cron entrypoint
(app.scrapers.run_healthcheck) AS WELL AS from the /admin dashboard. Both call
the SAME functions so the page-load path and the scheduled path can never
drift apart, and they dedup against the SAME durable store so they cannot
double-send (the Session-20 6/12 two-email bug).

Timezone discipline: all comparisons are done in AWARE UTC. completed_at is a
timestamptz and comes back aware, but the scraper writers use naive
datetime.utcnow(); _as_aware_utc() coerces anything we read so an aware-vs-naive
comparison can NEVER raise inside the headless cron (a TypeError there would be
a silent monitor death — the exact false-negative this work removes).
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from app.config import settings

logger = logging.getLogger(__name__)

# site_config keys for durable per-source dedup of the dead man's alert.
# Internal bookkeeping — NOT operator-editable content (see admin.save_config note).
_DEDUP_KEY_PREFIX = "deadmans_last_alert_"

# Minimum gap between alerts for the same source.
_ALERT_COOLDOWN = timedelta(hours=24)

SOURCES = ("laserfiche", "massdep")


def is_reserved_config_key(key: str) -> bool:
    """True for internal site_config keys that must not be operator-editable.

    These share the site_config table with editable content but are written
    only by the monitor's dedup logic. The admin save-config form must skip them.
    """
    return key.startswith(_DEDUP_KEY_PREFIX)


def _as_aware_utc(dt: Optional[datetime]) -> Optional[datetime]:
    """Coerce a possibly-naive datetime to aware UTC. None passes through.

    The scraper writes naive datetime.utcnow() into timestamptz columns; depending
    on driver/session config a value may come back naive. Treat naive as UTC so
    comparisons with datetime.now(timezone.utc) never raise.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def compute_scraper_status(db, source: str, now: Optional[datetime] = None) -> dict:
    """Return health snapshot for one scraper source.

    Pure read. Mirrors what the admin dashboard needs: last_success, last_run,
    currently_running, and a healthy bool (last success within the window).
    """
    # Imported here to keep this module importable in contexts where the model
    # package import graph might otherwise pull in heavy deps.
    from app.models.scraper import ScrapeRun

    if now is None:
        now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=settings.deadmans_window_days)

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

    last_success_at = _as_aware_utc(last_success.completed_at) if last_success else None
    healthy = bool(last_success_at and last_success_at >= cutoff)

    return {
        "last_success": last_success,
        "last_run": last_run,
        "currently_running": currently_running,
        "healthy": healthy,
        "last_success_at": last_success_at,  # aware UTC, convenience for callers
    }


def _get_last_alert(db, source: str) -> Optional[datetime]:
    """Read the durable last-alert timestamp for a source from site_config."""
    from app.models.site_config import SiteConfig

    row = (
        db.query(SiteConfig)
        .filter(SiteConfig.key == _DEDUP_KEY_PREFIX + source)
        .first()
    )
    if not row or not row.value:
        return None
    try:
        return _as_aware_utc(datetime.fromisoformat(row.value))
    except (ValueError, TypeError):
        # Corrupt/legacy value: treat as "no record" rather than crashing the monitor.
        logger.warning("Unparseable deadmans dedup value for %s: %r", source, row.value)
        return None


def _set_last_alert(db, source: str, when: datetime) -> None:
    """Persist the last-alert timestamp durably (upsert into site_config)."""
    from app.models.site_config import SiteConfig

    key = _DEDUP_KEY_PREFIX + source
    iso = _as_aware_utc(when).isoformat()
    row = db.query(SiteConfig).filter(SiteConfig.key == key).first()
    if row:
        row.value = iso
        row.updated_at = when
    else:
        db.add(SiteConfig(key=key, value=iso, updated_at=when))
    db.commit()


def check_and_alert(db, statuses: dict, now: Optional[datetime] = None) -> dict:
    """Send dead man's switch alerts for any unhealthy source, with durable dedup.

    `statuses` maps source -> status dict from compute_scraper_status().
    Returns a small report dict for logging by the caller.

    Dedup is read-modify-write against site_config, so the web dashboard and the
    scheduled cron share one cooldown and cannot double-send across processes or
    survive-a-deploy resets.

    Honors settings.deadmans_alerting_enabled (default True). When False (e.g.
    staging, set via Railway variable), no alerts are sent — but health is still
    computed and returned, so the monitor stays observable.
    """
    if now is None:
        now = datetime.now(timezone.utc)

    report = {"checked": [], "alerted": [], "skipped_healthy": [],
              "skipped_cooldown": [], "skipped_disabled": [], "errors": []}

    alerting_enabled = getattr(settings, "deadmans_alerting_enabled", True)

    if not settings.operator_email:
        report["errors"].append("no operator_email configured")
        return report

    for source in SOURCES:
        status = statuses.get(source)
        if status is None:
            continue
        report["checked"].append(source)

        if status["healthy"]:
            report["skipped_healthy"].append(source)
            continue

        if not alerting_enabled:
            report["skipped_disabled"].append(source)
            continue

        last_sent = _get_last_alert(db, source)
        if last_sent and (now - last_sent) < _ALERT_COOLDOWN:
            report["skipped_cooldown"].append(source)
            continue

        last_success = status["last_success"]
        last_run_at = last_success.completed_at if last_success else None

        try:
            from app.notifications.email import send_deadmans_alert
            send_deadmans_alert(source, last_run_at)
            _set_last_alert(db, source, now)
            report["alerted"].append(source)
        except Exception:
            logger.exception("Failed to send dead man's switch alert for %s", source)
            report["errors"].append(f"send failed for {source}")

    return report
