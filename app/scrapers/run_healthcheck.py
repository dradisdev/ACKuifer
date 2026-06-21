"""Entry point for the dead man's switch health monitor — Railway cron / CLI.

Runs INDEPENDENTLY of the scrape schedule and of the web app. This is the
false-negative fix: previously the health check fired only on an /admin page
load, so a dead scraper never paged the operator unless someone happened to
open the dashboard. This entrypoint runs the same health logic on its own
schedule and sends directly via Resend, so it does not depend on the web app
being up.

Usage:
    # Production (Railway cron — its OWN daily service, separate from the scrapers)
    python3 -m app.scrapers.run_healthcheck

    # Local dry run: compute + print health, never send an email
    python3 -m app.scrapers.run_healthcheck --dry-run

Exit codes:
    0  ran successfully (whether or not an alert was sent)
    2  failed to run (DB unreachable, etc.) — a non-zero exit makes a failed
       MONITOR itself visible in Railway's cron run history.

Identity-first: logs proxy-resolved server IP, current_database, and
pfas_results / scrape_runs counts BEFORE trusting any health result, matching
diag_scrape_runs.py discipline. Read-only except for the durable dedup
timestamp in site_config (the only write).
"""

import argparse
import logging
import sys
from datetime import datetime, timezone

from sqlalchemy import text

from app.config import settings
from app.database import SessionLocal
from app.monitoring.health import (
    SOURCES,
    check_and_alert,
    compute_scraper_status,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("run_healthcheck")


def _log_identity(db) -> None:
    """Self-report DB identity before trusting any rows. Trust host + counts."""
    ident = db.execute(text(
        "SELECT inet_server_addr()::text AS server_ip, "
        "current_database() AS db, "
        "(SELECT count(*) FROM pfas_results) AS pfas_results, "
        "(SELECT count(*) FROM scrape_runs)  AS scrape_runs"
    )).mappings().one()
    logger.info("=== IDENTITY (verify before trusting health) ===")
    logger.info("  server_ip    : %s", ident["server_ip"])
    logger.info("  database     : %s", ident["db"])
    logger.info("  pfas_results : %s", ident["pfas_results"])
    logger.info("  scrape_runs  : %s", ident["scrape_runs"])


def main():
    parser = argparse.ArgumentParser(description="Run the dead man's switch health monitor")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute and print health only; never send an alert or write dedup state.",
    )
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    logger.info("Dead man's switch monitor starting (dry_run=%s, window=%dd, alerting_enabled=%s)",
                args.dry_run, settings.deadmans_window_days,
                getattr(settings, "deadmans_alerting_enabled", True))

    try:
        with SessionLocal() as db:
            _log_identity(db)

            statuses = {s: compute_scraper_status(db, s, now=now) for s in SOURCES}
            for source, st in statuses.items():
                la = st["last_success_at"]
                logger.info(
                    "  %-10s healthy=%s last_success=%s running=%s",
                    source, st["healthy"],
                    la.strftime("%Y-%m-%d %H:%M UTC") if la else "Never",
                    bool(st["currently_running"]),
                )

            if args.dry_run:
                unhealthy = [s for s, st in statuses.items() if not st["healthy"]]
                logger.info("DRY RUN — no alerts sent. Would evaluate: %s",
                            unhealthy or "none (all healthy)")
                sys.exit(0)

            report = check_and_alert(db, statuses, now=now)
            logger.info("Monitor report: %s", report)

    except SystemExit:
        raise
    except Exception:
        logger.exception("Health monitor FAILED to run")
        sys.exit(2)


if __name__ == "__main__":
    main()
