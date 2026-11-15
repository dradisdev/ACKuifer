"""Entry point for the Laserfiche scraper — run via Railway cron or CLI.

Usage:
    # Production (Railway cron)
    python3 -m app.scrapers.run_laserfiche

    # Test against Map 21 only, visible browser
    python3 -m app.scrapers.run_laserfiche --map 21 --no-headless

    # Dry-run style: filter to a single map
    python3 -m app.scrapers.run_laserfiche --map 42

    # D-3b one-off catch-up run (or set CATCHUP_RUN=1 on the cron service)
    python3 -m app.scrapers.run_laserfiche --catchup
"""

import argparse
import logging
import sys

from app.config import settings
from app.scrapers.laserfiche import run_laserfiche_scraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("run_laserfiche")


def main():
    parser = argparse.ArgumentParser(description="Run the Laserfiche PFAS scraper")
    parser.add_argument(
        "--map",
        dest="map_filter",
        default=None,
        help="Only scrape this map number (e.g. '21'). Omit for full-island.",
    )
    parser.add_argument(
        "--no-headless",
        action="store_true",
        help="Run browser with visible UI (for debugging).",
    )
    parser.add_argument(
        "--catchup",
        action="store_true",
        help="D-3b one-off catch-up run: label every new result as an older report "
             "(same as CATCHUP_RUN=1).",
    )
    args = parser.parse_args()

    headless = not args.no_headless
    catchup = args.catchup or settings.catchup_run
    logger.info(
        "Starting Laserfiche scraper (headless=%s, map_filter=%s, catchup=%s)",
        headless,
        args.map_filter or "all",
        catchup,
    )

    result = run_laserfiche_scraper(
        headless=headless, map_filter=args.map_filter, catchup=catchup
    )

    logger.info("Scraper finished: %s", result["status"])
    logger.info(
        "  New docs found: %d | Parsed: %d | Errors: %d | Skipped (seen): %d",
        result["new_docs_found"],
        result["new_docs_parsed"],
        result["parse_errors"],
        result["skipped_seen"],
    )

    if result["errors"]:
        logger.warning("Parse errors:")
        for err in result["errors"]:
            logger.warning("  doc_id=%s: %s", err["doc_id"], err["error"])

    # Exit with non-zero if scraper run itself failed
    if result["status"] == "error":
        sys.exit(1)


if __name__ == "__main__":
    main()
