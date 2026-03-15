"""Entry point for the Laserfiche scraper — run via Railway cron or CLI.

Usage:
    # Production (Railway cron)
    python3 -m app.scrapers.run_laserfiche

    # Test against Map 21 only, visible browser
    python3 -m app.scrapers.run_laserfiche --map 21 --no-headless

    # Dry-run style: filter to a single map
    python3 -m app.scrapers.run_laserfiche --map 42
"""

import argparse
import logging
import sys

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
    args = parser.parse_args()

    headless = not args.no_headless
    logger.info(
        "Starting Laserfiche scraper (headless=%s, map_filter=%s)",
        headless,
        args.map_filter or "all",
    )

    result = run_laserfiche_scraper(headless=headless, map_filter=args.map_filter)

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
