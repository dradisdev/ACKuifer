"""Entry point for the MassDEP Source Discovery scraper — run via Railway cron or CLI.

Usage:
    # Production (Railway cron)
    python3 -m app.scrapers.run_massdep

    # Visible browser for debugging
    python3 -m app.scrapers.run_massdep --no-headless

    # Force re-download and re-parse all documents
    python3 -m app.scrapers.run_massdep --force
"""

import argparse
import logging
import sys

from app.scrapers.massdep import run_massdep_scraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("run_massdep")


def main():
    parser = argparse.ArgumentParser(description="Run the MassDEP Source Discovery scraper")
    parser.add_argument(
        "--no-headless",
        action="store_true",
        help="Run browser with visible UI (for debugging).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download and re-parse all documents, ignoring seen_documents.",
    )
    args = parser.parse_args()

    headless = not args.no_headless
    logger.info(
        "Starting MassDEP scraper (headless=%s, force=%s)",
        headless,
        args.force,
    )

    result = run_massdep_scraper(headless=headless, force=args.force)

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
