#!/usr/bin/env python3
"""Delete 22 Toms Way Source Discovery records so they re-parse clean.

The E-qualifier fix (commit 9c95d5c) and merge-first fix (commit 6c8870c)
together correct the PFHxS overstatement bug confirmed for 22 Toms Way.
The DB record currently shows PFAS6=879.7 because PFHxS=664 (E-flagged,
over-range estimate) was captured instead of the re-analyzed 556.

Deleting the existing record + its seen_documents entries causes the
next MassDEP scraper run to re-ingest the source PDFs with the corrected
parsing logic, producing PFAS6 ~771.7 ppt.

Usage:
    python scripts/cleanup_toms_way_record.py
    python scripts/cleanup_toms_way_record.py --confirm

DATABASE_URL is loaded from .env automatically.
"""

import argparse
import os
import sys

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Actually perform the deletes (default is dry-run).",
    )
    args = parser.parse_args()

    load_dotenv()
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL not set", file=sys.stderr)
        sys.exit(1)

    # Match any Toms Way variant in sample_location. Case-insensitive via ILIKE.
    # Covers: "22 TOMS WAY", "22 TOM'S WAY", "22 TOM'S WAY-F",
    #         "22 TOMS WAY-F_2", "22 TOM'S WAY-F_2", etc.
    toms_way_patterns = [
        "22 TOMS WAY%",
        "22 TOM'S WAY%",
    ]

    conn = psycopg2.connect(db_url)
    conn.autocommit = False

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # 1. Find matching records
            cur.execute("""
                SELECT
                    id,
                    sample_location,
                    sample_date,
                    medium,
                    pfhxs,
                    pfas6_sum,
                    result_status,
                    source_doc_url,
                    neighborhood
                FROM source_discovery_results
                WHERE sample_location ILIKE ANY(%s)
                ORDER BY sample_date DESC NULLS LAST, sample_location
            """, (toms_way_patterns,))

            rows = cur.fetchall()
            if not rows:
                print("No 22 Toms Way records found. Nothing to clean up.")
                return

            print(f"Found {len(rows)} source_discovery_results rows:")
            print(
                f"{'id':>4}  "
                f"{'sample_location':<30}  "
                f"{'date':<12}  "
                f"{'medium':<15}  "
                f"{'pfhxs':>7}  "
                f"{'pfas6':>7}  "
                f"status"
            )
            print("-" * 110)
            doc_urls = set()
            for row in rows:
                loc_str = row["sample_location"] or "—"
                date_str = str(row["sample_date"]) if row["sample_date"] else "—"
                medium_str = row["medium"] or "—"
                pfhxs_val = row["pfhxs"]
                pfhxs_str = f"{float(pfhxs_val):.1f}" if pfhxs_val is not None else "—"
                pfas6_val = row["pfas6_sum"]
                pfas6_str = f"{float(pfas6_val):.1f}" if pfas6_val is not None else "—"
                status_str = row["result_status"] or "—"
                print(
                    f"{row['id']:>4}  "
                    f"{loc_str:<30}  "
                    f"{date_str:<12}  "
                    f"{medium_str:<15}  "
                    f"{pfhxs_str:>7}  "
                    f"{pfas6_str:>7}  "
                    f"{status_str}"
                )
                if row["source_doc_url"]:
                    # Strip our internal "#well_id" fragment to get the
                    # canonical PDF URL used in seen_documents.
                    base_url = row["source_doc_url"].split("#")[0]
                    doc_urls.add(base_url)

            # 2. Find matching seen_documents
            if doc_urls:
                cur.execute("""
                    SELECT doc_key, discovered_at, parse_status
                    FROM seen_documents
                    WHERE source = 'massdep'
                      AND doc_key = ANY(%s)
                """, (list(doc_urls),))
                seen_rows = cur.fetchall()
            else:
                seen_rows = []

            print(
                f"\nFound {len(seen_rows)} matching seen_documents rows "
                f"(from {len(doc_urls)} unique PDF URLs):"
            )
            for sd in seen_rows:
                discovered_str = str(sd["discovered_at"])[:19]
                status_val = sd["parse_status"] or "—"
                print(f"  {status_val:<8}  {discovered_str}  {sd['doc_key']}")

            # IMPORTANT: these seen_documents rows may correspond to PDFs
            # that contain OTHER sample locations too — not just Toms Way.
            # Deleting them causes the entire PDF to be re-ingested, which
            # means any OTHER records in those same PDFs also get re-parsed.
            # This is desirable (same parsing bugs would have affected them),
            # but worth noting.
            print("\nNote: the PDFs being re-ingested contain other sample")
            print("locations too. Those records will also be re-parsed with")
            print("the corrected logic (desirable side effect).")

            # 3. Dry-run or commit
            if not args.confirm:
                print("\n[DRY RUN] No changes made. Re-run with --confirm to delete.")
                return

            print("\n--confirm supplied. Deleting...")

            # Delete source_discovery_results rows
            cur.execute("""
                DELETE FROM source_discovery_results
                WHERE sample_location ILIKE ANY(%s)
            """, (toms_way_patterns,))
            sd_deleted = cur.rowcount

            # Delete corresponding seen_documents rows
            if doc_urls:
                cur.execute("""
                    DELETE FROM seen_documents
                    WHERE source = 'massdep'
                      AND doc_key = ANY(%s)
                """, (list(doc_urls),))
                seen_deleted = cur.rowcount
            else:
                seen_deleted = 0

            conn.commit()
            print(f"Deleted {sd_deleted} source_discovery_results rows")
            print(f"Deleted {seen_deleted} seen_documents rows")
            print("Next MassDEP scraper run will re-ingest these PDFs.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
