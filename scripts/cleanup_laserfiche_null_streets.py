#!/usr/bin/env python3
"""Delete Laserfiche PFAS results with NULL street_name so they re-parse.

The DW Program regex was broadened in a recent commit. Any existing
pfas_results row with street_name=NULL was parsed with the old (broken)
regex. Deleting these rows + their seen_documents entries causes the
next Laserfiche scraper run to re-ingest them with the new logic.

Usage:
    DATABASE_URL=postgresql://... python scripts/cleanup_laserfiche_null_streets.py
    DATABASE_URL=postgresql://... python scripts/cleanup_laserfiche_null_streets.py --confirm

Without --confirm: dry-run only. Prints counts and a sample of rows.
With --confirm: performs the deletes inside a single transaction.
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

    conn = psycopg2.connect(db_url)
    conn.autocommit = False

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # 1. Count candidates
            cur.execute("""
                SELECT COUNT(*) AS n
                FROM pfas_results
                WHERE street_name IS NULL
            """)
            null_count = cur.fetchone()["n"]

            cur.execute("""
                SELECT COUNT(*) AS n
                FROM seen_documents sd
                WHERE sd.source = 'laserfiche'
                  AND sd.doc_key IN (
                    SELECT laserfiche_doc_id::text
                    FROM pfas_results
                    WHERE street_name IS NULL
                  )
            """)
            seen_count = cur.fetchone()["n"]

            print(f"Found {null_count} pfas_results rows with street_name=NULL")
            print(f"Found {seen_count} matching seen_documents rows")

            if null_count == 0:
                print("Nothing to clean up. Exiting.")
                return

            # 2. Print a sample for visual verification
            cur.execute("""
                SELECT
                    laserfiche_doc_id,
                    map_number,
                    parcel_number,
                    neighborhood,
                    sample_date,
                    pfas6_sum,
                    result_status
                FROM pfas_results
                WHERE street_name IS NULL
                ORDER BY sample_date DESC NULLS LAST
                LIMIT 10
            """)
            print("\nSample rows (up to 10 most recent):")
            print(f"{'doc_id':>8}  {'map':>4}  {'parcel':>8}  "
                  f"{'neighborhood':<20}  {'date':<12}  {'pfas6':>7}  status")
            print("-" * 90)
            for row in cur.fetchall():
                print(
                    f"{row['laserfiche_doc_id']:>8}  "
                    f"{(row['map_number'] or '—'):>4}  "
                    f"{(row['parcel_number'] or '—'):>8}  "
                    f"{(row['neighborhood'] or '—'):<20}  "
                    f"{(str(row['sample_date']) if row['sample_date'] else '—'):<12}  "
                    f"{(f'{float(row[\"pfas6_sum\"]):.1f}' if row['pfas6_sum'] is not None else '—'):>7}  "
                    f"{row['result_status'] or '—'}"
                )

            # 3. Dry-run or commit
            if not args.confirm:
                print("\n[DRY RUN] No changes made. Re-run with --confirm to delete.")
                return

            print("\n--confirm supplied. Deleting...")

            cur.execute("""
                DELETE FROM seen_documents
                WHERE source = 'laserfiche'
                  AND doc_key IN (
                    SELECT laserfiche_doc_id::text
                    FROM pfas_results
                    WHERE street_name IS NULL
                  )
            """)
            seen_deleted = cur.rowcount

            cur.execute("""
                DELETE FROM pfas_results
                WHERE street_name IS NULL
            """)
            results_deleted = cur.rowcount

            conn.commit()
            print(f"Deleted {results_deleted} pfas_results rows")
            print(f"Deleted {seen_deleted} seen_documents rows")
            print("Next Laserfiche scraper run will re-ingest these documents.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
