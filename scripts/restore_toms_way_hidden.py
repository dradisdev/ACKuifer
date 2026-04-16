#!/usr/bin/env python3
"""Restore the 4 deleted 22 Toms Way records, marked hidden.

Context: Earlier in this session, scripts/cleanup_toms_way_record.py
deleted 4 source_discovery_results rows (ids 155, 229, 230, 236) and 2
seen_documents entries, expecting the MassDEP scraper to re-ingest them
with corrected values via the E-qualifier + merge-first parser fixes.

We subsequently discovered (a) the MassDEP portal extractor has been
silently broken since Mar 25, and (b) the parser has additional under-
extraction bugs that would produce incorrect values even if re-ingested.

Until those bugs are fixed, restore the rows with their original values
so they don't disappear from ackuifer.org entirely. Mark them hidden=true
so the map doesn't display known-bad PFHxS values publicly. The admin
panel will still show them in the hide/unhide UI.

Usage:
    python scripts/restore_toms_way_hidden.py
    python scripts/restore_toms_way_hidden.py --confirm
"""

import argparse
import os
import sys
from decimal import Decimal

import psycopg2
from dotenv import load_dotenv


# Values captured from the dry-run output of cleanup_toms_way_record.py
# immediately before deletion. NOTE: these are the bad, inflated values.
# That's intentional — we're restoring state, not fixing values.
RECORDS = [
    {
        "id": 155,
        "sample_location": "22 TOMS WAY-R-3 (drinking_water)",
        "sample_date": "2024-03-13",
        "medium": "drinking_water",
        "pfhxs": "481.0",
        "pfas6_sum": "703.2",
        "result_status": "HAZARD",
    },
    {
        "id": 229,
        "sample_location": "22 TOM'S WAY",
        "sample_date": "2023-09-12",
        "medium": "drinking_water",
        "pfhxs": "664.0",
        "pfas6_sum": "879.7",
        "result_status": "HAZARD",
    },
    {
        "id": 230,
        "sample_location": "22 TOM'S WAY-F",
        "sample_date": "2023-09-12",
        "medium": "drinking_water",
        "pfhxs": "671.0",
        "pfas6_sum": "891.7",
        "result_status": "HAZARD",
    },
    {
        "id": 236,
        "sample_location": "22 TOMS WAY-F_2",
        "sample_date": "2023-10-05",
        "medium": "drinking_water",
        "pfhxs": "573.0",
        "pfas6_sum": "801.5",
        "result_status": "HAZARD",
    },
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Actually perform the inserts (default is dry-run).",
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
        with conn.cursor() as cur:
            # Check whether the original ids are still available
            ids = [r["id"] for r in RECORDS]
            cur.execute(
                "SELECT id FROM source_discovery_results WHERE id = ANY(%s)",
                (ids,),
            )
            existing = [row[0] for row in cur.fetchall()]
            if existing:
                print(
                    f"ERROR: these ids already exist in source_discovery_results: {existing}"
                )
                print("Has someone else re-ingested? Aborting.")
                return

            print("Would restore the following 4 records, all hidden=true:")
            print()
            print(f"{'id':>4}  {'sample_location':<32}  {'date':<12}  {'pfhxs':>7}  {'pfas6':>7}  status")
            print("-" * 90)
            for r in RECORDS:
                print(
                    f"{r['id']:>4}  "
                    f"{r['sample_location']:<32}  "
                    f"{r['sample_date']:<12}  "
                    f"{r['pfhxs']:>7}  "
                    f"{r['pfas6_sum']:>7}  "
                    f"{r['result_status']}"
                )

            if not args.confirm:
                print()
                print("[DRY RUN] No changes made. Re-run with --confirm to insert.")
                return

            print()
            print("--confirm supplied. Inserting...")
            for r in RECORDS:
                cur.execute("""
                    INSERT INTO source_discovery_results
                        (id, sample_location, sample_date, medium,
                         pfhxs, pfas6_sum, result_status, hidden)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
                """, (
                    r["id"],
                    r["sample_location"],
                    r["sample_date"],
                    r["medium"],
                    Decimal(r["pfhxs"]),
                    Decimal(r["pfas6_sum"]),
                    r["result_status"],
                ))

            # Bump the id sequence past our highest id, in case it's now behind
            cur.execute("""
                SELECT setval(
                    pg_get_serial_sequence('source_discovery_results', 'id'),
                    GREATEST(
                        (SELECT MAX(id) FROM source_discovery_results),
                        currval(pg_get_serial_sequence('source_discovery_results', 'id'))
                    )
                )
            """)

            conn.commit()
            print(f"Inserted {len(RECORDS)} rows. All hidden=true; they won't appear on the map.")
            print("Admin panel hide/unhide UI will show them by id if needed.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
