#!/usr/bin/env python3
"""Find pfas_results records where pfas6_sum is mathematically inconsistent
with the individual compound columns.

A record is flagged when pfas6_sum is less than the largest individual
regulated compound value (PFOS, PFOA, PFHxS, PFNA, PFHpA, PFDA), which is
impossible if pfas6_sum was correctly computed as the sum of those six.

Surfaced in Session 10 as Bug #4. Working hypothesis is a units/scale
mismatch (ug/L vs ng/L, factor of 1000) or a regex in
app/scrapers/laserfiche.py:_extract_compound_value capturing the wrong
column during PFAS6 extraction. Session 10 counted ~142 affected records
out of ~298 surviving pfas_results — this script is the canonical way to
re-derive that list.

USAGE
    python -m scripts.find_inconsistent_pfas6
    python -m scripts.find_inconsistent_pfas6 --limit 20
    python -m scripts.find_inconsistent_pfas6 --csv > /tmp/inconsistent.csv

The script is read-only. It does not modify the database.
"""

import argparse
import csv
import os
import sys

try:
    from dotenv import load_dotenv
except ImportError:
    sys.exit("ERROR: python-dotenv not installed. pip install python-dotenv")

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    sys.exit("ERROR: psycopg2 not installed. pip install psycopg2-binary")

load_dotenv()

# Records where pfas6_sum < max(individual compound) are mathematically
# inconsistent. We also project calculated_sum (the sum of all 6 regulated
# compounds) so the operator can see what pfas6_sum *should* be if it's
# computed correctly from the compound columns.
QUERY = """
SELECT
    id,
    laserfiche_doc_id,
    map_number,
    parcel_number,
    neighborhood,
    street_name,
    sample_date,
    pfos, pfoa, pfhxs, pfna, pfhpa, pfda,
    pfas6_sum,
    result_status,
    GREATEST(
        COALESCE(pfos,  0),
        COALESCE(pfoa,  0),
        COALESCE(pfhxs, 0),
        COALESCE(pfna,  0),
        COALESCE(pfhpa, 0),
        COALESCE(pfda,  0)
    ) AS max_compound,
    (
        COALESCE(pfos,  0) + COALESCE(pfoa,  0) + COALESCE(pfhxs, 0) +
        COALESCE(pfna,  0) + COALESCE(pfhpa, 0) + COALESCE(pfda,  0)
    ) AS calculated_sum
FROM pfas_results
WHERE COALESCE(pfas6_sum, 0) < GREATEST(
        COALESCE(pfos,  0),
        COALESCE(pfoa,  0),
        COALESCE(pfhxs, 0),
        COALESCE(pfna,  0),
        COALESCE(pfhpa, 0),
        COALESCE(pfda,  0)
    )
ORDER BY (
    COALESCE(pfos,  0) + COALESCE(pfoa,  0) + COALESCE(pfhxs, 0) +
    COALESCE(pfna,  0) + COALESCE(pfhpa, 0) + COALESCE(pfda,  0)
) - COALESCE(pfas6_sum, 0) DESC;
"""


def _fmt_num(val):
    """Format a Decimal/None as a fixed-width number string."""
    if val is None:
        return "    ND"
    try:
        return f"{float(val):>6.2f}"
    except (TypeError, ValueError):
        return str(val)[:6]


def main():
    parser = argparse.ArgumentParser(
        description="Find pfas_results records where pfas6_sum < max(compound)."
    )
    parser.add_argument(
        "--csv",
        action="store_true",
        help="Emit results as CSV to stdout (full per-compound values).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit output to N most-discrepant rows.",
    )
    args = parser.parse_args()

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        sys.exit("ERROR: DATABASE_URL not set. Check your .env file.")

    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(QUERY)
                rows = cur.fetchall()
    except psycopg2.Error as e:
        sys.exit(f"ERROR: Database query failed: {e}")

    if args.limit:
        rows = rows[: args.limit]

    if not rows:
        print("No inconsistent records found.")
        return

    if args.csv:
        # Convert any Decimal/date values to strings for CSV
        fieldnames = list(rows[0].keys())
        writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: ("" if v is None else str(v)) for k, v in row.items()})
        return

    # Human-readable summary
    print(
        f"Found {len(rows)} pfas_results records where pfas6_sum < max(compound)."
    )
    print(
        "Session 10 BUILD_NOTES recorded ~142 such records; significant "
        "drift from that number warrants investigation."
    )
    print()

    header = (
        f"{'doc_id':>9}  {'street':<24}  {'date':<10}  "
        f"{'pfas6':>7}  {'max':>7}  {'calc_sum':>8}  {'status':<12}"
    )
    print(header)
    print("-" * len(header))

    for r in rows:
        doc_id = str(r["laserfiche_doc_id"] or "?")[:9]
        street = (r["street_name"] or "")[:24]
        date = str(r["sample_date"] or "?")[:10]
        pfas6 = _fmt_num(r["pfas6_sum"])
        max_cmp = _fmt_num(r["max_compound"])
        calc = _fmt_num(r["calculated_sum"])
        status = (r["result_status"] or "?")[:12]
        print(
            f"{doc_id:>9}  {street:<24}  {date:<10}  "
            f"{pfas6:>7}  {max_cmp:>7}  {calc:>8}  {status:<12}"
        )

    print()
    print("Tip: rerun with --csv > /tmp/inconsistent.csv for full compound values.")


if __name__ == "__main__":
    main()