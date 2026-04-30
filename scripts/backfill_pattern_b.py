#!/usr/bin/env python3
"""Backfill Pattern B records: recompute pfas6_sum from compound columns.

Pattern B (Bug #4): Standard Barnstable lab reports where all detections
are J-qualified below the reporting limit. The lab follows the regulatory
exclusion convention and reports PFAS6=ND. Pre-fix, the parser captured
this as 0.0 and the calculation fallback didn't fire (it only fired on
None). Result: compound columns have correct J-qualified values, but
pfas6_sum is stuck at 0.

This script identifies those records and recomputes pfas6_sum from the six
regulated compounds, then re-classifies via classify_result_status from
app.config — same logic the (now-fixed) parser uses going forward.

USAGE
    python -m scripts.backfill_pattern_b           # dry-run preview (default)
    python -m scripts.backfill_pattern_b --csv     # CSV preview for audit
    python -m scripts.backfill_pattern_b --apply   # actually commit changes

CRITERIA for Pattern B candidate:
    j_qualifier_present = True
    AND pfas6_sum = 0
    AND at least one regulated compound column is non-null
    AND hidden = False

This filter excludes:
    - True non-detects (all compounds null AND j_qualifier_present false)
    - Pattern A records (DW Program forms set j_qualifier_present false
      because the form uses a different qualifier column convention)
    - Records the parser handled correctly (pfas6_sum > 0)
    - Operator-hidden records (the operator hid them deliberately)

IDEMPOTENT: re-running on already-fixed records is a no-op since the
filter excludes records where pfas6_sum > 0. Safe to run repeatedly.

TRANSACTIONAL: each record is committed in its own transaction. A mid-run
failure on one record doesn't roll back records already committed.
"""

import argparse
import csv
import sys
from decimal import Decimal
from pathlib import Path

# Path setup — works whether run as `python -m scripts.backfill_pattern_b`
# from project root or directly.
_HERE = Path(__file__).resolve().parent
_PROJECT_ROOT = _HERE.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(_PROJECT_ROOT / ".env")

from sqlalchemy import or_  # noqa: E402

from app.config import classify_result_status  # noqa: E402
from app.database import SessionLocal  # noqa: E402
from app.models.results import PfasResult  # noqa: E402


REGULATED_COLUMNS = ["pfos", "pfoa", "pfhxs", "pfna", "pfhpa", "pfda"]


def _calc_pfas6_sum(record) -> Decimal:
    """Sum the six regulated compound values, treating None as 0."""
    total = Decimal("0")
    for col in REGULATED_COLUMNS:
        val = getattr(record, col, None)
        if val is not None:
            total += val
    return total


def _find_pattern_b_candidates(db):
    """Query Pattern B candidates per the filter in the module docstring."""
    return (
        db.query(PfasResult)
        .filter(
            PfasResult.j_qualifier_present == True,  # noqa: E712
            PfasResult.pfas6_sum == Decimal("0"),
            PfasResult.hidden == False,  # noqa: E712
            or_(
                PfasResult.pfos.isnot(None),
                PfasResult.pfoa.isnot(None),
                PfasResult.pfhxs.isnot(None),
                PfasResult.pfna.isnot(None),
                PfasResult.pfhpa.isnot(None),
                PfasResult.pfda.isnot(None),
            ),
        )
        .order_by(PfasResult.sample_date.desc())
        .all()
    )


def _emit_csv(candidates) -> None:
    """Write the change preview as CSV to stdout for audit/review."""
    writer = csv.writer(sys.stdout)
    writer.writerow([
        "id", "doc_id", "street_name", "sample_date",
        "old_pfas6_sum", "new_pfas6_sum",
        "old_status", "new_status",
        "pfos", "pfoa", "pfhxs", "pfna", "pfhpa", "pfda",
    ])
    for r in candidates:
        new_sum = _calc_pfas6_sum(r)
        new_status = classify_result_status(float(new_sum))
        writer.writerow([
            str(r.id),
            r.laserfiche_doc_id,
            r.street_name or "",
            r.sample_date.isoformat() if r.sample_date else "",
            str(r.pfas6_sum),
            str(new_sum),
            r.result_status,
            new_status,
            r.pfos, r.pfoa, r.pfhxs, r.pfna, r.pfhpa, r.pfda,
        ])


def _emit_preview(candidates) -> list:
    """Print a human-readable preview table. Returns the list of (record,
    new_sum, new_status) tuples ready for application."""
    fmt = "  {0:>7}  {1:<22}  {2:<10}  {3:>9}  {4:>9}  {5:<12}  {6:<12}"
    print(fmt.format("doc", "street", "date", "old_sum", "new_sum",
                     "old_status", "new_status"))
    print("  " + "-" * 90)

    flips: dict = {}
    updates = []
    for r in candidates:
        new_sum = _calc_pfas6_sum(r)
        new_status = classify_result_status(float(new_sum))
        old_status = r.result_status
        flip_key = (
            f"{old_status} → {new_status}"
            if old_status != new_status
            else "(no status change)"
        )
        flips[flip_key] = flips.get(flip_key, 0) + 1

        print(fmt.format(
            str(r.laserfiche_doc_id),
            (r.street_name or "(none)")[:22],
            r.sample_date.isoformat() if r.sample_date else "—",
            f"{float(r.pfas6_sum):.2f}",
            f"{float(new_sum):.2f}",
            old_status or "—",
            new_status,
        ))
        updates.append((r, new_sum, new_status))

    print()
    print("Status flip summary:")
    for k, v in sorted(flips.items()):
        print(f"  {k}: {v}")

    return updates


def _apply_updates(db, updates) -> tuple:
    """Apply updates one record at a time, each in its own transaction.
    Returns (success_count, fail_count)."""
    success = 0
    failed = 0
    for r, new_sum, new_status in updates:
        try:
            r.pfas6_sum = new_sum
            r.result_status = new_status
            db.commit()
            success += 1
        except Exception as e:
            db.rollback()
            print(f"  ERROR updating record {r.id} (doc {r.laserfiche_doc_id}): {e}")
            failed += 1
    return success, failed


def main():
    parser = argparse.ArgumentParser(
        description="Backfill Pattern B records (Bug #4): recompute pfas6_sum "
                    "from compound columns where the lab reported regulatory PFAS6 as ND "
                    "but individual J-qualified detections are present."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually commit changes. Without this flag, runs in dry-run mode (default).",
    )
    parser.add_argument(
        "--csv",
        action="store_true",
        help="Output the change preview as CSV (for audit/review).",
    )
    args = parser.parse_args()

    if args.apply and args.csv:
        sys.exit("ERROR: --apply and --csv are mutually exclusive.")

    db = SessionLocal()
    try:
        candidates = _find_pattern_b_candidates(db)
    except Exception as e:
        db.close()
        sys.exit(f"ERROR querying database: {e}")

    if not candidates:
        print("No Pattern B candidates found. Nothing to do.")
        db.close()
        return

    if args.csv:
        _emit_csv(candidates)
        db.close()
        return

    n = len(candidates)
    print(f"Found {n} Pattern B candidate record{'s' if n != 1 else ''}.")
    print()
    updates = _emit_preview(candidates)
    print()

    if not args.apply:
        print(f"DRY RUN: no changes committed.")
        print(f"Re-run with --apply to commit {len(updates)} update(s).")
        db.close()
        return

    # Apply mode: explicit interactive confirmation before any DB writes.
    response = input(
        f"Apply {len(updates)} updates to the database? Type 'yes' to confirm: "
    ).strip().lower()
    if response != "yes":
        print("Aborted. No changes committed.")
        db.close()
        return

    print()
    print("Applying updates...")
    success, failed = _apply_updates(db, updates)
    print()
    print(f"Done. {success} record(s) updated, {failed} failed.")
    db.close()


if __name__ == "__main__":
    main()