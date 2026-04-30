#!/usr/bin/env python3
"""Backfill Pattern A records: re-parse from live Laserfiche.

Pattern A (Bug #4): MassDEP DW Program form rows render as
"(SHORT) NAME MDL MRL VALUE" where MRL "2.00" is concatenated to VALUE
without a separator. The pre-fix parser's Pattern 2 regex captured only
the trailing digit of the concatenated number due to greedy backtracking,
producing nonsense integer values 1-9 in the compound columns. The PFAS6
fallback regex captured ".0" from "=2043.0", so pfas6_sum was always 0.

Unlike Pattern B (deterministic SQL recompute), Pattern A records can't
be repaired from DB state alone — the current compound values are noise,
not signal. This script drives the (now-fixed) parser against each
candidate doc using Playwright and overwrites the bad values.

USAGE
    # Dry-run on 1 record (default — investigate first)
    python -m scripts.backfill_pattern_a

    # Dry-run on 5 records to spot-check
    python -m scripts.backfill_pattern_a --limit 5

    # Apply on a single specific doc (for targeted testing)
    python -m scripts.backfill_pattern_a --doc-id 260046

    # Watch the browser drive (helpful for first run)
    python -m scripts.backfill_pattern_a --visible

    # APPLY mode requires BOTH --apply and --commit-changes
    python -m scripts.backfill_pattern_a --limit 5 --apply --commit-changes

    # Apply all remaining (after spot-checking)
    python -m scripts.backfill_pattern_a --limit 200 --apply --commit-changes

CRITERIA for Pattern A candidate:
    pfas6_sum = 0
    AND j_qualifier_present = False  (Pattern A is DW Program forms;
                                       J-qualifier signals Pattern B)
    AND at least one regulated compound column is non-null
    AND hidden = False

SAFETY GUARDS:
    1. --apply alone is rejected. Both --apply AND --commit-changes
       must be passed explicitly. Two flags = two intentional decisions.
       No interactive prompt that can be auto-piped.
    2. --limit defaults to 1. You must explicitly raise it.
    3. JSON backup of every to-be-modified record is written to
       /tmp/pattern_a_backup_<timestamp>.json BEFORE any DB writes.
       Restore is a manual operation but the data is preserved.
    4. Each record commits in its own transaction. Mid-run failure
       on one record doesn't roll back records already committed.
    5. Idempotent: filter excludes records with pfas6_sum > 0, so
       re-running on already-fixed records is a no-op.
    6. Failed records (parser returns None, exception raised) are
       skipped, logged, and don't block subsequent records. Re-run
       to retry them.

LOGS:
    Stdout: pretty progress for live watching.
    File:   /tmp/pattern_a_run_<timestamp>.log — full structured output
            including before/after values for every record processed.

NOTE: this script overwrites the following PfasResult fields ONLY:
      pfos, pfoa, pfhxs, pfna, pfhpa, pfda, pfas6_sum, result_status,
      pass_fail. It does NOT touch street_name, sample_date,
      neighborhood, map_number, parcel_number, or hidden — those were
      not affected by the parsing bug and may have been operator-corrected.
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

# Path setup — works as `python -m scripts.backfill_pattern_a` from project root
_HERE = Path(__file__).resolve().parent
_PROJECT_ROOT = _HERE.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(_PROJECT_ROOT / ".env")

from sqlalchemy import or_  # noqa: E402
from playwright.sync_api import sync_playwright  # noqa: E402

from app.config import classify_result_status  # noqa: E402
from app.database import SessionLocal  # noqa: E402
from app.models.results import PfasResult  # noqa: E402
from app.scrapers.laserfiche import _parse_report  # noqa: E402


REGULATED_COLUMNS = ["pfos", "pfoa", "pfhxs", "pfna", "pfhpa", "pfda"]


def _setup_file_logging(timestamp: str) -> Path:
    """Set up a file handler that captures everything the script does.
    Returns the log file path."""
    log_path = Path(f"/tmp/pattern_a_run_{timestamp}.log")
    file_handler = logging.FileHandler(log_path, mode="w")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    ))
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(file_handler)
    return log_path


def _find_pattern_a_candidates(db, doc_id_filter: int = None):
    """Query Pattern A candidates. If doc_id_filter is provided, narrow to
    just that record (still applying all other criteria as a safety check)."""
    q = db.query(PfasResult).filter(
        PfasResult.pfas6_sum == Decimal("0"),
        PfasResult.j_qualifier_present == False,  # noqa: E712
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
    if doc_id_filter is not None:
        q = q.filter(PfasResult.laserfiche_doc_id == doc_id_filter)
    return q.order_by(PfasResult.sample_date.desc()).all()


def _serialize_record(r) -> dict:
    """Serialize a PfasResult to a dict suitable for JSON backup."""
    def _dec_str(v):
        return str(v) if v is not None else None

    return {
        "id": str(r.id),
        "laserfiche_doc_id": r.laserfiche_doc_id,
        "street_name": r.street_name,
        "sample_date": r.sample_date.isoformat() if r.sample_date else None,
        "pfas6_sum": _dec_str(r.pfas6_sum),
        "result_status": r.result_status,
        "pass_fail": r.pass_fail,
        "pfos": _dec_str(r.pfos),
        "pfoa": _dec_str(r.pfoa),
        "pfhxs": _dec_str(r.pfhxs),
        "pfna": _dec_str(r.pfna),
        "pfhpa": _dec_str(r.pfhpa),
        "pfda": _dec_str(r.pfda),
        "j_qualifier_present": r.j_qualifier_present,
    }


def _to_dec(val):
    """Mirrors _process_document's to_dec convention: None or 0 → None,
    otherwise Decimal."""
    if val is None or val == 0:
        return None
    return Decimal(str(val))


def _format_compounds(r) -> str:
    """Format compound values for display."""
    return (
        f"PFOS={r.pfos} PFOA={r.pfoa} PFHxS={r.pfhxs} "
        f"PFNA={r.pfna} PFHpA={r.pfhpa} PFDA={r.pfda}"
    )


def _format_compounds_dict(d: dict) -> str:
    """Format compounds dict (from _to_dec applied) for display."""
    return (
        f"PFOS={d.get('pfos')} PFOA={d.get('pfoa')} PFHxS={d.get('pfhxs')} "
        f"PFNA={d.get('pfna')} PFHpA={d.get('pfhpa')} PFDA={d.get('pfda')}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Backfill Pattern A records by re-parsing live Laserfiche docs.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply mode flag #1. Required to commit changes (along with --commit-changes).",
    )
    parser.add_argument(
        "--commit-changes",
        action="store_true",
        help="Apply mode flag #2. Required to commit changes (along with --apply). "
             "Two flags prevents auto-bypass of safety. Type both to commit.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1,
        help="Maximum records to process (default: 1). Required: explicitly raise to do more.",
    )
    parser.add_argument(
        "--doc-id",
        type=int,
        default=None,
        help="Process only this Laserfiche doc_id (for targeted testing).",
    )
    parser.add_argument(
        "--visible",
        action="store_true",
        help="Run Playwright with headless=False so you can watch the browser drive.",
    )
    args = parser.parse_args()

    # Two-flag safety check
    will_commit = args.apply and args.commit_changes
    if args.apply and not args.commit_changes:
        sys.exit(
            "ERROR: --apply requires --commit-changes. Both flags required to commit;\n"
            "       this prevents auto-bypass of the safety check.\n"
            "       Re-run with both: --apply --commit-changes"
        )
    if args.commit_changes and not args.apply:
        sys.exit(
            "ERROR: --commit-changes requires --apply too. Both flags required.\n"
            "       Re-run with both: --apply --commit-changes"
        )

    # Persistent log
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    log_path = _setup_file_logging(timestamp)
    logger = logging.getLogger("backfill_a")
    logger.info(
        "Pattern A backfill starting. limit=%d will_commit=%s doc_id=%s visible=%s",
        args.limit, will_commit, args.doc_id, args.visible,
    )

    # Find candidates
    db = SessionLocal()
    try:
        all_candidates = _find_pattern_a_candidates(db, doc_id_filter=args.doc_id)
    except Exception as e:
        db.close()
        sys.exit(f"ERROR querying database: {e}")

    if not all_candidates:
        msg = (f"No Pattern A candidate found for doc_id {args.doc_id}."
               if args.doc_id else "No Pattern A candidates found.")
        print(msg + " Nothing to do.")
        db.close()
        return

    candidates = all_candidates[:args.limit]
    print(f"Found {len(all_candidates)} Pattern A candidate(s) total. "
          f"Processing {len(candidates)} (--limit={args.limit}).")
    print(f"Mode: {'APPLY (will commit)' if will_commit else 'DRY-RUN (no DB writes)'}")
    print(f"Browser: {'visible' if args.visible else 'headless'}")
    print(f"Log:     {log_path}")

    # Backup before any writes
    backup_path = None
    if will_commit:
        backup_path = Path(f"/tmp/pattern_a_backup_{timestamp}.json")
        backup_data = [_serialize_record(r) for r in candidates]
        backup_path.write_text(json.dumps(backup_data, indent=2))
        print(f"Backup:  {backup_path} ({len(backup_data)} records)")

    print()

    # Run Playwright
    success = 0
    failed = 0
    skipped = 0
    no_change = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not args.visible)
        page = browser.new_page()
        page.set_default_timeout(30000)

        for i, r in enumerate(candidates, 1):
            doc_id = str(r.laserfiche_doc_id)
            label = f"[{i}/{len(candidates)}] doc {doc_id} — {r.street_name or '(no street)'}"
            print(label)
            logger.info(label)
            logger.debug("OLD: %s", _serialize_record(r))

            # Re-parse
            try:
                parsed = _parse_report(page, doc_id)
            except Exception as e:
                print(f"   FAIL: parser exception — {type(e).__name__}: {e}")
                logger.exception("Parser exception for doc %s", doc_id)
                failed += 1
                continue

            if parsed is None:
                print(f"   SKIP: parser returned None (could not open Plain Text view)")
                logger.warning("Parser returned None for doc %s", doc_id)
                skipped += 1
                continue

            # Build new DB values mirroring _process_document conventions
            compounds = parsed.get("compounds") or {}
            new_vals = {
                "pfos": _to_dec(compounds.get("PFOS")),
                "pfoa": _to_dec(compounds.get("PFOA")),
                "pfhxs": _to_dec(compounds.get("PFHxS")),
                "pfna": _to_dec(compounds.get("PFNA")),
                "pfhpa": _to_dec(compounds.get("PFHpA")),
                "pfda": _to_dec(compounds.get("PFDA")),
            }
            pfas6_raw = parsed.get("pfas6")
            new_pfas6 = (
                Decimal(str(pfas6_raw)) if pfas6_raw is not None else Decimal("0")
            )
            new_status = classify_result_status(float(new_pfas6))
            new_pass_fail = parsed.get("pass_fail")

            # Sanity check: did anything actually change?
            unchanged = (
                new_pfas6 == r.pfas6_sum
                and new_vals["pfos"] == r.pfos
                and new_vals["pfoa"] == r.pfoa
                and new_vals["pfhxs"] == r.pfhxs
                and new_vals["pfna"] == r.pfna
                and new_vals["pfhpa"] == r.pfhpa
                and new_vals["pfda"] == r.pfda
                and new_status == r.result_status
            )

            print(f"   OLD: pfas6={r.pfas6_sum} status={r.result_status}")
            print(f"        {_format_compounds(r)}")
            print(f"   NEW: pfas6={new_pfas6} status={new_status}")
            print(f"        {_format_compounds_dict(new_vals)}")

            logger.debug(
                "NEW for doc %s: pfas6=%s status=%s pass_fail=%s compounds=%s",
                doc_id, new_pfas6, new_status, new_pass_fail, new_vals,
            )

            if unchanged:
                print(f"   NOOP: re-parse produced identical values; nothing to update.")
                logger.info("doc %s: re-parse identical, no update needed", doc_id)
                no_change += 1
                continue

            if not will_commit:
                continue  # dry-run

            # Apply: update only the bug-affected fields, leave the rest alone
            try:
                r.pfos = new_vals["pfos"]
                r.pfoa = new_vals["pfoa"]
                r.pfhxs = new_vals["pfhxs"]
                r.pfna = new_vals["pfna"]
                r.pfhpa = new_vals["pfhpa"]
                r.pfda = new_vals["pfda"]
                r.pfas6_sum = new_pfas6
                r.result_status = new_status
                if new_pass_fail:
                    r.pass_fail = new_pass_fail
                db.commit()
                success += 1
                print(f"   ✓ COMMITTED")
                logger.info("doc %s: committed", doc_id)
            except Exception as e:
                db.rollback()
                print(f"   FAIL: commit error — {type(e).__name__}: {e}")
                logger.exception("DB write failed for doc %s", doc_id)
                failed += 1

        browser.close()

    db.close()

    # Summary
    print()
    print("=" * 60)
    if will_commit:
        print(f"DONE — Committed: {success} | Skipped: {skipped} | "
              f"Failed: {failed} | No-change: {no_change}")
        print(f"Log:     {log_path}")
        print(f"Backup:  {backup_path}")
    else:
        would_apply = len(candidates) - skipped - failed - no_change
        print(f"DRY-RUN — Would commit: {would_apply} | Skipped: {skipped} | "
              f"Failed: {failed} | No-change: {no_change}")
        print(f"Log:     {log_path}")
        print()
        print(f"To apply, re-run with: --apply --commit-changes")


if __name__ == "__main__":
    main()