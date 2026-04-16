#!/usr/bin/env python3
"""Dry-run parser: show what massdep.py would extract from a given PDF.

Runs the production _parse_pdf() function from app.scrapers.massdep
against a local PDF file. Prints each extracted sample location with
its compound values and computed PFAS6 sum. No database writes.

Usage:
    python scripts/parse_pdf_dry.py path/to/report.pdf

Optional: filter output to only show locations matching a substring.
    python scripts/parse_pdf_dry.py path/to/report.pdf --filter "TOMS WAY"
"""

import argparse
import os
import sys
from pathlib import Path

# Make the app/ package importable when running the script from the repo root.
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pdf_path", help="Path to the PDF to parse.")
    parser.add_argument(
        "--filter",
        dest="filter_str",
        default=None,
        help="Only print locations whose sample_location/well_id contains this substring (case-insensitive).",
    )
    args = parser.parse_args()

    pdf_path = Path(args.pdf_path)
    if not pdf_path.exists():
        print(f"ERROR: file not found: {pdf_path}", file=sys.stderr)
        sys.exit(1)

    # Import after sys.path is set up, so `from app.scrapers.massdep` works.
    from app.scrapers.massdep import _parse_pdf

    print(f"Parsing: {pdf_path}")
    print(f"File size: {pdf_path.stat().st_size:,} bytes")
    print()

    try:
        locations = _parse_pdf(str(pdf_path))
    except Exception as e:
        print(f"ERROR: _parse_pdf raised an exception: {e}", file=sys.stderr)
        raise

    if not locations:
        print("No sample locations extracted from this PDF.")
        return

    if args.filter_str:
        needle = args.filter_str.lower()
        filtered = [
            loc for loc in locations
            if needle in (loc.get("well_id", "") or "").lower()
               or needle in (loc.get("address", "") or "").lower()
        ]
        print(f"Filter: {args.filter_str!r}")
        print(f"Matching locations: {len(filtered)} of {len(locations)} total")
        print()
        locations_to_show = filtered
    else:
        print(f"Extracted {len(locations)} sample locations.")
        print()
        locations_to_show = locations

    for i, loc in enumerate(locations_to_show, 1):
        well_id = loc.get("well_id", "?")
        medium = loc.get("medium", "?")
        sample_date = loc.get("sample_date", "?")
        pfas6 = loc.get("pfas6")
        status = loc.get("status", "?")
        compounds = loc.get("compounds", {})

        print(f"--- Location {i} ---")
        print(f"  well_id:     {well_id}")
        print(f"  medium:      {medium}")
        print(f"  sample_date: {sample_date}")
        pfas6_str = f"{pfas6:.2f}" if pfas6 is not None else "—"
        print(f"  pfas6:       {pfas6_str}")
        print(f"  status:      {status}")
        print(f"  compounds ({len(compounds)}):")
        # Show the 6 regulated compounds first, in a stable order
        regulated_order = ["PFOS", "PFOA", "PFNA", "PFHxS", "PFHpA", "PFDA"]
        for name in regulated_order:
            val = compounds.get(name)
            val_str = f"{val:.2f}" if val is not None else "—"
            marker = "  *" if name in regulated_order else "   "
            print(f"    {marker} {name:<8} {val_str}")
        # Any other compounds
        other_names = [n for n in compounds.keys() if n not in regulated_order]
        if other_names:
            for name in sorted(other_names):
                val = compounds[name]
                val_str = f"{val:.2f}" if val is not None else "—"
                print(f"      {name:<8} {val_str}")
        print()

    # Summary of PFAS6 sums for filtered set
    if args.filter_str and locations_to_show:
        print("Summary (filtered):")
        for loc in locations_to_show:
            well_id = loc.get("well_id", "?")
            pfas6 = loc.get("pfas6")
            pfhxs = loc.get("compounds", {}).get("PFHxS")
            pfas6_str = f"{pfas6:.2f}" if pfas6 is not None else "—"
            pfhxs_str = f"{pfhxs:.2f}" if pfhxs is not None else "—"
            print(f"  {well_id:<30}  PFHxS={pfhxs_str:>8}  PFAS6={pfas6_str:>8}")


if __name__ == "__main__":
    main()
