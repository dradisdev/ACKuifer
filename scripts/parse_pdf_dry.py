#!/usr/bin/env python3
"""Dry-run parser: show what massdep.py would extract from a given PDF.

Two modes:

1. Default — runs the production _parse_pdf() function from
   app.scrapers.massdep against a local PDF file. Prints each extracted
   sample location with its compound values and computed PFAS6 sum.
   No database writes.

2. --trace SUBSTR — diagnostic mode. Walks every Client ID block whose
   contents contain SUBSTR (case-insensitive) and prints the fate of
   each at every rejection gate in _parse_lab_cert_block. Use this
   when a sample you expect to find in the regular output isn't there.

Usage:
    python scripts/parse_pdf_dry.py path/to/report.pdf
    python scripts/parse_pdf_dry.py path/to/report.pdf --filter "TOMS WAY"
    python scripts/parse_pdf_dry.py path/to/report.pdf --trace "TOM"
"""

import argparse
import re
import sys
from pathlib import Path

# Make the app/ package importable when running the script from the repo root.
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))


def _run_trace(pdf_path, needle):
    """Walk every sample block whose text contains `needle` and print
    the fate of each at every rejection gate in _parse_lab_cert_block.

    NOTE: the rejection-check sequence below is intentionally duplicated
    from app/scrapers/massdep.py::_parse_lab_cert_block. This is a
    diagnostic tool and isolates risk away from production code. If
    massdep.py's rejection logic changes, update this function in
    lockstep — the line ordering mirrors the production function on
    purpose.
    """
    import pdfplumber

    from app.scrapers.massdep import (
        _split_into_sample_blocks,
        _detect_format,
        INDIVIDUAL_COMPOUND_PATTERN,
        PFAS6_LINE_PATTERN,
    )

    print(f"Tracing: {pdf_path}")
    print(f"Filter:  {needle!r}")
    print()

    # Extract text the same way _parse_pdf does.
    with pdfplumber.open(str(pdf_path)) as pdf:
        pages_text = [page.extract_text() or "" for page in pdf.pages]
        all_text = "\n".join(pages_text)

    fmt = _detect_format(all_text)
    print(f"Detected format: {fmt}")
    if fmt != "lab_cert":
        print()
        print(
            f"WARNING: --trace currently diagnoses lab_cert format only. "
            f"This PDF detected as {fmt!r}; the rejection gates printed "
            f"below are not the ones the production parser will actually "
            f"apply for that format."
        )
    print()

    blocks = _split_into_sample_blocks(all_text, pages_text)
    print(f"Total blocks from _split_into_sample_blocks: {len(blocks)}")

    needle_lower = needle.lower()
    matching = [(i, b) for i, b in enumerate(blocks) if needle_lower in b.lower()]
    print(f"Blocks containing {needle!r} (case-insensitive): {len(matching)}")
    print()

    fates = {}

    def _bump(name):
        fates[name] = fates.get(name, 0) + 1

    for i, block in matching:
        snippet = re.sub(r"\s+", " ", block[:140]).strip()
        print(f"=== Block {i} (len={len(block)}, snippet: {snippet!r}) ===")

        # ----- Replicate _parse_lab_cert_block rejection logic -----

        # Extract Client ID (lines 478-490 of massdep.py)
        client_id = None
        m = re.search(r"Client\s+ID\s*[:\t]+\s*([^\n\r]+)", block, re.I)
        if m:
            client_id = m.group(1).strip().rstrip(".,;")
            print(f"  Client ID raw:        {client_id!r}")
        else:
            m2 = re.search(r"Lab\s+Sample\s+ID\s*[:\t]+\s*([^\n\r]+)", block, re.I)
            if m2:
                client_id = f"LAB-{m2.group(1).strip()}"
                print(f"  Client ID via Lab ID: {client_id!r}")

        if not client_id:
            print("  REJECT: no Client ID extracted")
            _bump("no_client_id")
            print()
            continue

        # Cleaning (lines 493-495)
        before_clean = client_id
        client_id = re.split(r"\s+Date\s+Received:", client_id, flags=re.I)[0].strip()
        client_id = re.sub(r"\s*\[[\d.\-']*\]?\s*$", "", client_id).strip()
        client_id = re.split(r"\s{2,}", client_id)[0].strip()
        if client_id != before_clean:
            print(f"  Client ID cleaned:    {client_id!r}")

        # QC entry rejection (lines 498-502)
        if re.match(r"^(MS|DUP|LCS|MB|MSD|LCSD)\b", client_id, re.I):
            print("  REJECT: QC prefix (MS/DUP/LCS/MB/MSD/LCSD)")
            _bump("qc_prefix")
            print()
            continue
        if re.search(
            r"\b(MS Sample|DUP Sample|Method Blank|Lab Blank|"
            r"Equipment Blank|Field Blank|Duplicate\s*\d*)\b",
            client_id, re.I,
        ):
            print("  REJECT: QC phrase in client_id")
            _bump("qc_phrase_in_id")
            print()
            continue

        # First-500 char QC rejection (lines 504-506)
        first_500 = block[:500]
        if re.search(
            r"Solids,\s*Total|QC\s+Batch|Sample\s+Receipt|Standard\s+Reference",
            first_500, re.I,
        ):
            print("  REJECT: QC indicator in first 500 chars")
            _bump("qc_indicator_first_500")
            print()
            continue

        # QC analysis page rejection (lines 514-520) — checks WHOLE BLOCK
        qc_headers_in_block = [
            qc for qc in (
                "Lab Duplicate Analysis",
                "Matrix Spike Analysis",
                "Lab Control Sample Analysis",
                "Method Blank Analysis",
            ) if qc in block
        ]
        if qc_headers_in_block:
            # Useful diagnostic: where in the block do these headers appear?
            print("  REJECT: QC analysis header(s) found in block")
            for qc in qc_headers_in_block:
                pos = block.find(qc)
                pct = (100.0 * pos / max(len(block), 1))
                print(f"          {qc!r} at offset {pos} ({pct:.1f}% into block)")
            _bump("qc_analysis_header")
            print()
            continue

        # Truncated id rejection (line 523-524)
        if re.match(r"^\d+$", client_id):
            print("  REJECT: Client ID is just digits (truncated)")
            _bump("truncated_id_digits_only")
            print()
            continue

        # Road suffix check (lines 525-530) — using POST-FIX regex.
        # This intentionally mirrors the patched massdep.py regex; if
        # they ever diverge, the trace will lie about what the production
        # parser does.
        road_suffixes = (
            r"\b(?:WAY|ROAD|RD|STREET|ST|LANE|LN|DRIVE|DR|AVE|AVENUE|"
            r"CIRCLE|CIR|COURT|CT|PLACE|PL|PATH|TRAIL|TRL|BLVD|"
            r"BOULEVARD|TERRACE|TER|PIKE|HWY|HIGHWAY)(?:_[A-Z0-9]+)*\b"
        )
        if re.match(r"^\d+\s+[A-Za-z]", client_id):
            if not re.search(road_suffixes, client_id, re.I):
                print("  REJECT: no road suffix found in client_id")
                _bump("no_road_suffix")
                print()
                continue

        # Survived all rejection gates. Show what would be extracted.
        sample_date_m = re.search(
            r"(?:Sample\s+Date|Date\s+Collected|Collection\s+Date)\s*[:\t]+\s*"
            r"(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
            block, re.I,
        )
        sample_date = sample_date_m.group(1) if sample_date_m else None

        # Quick scan — Strategy A regex from massdep.
        # Production skips matches with E qualifier; we surface both
        # so it's clear when E re-extractions are present in the block.
        strategy_a_hits = []
        strategy_a_e_skipped = 0
        for cm in INDIVIDUAL_COMPOUND_PATTERN.finditer(block):
            qual = (cm.group(3) or "").upper()
            if qual == "E":
                strategy_a_e_skipped += 1
            else:
                strategy_a_hits.append((cm.group(1).upper(), cm.group(2), qual))

        pfas6_line = bool(PFAS6_LINE_PATTERN.search(block))

        print(f"  ACCEPT: client_id={client_id!r}, sample_date={sample_date}")
        print(f"          Strategy A non-E compound matches: {len(strategy_a_hits)}")
        if strategy_a_e_skipped:
            print(f"          Strategy A E-qualifier matches skipped: {strategy_a_e_skipped}")
        if strategy_a_hits:
            # Show up to 8 to keep output bounded
            for name, val, qual in strategy_a_hits[:8]:
                qual_str = f" ({qual})" if qual else ""
                print(f"             {name:<8} {val}{qual_str}")
            if len(strategy_a_hits) > 8:
                print(f"             ... and {len(strategy_a_hits) - 8} more")
        print(f"          PFAS6 summary line found:          {pfas6_line}")
        _bump("accepted")
        print()

    # Summary
    print("=" * 64)
    print("Trace summary")
    print(f"  Total blocks                                     {len(blocks)}")
    print(f"  Blocks containing {needle!r}                       {len(matching)}")
    for fate in (
        "accepted",
        "no_client_id",
        "qc_prefix",
        "qc_phrase_in_id",
        "qc_indicator_first_500",
        "qc_analysis_header",
        "truncated_id_digits_only",
        "no_road_suffix",
    ):
        n = fates.get(fate, 0)
        print(f"    {fate:<46} {n}")


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("pdf_path", help="Path to the PDF to parse.")
    parser.add_argument(
        "--filter",
        dest="filter_str",
        default=None,
        help="Only print locations whose sample_location/well_id contains this substring (case-insensitive).",
    )
    parser.add_argument(
        "--trace",
        dest="trace_str",
        default=None,
        help=(
            "Diagnostic mode. Walks every Client ID block whose contents "
            "contain this substring (case-insensitive) and prints the "
            "fate of each at every rejection gate in _parse_lab_cert_block. "
            "Use to find why a sample isn't appearing in the regular output."
        ),
    )
    args = parser.parse_args()

    pdf_path = Path(args.pdf_path)
    if not pdf_path.exists():
        print(f"ERROR: file not found: {pdf_path}", file=sys.stderr)
        sys.exit(1)

    # Trace mode short-circuits the regular parse output.
    if args.trace_str is not None:
        _run_trace(pdf_path, args.trace_str)
        return

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
