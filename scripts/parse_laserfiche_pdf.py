#!/usr/bin/env python3
"""Dry-run the Laserfiche parser against a local PDF.

Approximates Laserfiche's plain-text mode by running pdftotext in default
(reading-order) mode against the PDF, then invoking _parse_report_text
from app.scrapers.laserfiche on that text. No browser, no DB, no network.

USE CASE: investigating parser bugs against locally-saved reference PDFs
without needing to drive Playwright through a real Laserfiche document.

CAVEAT: Laserfiche serves OCR'd text from its own viewer. pdftotext's
output is a close approximation but not identical — if a PDF has unusual
typography or column layout, results may diverge. For final verification
of a parser fix, run against the real Laserfiche document.

USAGE
    python -m scripts.parse_laserfiche_pdf path/to/report.pdf
    python -m scripts.parse_laserfiche_pdf path/to/report.pdf --trace
    python -m scripts.parse_laserfiche_pdf path/to/report.pdf --dump-text
    python -m scripts.parse_laserfiche_pdf path/to/report.pdf --layout
    python -m scripts.parse_laserfiche_pdf --text-file dump.txt --trace

--trace          Show what each compound-extraction regex pattern matches
                 (1a, 1b, 1c, 2) per compound, even if the value is None.
--dump-text      Print the extracted text before parsing.
--layout         Use pdftotext -layout (column-preserving). Default is
                 default reading-order, which is closer to Laserfiche's
                 inner_text() behavior.
--text-file PATH Skip PDF extraction; read pre-extracted text from PATH.
                 Use this for pasted Laserfiche plain-text dumps — gives
                 ground-truth text shape without pdftotext approximation.
"""

import argparse
import re
import shutil
import subprocess
import sys
from pathlib import Path

# Adjust sys.path so the script runs whether invoked as `python -m
# scripts.parse_laserfiche_pdf` from project root, or directly.
_HERE = Path(__file__).resolve().parent
_PROJECT_ROOT = _HERE.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from app.scrapers.laserfiche import (  # noqa: E402
    PFAS_COMPOUNDS,
    _extract_compound_value,
    _parse_report_text,
    _strip_ui_chrome,
)


def _run_pdftotext(pdf_path: Path, layout: bool) -> str:
    """Extract text from a PDF using pdftotext."""
    if shutil.which("pdftotext") is None:
        sys.exit(
            "ERROR: pdftotext not found. Install poppler-utils:\n"
            "  macOS:  brew install poppler\n"
            "  Linux:  apt install poppler-utils"
        )
    cmd = ["pdftotext"]
    if layout:
        cmd.append("-layout")
    cmd.extend([str(pdf_path), "-"])
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        sys.exit(f"ERROR: pdftotext failed: {result.stderr}")
    return result.stdout


def _trace_compound_patterns(content: str, short_name: str, long_name: str) -> None:
    """Print which of the 4 regex patterns in _extract_compound_value match
    this compound, and what each captures. Reveals which pattern is doing
    the work (or, more usefully, which is silently mis-firing).
    """
    print(f"  {short_name}:")

    patterns = [
        (
            "1a (value ng/L ... LONG-SHORT)",
            rf"([\d.]+|ND)\s+ng/L\s+[\d.]+\s+[\d.]+\s+\d+{re.escape(long_name[:10])}[^\n]*-?{re.escape(short_name)}",
        ),
        (
            "1b (value J? ng/L ... ACID-SHORT)",
            rf"([\d.]+|ND)\s*J?\s*ng/L[^\n]*ACID-{re.escape(short_name)}",
        ),
        (
            "1c (value ng/L ... SHORT)",
            rf"([\d.]+|ND)\s+ng/L[^\n]*{re.escape(short_name)}\b",
        ),
        (
            "2  ((SHORT) ... 3rd numeric)",
            rf"\({re.escape(short_name)}\)[^\d]*[\d.]+\s+[\d.]+(ND|[\d.]+)",
        ),
    ]

    matched_anything = False
    for label, pattern in patterns:
        m = re.search(pattern, content, re.IGNORECASE)
        if m:
            captured = m.group(1) if m.lastindex else m.group(0)
            # Show a short context window around the match
            start = max(0, m.start() - 20)
            end = min(len(content), m.end() + 20)
            ctx = content[start:end].replace("\n", "\\n")
            print(f"    [{label}] capture={captured!r}  ctx=...{ctx}...")
            matched_anything = True
        else:
            print(f"    [{label}] no match")
    if not matched_anything:
        print("    (no pattern matched)")


def _summarize(parsed: dict) -> None:
    """Pretty-print the parser output."""
    pfas6 = parsed.get("pfas6")
    print("\n=== Parsed Result ===")
    print(f"  sample_address:      {parsed.get('sample_address')!r}")
    print(f"  sample_date:         {parsed.get('sample_date')!r}")
    print(f"  pass_fail:           {parsed.get('pass_fail')!r}")
    print(f"  j_qualifier_present: {parsed.get('j_qualifier_present')}")
    print(f"  pfas6:               {pfas6}")

    print("\n  Compounds (None = not extracted, 0.0 = ND):")
    compounds = parsed.get("compounds", {})
    for short_name, _long, in_pfas6 in PFAS_COMPOUNDS:
        val = compounds.get(short_name)
        flag = " *" if in_pfas6 else "  "
        print(f"   {flag} {short_name:<14} {val}")

    # Cross-check: what would calculated_sum be from the 6 regulated compounds?
    regulated_vals = [
        compounds.get(name) for name, _, in_pfas6 in PFAS_COMPOUNDS if in_pfas6
    ]
    calc = sum(v for v in regulated_vals if v is not None)
    nonzero_calc = sum(v for v in regulated_vals if v is not None and v > 0)
    print()
    print(f"  Sum of regulated compounds (incl ND=0):   {calc}")
    print(f"  Sum of regulated compounds (excl ND=0):   {nonzero_calc}")
    if pfas6 is not None and abs(float(pfas6) - calc) > 0.01:
        print(
            f"  >>> INCONSISTENCY: stored pfas6={pfas6} disagrees with calc_sum={calc}"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Dry-run the Laserfiche parser against a local PDF or text file."
    )
    parser.add_argument(
        "pdf_path",
        type=Path,
        nargs="?",
        default=None,
        help="Path to the PDF file (omit if using --text-file)",
    )
    parser.add_argument(
        "--text-file",
        type=Path,
        default=None,
        help="Path to a pre-extracted text file (e.g. a pasted Laserfiche dump)",
    )
    parser.add_argument(
        "--trace",
        action="store_true",
        help="Show which compound-extraction regex patterns match for each compound",
    )
    parser.add_argument(
        "--dump-text",
        action="store_true",
        help="Print the extracted text before parsing",
    )
    parser.add_argument(
        "--layout",
        action="store_true",
        help="Use pdftotext -layout instead of default reading-order mode",
    )
    args = parser.parse_args()

    if args.text_file and args.pdf_path:
        sys.exit("ERROR: pass either a PDF path or --text-file, not both.")
    if not args.text_file and not args.pdf_path:
        sys.exit("ERROR: must pass a PDF path or --text-file.")

    if args.text_file:
        if not args.text_file.exists():
            sys.exit(f"ERROR: {args.text_file} does not exist.")
        raw_text = args.text_file.read_text()
    else:
        if not args.pdf_path.exists():
            sys.exit(f"ERROR: {args.pdf_path} does not exist.")
        raw_text = _run_pdftotext(args.pdf_path, layout=args.layout)

    content = _strip_ui_chrome(raw_text)

    if args.dump_text:
        print("=== Extracted Text (after _strip_ui_chrome) ===")
        print(content)
        print("=== End Text ===\n")

    if args.trace:
        print("=== Compound Pattern Trace ===")
        for short_name, long_name, _ in PFAS_COMPOUNDS:
            _trace_compound_patterns(content, short_name, long_name)
        print()

    parsed = _parse_report_text(content)
    _summarize(parsed)


if __name__ == "__main__":
    main()
