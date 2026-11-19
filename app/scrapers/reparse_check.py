"""D-8: compare a stored Laserfiche result with a fresh parse of its document.

Pure functions, no database, no browser. Used by a private read-only script that
parses the prototype-seeded documents once, saves the values the crawler would
store today, and compares each environment's stored rows with that file.

What counts as the same:
  - PFAS values within REPARSE_MATCH_TOLERANCE_PPT; a missing value and 0 are both
    non-detect, so they match (the crawler stores non-detect compounds as NULL);
  - street names after normalize_street_name on both sides (spelling is already
    standardized at display, D-3/D-8, so a stored "Quidnet Rd" is not a difference);
  - sample dates as ISO dates; status, pass/fail and the J flag exactly.
"""

from decimal import Decimal
from typing import Optional

from app.config import REPARSE_MATCH_TOLERANCE_PPT
from app.geo.street_names import normalize_street_name

COMPOUND_FIELDS = ("pfos", "pfoa", "pfhxs", "pfna", "pfhpa", "pfda")
NUMERIC_FIELDS = ("pfas6_sum",) + COMPOUND_FIELDS
EXACT_FIELDS = ("result_status", "pass_fail", "j_qualifier_present")
COMPARED_FIELDS = NUMERIC_FIELDS + EXACT_FIELDS + ("street_name", "sample_date")


def _num(value) -> float:
    if value is None or value == "":
        return 0.0
    return float(Decimal(str(value)))


def _date(value) -> Optional[str]:
    if value is None or value == "":
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)[:10]


def diff_row(stored: Optional[dict], expected: dict) -> list[tuple[str, object, object]]:
    """Return (field, stored, expected) for every field that differs.

    stored is None when the environment has no row for the document.
    """
    if stored is None:
        return [("row", "missing", "present")]
    diffs = []
    for f in NUMERIC_FIELDS:
        if abs(_num(stored.get(f)) - _num(expected.get(f))) > REPARSE_MATCH_TOLERANCE_PPT:
            diffs.append((f, stored.get(f), expected.get(f)))
    for f in EXACT_FIELDS:
        if stored.get(f) != expected.get(f):
            diffs.append((f, stored.get(f), expected.get(f)))
    s_street = normalize_street_name(stored.get("street_name"))
    e_street = normalize_street_name(expected.get("street_name"))
    if s_street != e_street:
        diffs.append(("street_name", s_street, e_street))
    if _date(stored.get("sample_date")) != _date(expected.get("sample_date")):
        diffs.append(("sample_date", _date(stored.get("sample_date")), _date(expected.get("sample_date"))))
    return diffs
