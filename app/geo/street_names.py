"""D-3: one standard spelling per street, for display and for new results.

Mechanical rules only, no guessing:
  - a leading fraction left over from a house number is removed
    ("1/2 Skyline Drive" -> "Skyline Drive"; house numbers are never shown);
  - a trailing parcel tag is removed ("Deer Run Rd M/P: 57-13.1" -> "Deer Run Road");
  - the town name and anything after it are removed, as the rest of the address
    ("Polpis Rd Nantucket" -> "Polpis Road"; "Millbrook Road Nantucket <name>"
    -> "Millbrook Road", so an owner's name is never shown), unless "Nantucket"
    is the first word;
  - an all-caps word becomes title case ("SESACHACHA" -> "Sesachacha");
  - a trailing abbreviated suffix is spelled out from STREET_SUFFIXES
    ("Polpis Rd" / "Polpis RD" / "Polpis Rd." -> "Polpis Road"), also before a
    trailing parenthetical ("Polpis Rd (A)" -> "Polpis Road (A)");
  - known corrections from STREET_NAME_CORRECTIONS ("5th Way" -> "Fifth Way").

Not handled (needs judgement, see D-8): missing suffixes ("Quidnet"), apostrophes
("Lover's Lane" vs "Lovers Lane"), different real suffixes ("Long Pond Drive" vs
"Long Pond Road"), and other free text.
"""

import re
from typing import Optional

from app.config import STREET_NAME_CORRECTIONS, STREET_SUFFIXES

_LEADING_FRACTION = re.compile(r"^\d+/\d+\s+")
_PARCEL_TAG = re.compile(r"\s+m\s*/\s*p\b.*$", re.I)
_TRAILING_TOWN = re.compile(r",?\s+nantucket\b.*$", re.I)
_TRAILING_PAREN = re.compile(r"(\s+\([^)]*\))$")
_CORRECTIONS = {k.lower(): v for k, v in STREET_NAME_CORRECTIONS.items()}


def _fix_case(word: str) -> str:
    if len(word) > 1 and word.isalpha() and word.isupper():
        return word.capitalize()
    return word


def normalize_street_name(name: Optional[str]) -> Optional[str]:
    """Return the standard spelling of a street name (None/empty passes through)."""
    if not name:
        return name
    s = " ".join(name.split())
    s = _LEADING_FRACTION.sub("", s)
    s = _PARCEL_TAG.sub("", s).strip()
    s = _TRAILING_TOWN.sub("", s).strip()
    if not s:
        return name

    m = _TRAILING_PAREN.search(s)
    tail = m.group(1) if m else ""
    core = s[: m.start()] if m else s

    words = [_fix_case(w) for w in core.split()]
    if len(words) >= 2:
        full = STREET_SUFFIXES.get(words[-1].rstrip(".").lower())
        if full:
            words[-1] = full
    core = " ".join(words)
    core = _CORRECTIONS.get(core.lower(), core)
    return core + tail


def apply_search_corrections(query: str) -> str:
    """Spell out known street names inside a free-text search ("10 5th Way" -> "10 Fifth Way")."""
    for wrong, right in STREET_NAME_CORRECTIONS.items():
        query = re.sub(rf"(?<![\w/]){re.escape(wrong)}\b", right, query, flags=re.I)
    return query
