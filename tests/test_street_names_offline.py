"""Offline test of D-3 street-name standardization; no database, no browser.

Proves:
  1. normalize_street_name spells out abbreviated suffixes (all the staging
     variants: Rd, RD, Rd., Ave, Ave., Ln, Dr, Dr., St, St.), fixes all-caps,
     removes a leading house-number fraction, a trailing M/P parcel tag, and the
     town name with anything after it (so an owner's name is never shown),
     applies the known corrections (2nd-5th Way) and the operator's D-8
     decisions, and leaves alone what it can't decide (Rainbow's End,
     Polpis Road (A), a leading "St").
  2. Every multi-spelling group that differs only by an abbreviation collapses
     to one name (e.g. "Polpis Rd" and "Polpis Road"), while different real
     streets stay apart (Pochick Avenue in Surfside vs Pochick Street in Sconset).
  3. The crawler's _extract_street_name drops the house number, its fraction,
     and stores the standard spelling.
  4. Search corrections spell out "5th Way" inside a query, and leave house
     numbers and other text alone.
  5. The map API, digest email and SMS all pass street names through it.
  6. D-8 (operator 2026-11-17): every staging spelling in a decided group shows
     one name; the real street pairs stay apart; the D-8 list is display-only,
     so a search for a village name ("Madaket", "Quidnet") is not rewritten.
The suffix map and corrections are read from the real app/config.py.
Run: python3 tests/test_street_names_offline.py
"""
import ast
import importlib.util
import re
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _config_constant(name):
    tree = ast.parse((ROOT / "app/config.py").read_text())
    for node in tree.body:
        if isinstance(node, ast.AnnAssign) and getattr(node.target, "id", None) == name:
            return ast.literal_eval(node.value)
    raise KeyError(name)


cfg = types.ModuleType("app.config")
cfg.STREET_SUFFIXES = _config_constant("STREET_SUFFIXES")
cfg.STREET_NAME_CORRECTIONS = _config_constant("STREET_NAME_CORRECTIONS")
cfg.STREET_NAME_DISPLAY_CORRECTIONS = _config_constant("STREET_NAME_DISPLAY_CORRECTIONS")
sys.modules["app"] = types.ModuleType("app")
sys.modules["app.config"] = cfg
spec = importlib.util.spec_from_file_location("app.geo.street_names", ROOT / "app/geo/street_names.py")
sn = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sn)
norm = sn.normalize_street_name

failures = 0
def check(name, cond, detail=""):
    global failures
    print(("PASS " if cond else "FAIL ") + name + (f"  ({detail})" if detail and not cond else ""))
    failures += 0 if cond else 1

def eq(name, got, want):
    check(name, got == want, f"got {got!r}, want {want!r}")


# 1. The function
CASES = [
    ("Sesachacha RD", "Sesachacha Road"),
    ("SESACHACHA RD", "Sesachacha Road"),
    ("Polpis Rd", "Polpis Road"),
    ("Eel Pond Rd.", "Eel Pond Road"),
    ("Arkansas Ave", "Arkansas Avenue"),
    ("Tristan Ave.", "Tristan Avenue"),
    ("Roberts Ln", "Roberts Lane"),
    ("Skyline Dr", "Skyline Drive"),
    ("Skyline Dr.", "Skyline Drive"),
    ("Exeter St", "Exeter Street"),
    ("Clarendon St.", "Clarendon Street"),
    ("Polpis Road", "Polpis Road"),
    ("1/2 Skyline Drive", "Skyline Drive"),
    ("Deer Run Rd M/P: 57-13.1", "Deer Run Road"),
    ("Madaket Road m/p:39/13.6", "Madaket Road"),
    ("Polpis Rd Nantucket", "Polpis Road"),
    ("Polpis Rd, Nantucket, MA", "Polpis Road"),
    ("Nantucket", "Nantucket"),
    ("Pochick Avenue Nantucket customer", "Pochick Avenue"),
    ("Millbrook Road Nantucket Jane Doe", "Millbrook Road"),
    ("Nantucket Avenue", "Nantucket Avenue"),
    ("5th Way", "Fifth Way"),
    ("5TH WAY", "Fifth Way"),
    ("2nd Way", "Second Way"),
    ("3rd Way", "Third Way"),
    ("4th Way", "Fourth Way"),
    ("Millbrook", "Millbrook Road"),
    ("MILLBROOK", "Millbrook Road"),
    ("Millbrook Rd", "Millbrook Road"),
    ("Polpis Rd (A)", "Polpis Road (A)"),
    ("Polpis Road (D)", "Polpis Road (D)"),
    ("Rainbow's End", "Rainbow's End"),
    ("Quidnet", "Quidnet Road"),
    ("St Paul Lane", "St Paul Lane"),
    ("St", "St"),
    ("McKinley Ave", "McKinley Avenue"),
    ("  Polpis   Rd ", "Polpis Road"),
]
for raw, want in CASES:
    eq(f"1 {raw!r}", norm(raw), want)
check("1 None and empty pass through", norm(None) is None and norm("") == "")

# 2. Abbreviation-only spelling groups collapse (from staging, 2026-11-16)
GROUPS = [
    ["Arkansas Ave", "Arkansas Avenue"], ["Berkley St", "Berkley Street"],
    ["Eel Point Rd", "Eel Point Road"], ["Polpis Rd", "Polpis Road"],
    ["Roberts Lane", "Roberts Ln"], ["Sesachacha RD", "Sesachacha Road"],
    ["Skyline Dr", "Skyline Dr.", "Skyline Drive"], ["Surfside Rd", "Surfside Road"],
    ["Western Ave", "Western Avenue"], ["Okorwaw Ave", "Okorwaw Avenue"],
    ["Millbrook", "Millbrook Road"],
]
for g in GROUPS:
    out = {norm(x) for x in g}
    check(f"2 {g} -> one name", len(out) == 1, out)
check("2 different real suffixes stay different",
      norm("Miacomet Avenue") != norm("Miacomet Road"))

# Pochick Avenue (Surfside) and Pochick Street (Siasconset) are different real
# streets and must never be merged, by the suffix map or by a correction.
eq("2 Pochick Ave -> Avenue", norm("Pochick Ave"), "Pochick Avenue")
eq("2 Pochick St -> Street", norm("Pochick St."), "Pochick Street")
eq("2 Pochick Avenue unchanged", norm("Pochick Avenue"), "Pochick Avenue")
eq("2 Pochick Street unchanged", norm("Pochick Street"), "Pochick Street")
pochick = {k.lower(): v for k, v in _config_constant("STREET_NAME_CORRECTIONS").items()}
check("2 no correction maps one Pochick onto the other",
      not any("pochick" in k or "pochick" in v.lower() for k, v in pochick.items()))

# 3. Crawler extraction (function body taken from the real file)
src = (ROOT / "app/scrapers/laserfiche.py").read_text()
fn = next(n for n in ast.parse(src).body
          if isinstance(n, ast.FunctionDef) and n.name == "_extract_street_name")
ns = {"re": re, "Optional": __import__("typing").Optional, "normalize_street_name": norm}
exec(compile(ast.Module([fn], []), "laserfiche.py", "exec"), ns)
extract = ns["_extract_street_name"]
eq("3 house number dropped, suffix spelled out", extract("12 Sesachacha RD, Nantucket, MA"), "Sesachacha Road")
eq("3 number with letter", extract("30R Polpis Rd, Nantucket"), "Polpis Road")
eq("3 number with fraction", extract("12 1/2 Skyline Drive, Nantucket"), "Skyline Drive")
eq("3 ordinal street kept", extract("7 5th Way, Nantucket"), "Fifth Way")
check("3 empty address", extract(None) is None and extract("") is None)

# 4. Search corrections
asc = sn.apply_search_corrections
eq("4 ordinal in query", asc("10 5th Way"), "10 Fifth Way")
eq("4 any case", asc("10 5TH way, Nantucket"), "10 Fifth Way, Nantucket")
eq("4 other text untouched", asc("10 Polpis Rd"), "10 Polpis Rd")
eq("4 longer number untouched", asc("105th Way"), "105th Way")
eq("4 bare Millbrook gets its suffix", asc("10 Millbrook"), "10 Millbrook Road")
eq("4 bare Millbrook before town", asc("10 Millbrook, Nantucket"), "10 Millbrook Road, Nantucket")
eq("4 Millbrook Rd not doubled", asc("10 Millbrook Rd"), "10 Millbrook Rd")
eq("4 Millbrook Road not doubled", asc("10 Millbrook Road"), "10 Millbrook Road")
eq("4 ordinal followed by suffix-like word untouched", asc("10 5th Way Extension"), "10 5th Way Extension")

# 6. D-8 operator decisions (2026-11-17), every staging spelling per group
D8_GROUPS = {
    "Dukes Road": ["Dukes Road", "Dukes", "DUKES RD"],
    "Madaket Road": ["Madaket Road", "Madaket", "Madaket Rd"],
    "Okorwaw Avenue": ["Okorwaw Avenue", "Okorwaw"],
    "Quidnet Road": ["Quidnet Road", "Quidnet"],
    "Sesapana Road": ["Sesapana Road", "Sesapana"],
    "Tennessee Avenue": ["Tennessee Avenue", "Tennessee", "Tennessee Ave"],
    "Vestal Street": ["Vestal Street", "Vestal", "Vestal St"],
    "Chuck Hollow Road": ["Chuck Hollow Road", "Chuck Hollow"],
    "Wood Hollow Road": ["Wood Hollow Road", "Wood Hollow"],
    "Hummock Pond Road": ["Hummock Pond Road", "Hummock Pond"],
    "Tom Nevers Road": ["Tom Nevers Road", "Tom Nevers"],
    "Eel Point Road": ["Eel Point Road", "Eel Point", "Eel Point Rd"],
    "Helens Drive": ["Helens Drive", "Helen's Drive", "Helen's Dr"],
    "Lovers Lane": ["Lovers Lane", "Lover's Lane", "LOVER'S LN"],
    "Pippens Way": ["Pippens Way", "Pippen's Way"],
    "Long Pond Drive": ["Long Pond Drive", "Long Pond Road", "Long Pond Rd"],
    "Shawkemo Hills Lane": ["Shawkemo Hills Lane", "Shawkemo Hills Road"],
    "Brier Patch Road": ["Brier Patch Road", "Briar Patch Road", "Briar Patch Rd"],
    "Camp Richard": ["Camp Richard - well 1"],
    "Rugged Road": ["Rugged Road", "Rugged Road - Main House -"],
}
for want, spellings in D8_GROUPS.items():
    for raw in spellings:
        eq(f"6 {raw!r}", norm(raw), want)
eq("6 town text still removed before a correction", norm("Quidnet Nantucket"), "Quidnet Road")
# Different real streets (operator 2026-11-17): never merged
for a, b in (("Miacomet Avenue", "Miacomet Road"),
             ("West Miacomet Avenue", "West Miacomet Road"),
             ("Somerset Road", "Somerset Lane"),
             ("Pochick Avenue", "Pochick Street")):
    eq(f"6 {a} unchanged", norm(a), a)
    eq(f"6 {b} unchanged", norm(b), b)
d8 = _config_constant("STREET_NAME_DISPLAY_CORRECTIONS")
never = ("miacomet", "somerset", "pochick")
check("6 no D-8 correction touches a never-merge street",
      not any(n in (k + v).lower() for k, v in d8.items() for n in never))
check("6 D-8 list kept out of the search list",
      not set(k.lower() for k in d8) & set(k.lower() for k in _config_constant("STREET_NAME_CORRECTIONS")))
# Polpis (A)/(D) left as they are (operator 2026-11-17)
eq("6 Polpis Road (A) kept", norm("Polpis Road (A)"), "Polpis Road (A)")
# Search: display-only, a village name is not rewritten
for q in ("Madaket", "Quidnet", "Tom Nevers", "10 Long Pond Road", "Eel Point"):
    eq(f"6 search leaves {q!r}", asc(q), q)

# 5. Wiring
api = (ROOT / "app/routers/api.py").read_text()
email = (ROOT / "app/notifications/email.py").read_text()
sms = (ROOT / "app/notifications/sms.py").read_text()
mapr = (ROOT / "app/routers/map.py").read_text()
check("5 map API: Laserfiche and MassDEP names normalized",
      "normalize_street_name(strip_unit_designator(" in api
      and "normalize_street_name(_clean_sd_street_name(" in api)
for label, text in (("email", email), ("SMS", sms)):
    check(f"5 {label}: Laserfiche and MassDEP names normalized",
          "normalize_street_name(strip_unit_designator(result.street_name))" in text
          and "normalize_street_name(_clean_sd_street_name(" in text)
check("5 search applies corrections", "apply_search_corrections(address)" in mapr)

print()
if failures:
    print(f"{failures} FAILED")
    sys.exit(1)
print("ALL PASS")
