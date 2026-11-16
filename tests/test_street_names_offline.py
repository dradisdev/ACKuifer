"""Offline test of D-3 street-name standardization; no database, no browser.

Proves:
  1. normalize_street_name spells out abbreviated suffixes (all the staging
     variants: Rd, RD, Rd., Ave, Ave., Ln, Dr, Dr., St, St.), fixes all-caps,
     removes a leading house-number fraction, a trailing M/P parcel tag, and the
     town name with anything after it (so an owner's name is never shown),
     applies the known corrections (2nd-5th Way), and leaves alone what it
     can't decide (Rainbow's End, Quidnet, Polpis Road (A), a leading "St").
  2. Every multi-spelling group that differs only by an abbreviation collapses
     to one name (e.g. "Polpis Rd" and "Polpis Road").
  3. The crawler's _extract_street_name drops the house number, its fraction,
     and stores the standard spelling.
  4. Search corrections spell out "5th Way" inside a query, and leave house
     numbers and other text alone.
  5. The map API, digest email and SMS all pass street names through it.
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
    ("Polpis Rd (A)", "Polpis Road (A)"),
    ("Polpis Road (D)", "Polpis Road (D)"),
    ("Rainbow's End", "Rainbow's End"),
    ("Quidnet", "Quidnet"),
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
]
for g in GROUPS:
    out = {norm(x) for x in g}
    check(f"2 {g} -> one name", len(out) == 1, out)
check("2 different real suffixes stay different",
      norm("Long Pond Drive") != norm("Long Pond Road"))

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
