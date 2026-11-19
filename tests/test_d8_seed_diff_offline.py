"""Offline test of the D-8 seeded-row comparison; no database, no browser.

Proves diff_row (app/scrapers/reparse_check.py):
  1. an identical row has no differences;
  2. a changed PFAS6 total, compound, status, pass/fail, J flag or sample date
     is reported;
  3. differences within REPARSE_MATCH_TOLERANCE_PPT are not reported, and
     NULL vs 0 (both non-detect) is not a difference;
  4. a street that differs only in spelling ("Quidnet Rd" vs "Quidnet Road")
     is not a difference, while a different street is;
  5. a missing row is reported.
Constants are read from the real app/config.py.
Run: python3 tests/test_d8_seed_diff_offline.py
"""
import ast
import datetime
import importlib.util
import sys
import types
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _config_constant(name):
    tree = ast.parse((ROOT / "app/config.py").read_text())
    for node in tree.body:
        if isinstance(node, ast.AnnAssign) and getattr(node.target, "id", None) == name:
            return ast.literal_eval(node.value)
    raise KeyError(name)


cfg = types.ModuleType("app.config")
for name in ("STREET_SUFFIXES", "STREET_NAME_CORRECTIONS", "STREET_NAME_DISPLAY_CORRECTIONS",
             "REPARSE_MATCH_TOLERANCE_PPT"):
    setattr(cfg, name, _config_constant(name))
sys.modules["app"] = types.ModuleType("app")
sys.modules["app.config"] = cfg
sys.modules["app.geo"] = types.ModuleType("app.geo")
for mod, path in (("app.geo.street_names", "app/geo/street_names.py"),
                  ("app.scrapers.reparse_check", "app/scrapers/reparse_check.py")):
    spec = importlib.util.spec_from_file_location(mod, ROOT / path)
    m = importlib.util.module_from_spec(spec)
    sys.modules[mod] = m
    spec.loader.exec_module(m)
diff_row = sys.modules["app.scrapers.reparse_check"].diff_row

failures = 0
def check(name, cond, detail=""):
    global failures
    print(("PASS " if cond else "FAIL ") + name + (f"  ({detail})" if detail and not cond else ""))
    failures += 0 if cond else 1

EXPECTED = {
    "pfas6_sum": 5.04, "pfos": 3.1, "pfoa": 1.94, "pfhxs": None, "pfna": None,
    "pfhpa": None, "pfda": None, "result_status": "DETECT", "pass_fail": "PASS",
    "j_qualifier_present": False, "street_name": "Quidnet Road", "sample_date": "2025-07-29",
}
STORED = {
    "pfas6_sum": Decimal("5.04"), "pfos": Decimal("3.1"), "pfoa": Decimal("1.94"),
    "pfhxs": None, "pfna": None, "pfhpa": None, "pfda": None,
    "result_status": "DETECT", "pass_fail": "PASS", "j_qualifier_present": False,
    "street_name": "Quidnet Road", "sample_date": datetime.date(2025, 7, 29),
}

# 1. identical
d = diff_row(dict(STORED), EXPECTED)
check("identical row has no differences", d == [], d)

# 2. each change is reported
for field, value in (("pfas6_sum", Decimal("0")), ("pfos", Decimal("6.0")),
                     ("result_status", "NON-DETECT"), ("pass_fail", "FAIL"),
                     ("j_qualifier_present", True), ("sample_date", datetime.date(2025, 7, 15))):
    row = dict(STORED, **{field: value})
    d = diff_row(row, EXPECTED)
    check(f"changed {field} is reported", [x[0] for x in d] == [field], d)

# 3. tolerance and non-detect
d = diff_row(dict(STORED, pfas6_sum=Decimal("5.044")), EXPECTED)
check("difference within tolerance is not reported", d == [], d)
d = diff_row(dict(STORED, pfas6_sum=Decimal("5.06")), EXPECTED)
check("difference over tolerance is reported", [x[0] for x in d] == ["pfas6_sum"], d)
d = diff_row(dict(STORED, pfhxs=Decimal("0"), pfda=Decimal("0.0")), EXPECTED)
check("stored 0 vs parsed NULL (non-detect) is not a difference", d == [], d)
d = diff_row(dict(STORED, pfas6_sum=None, pfos=None, pfoa=None, result_status="NON-DETECT"),
             dict(EXPECTED, pfas6_sum=0, pfos=None, pfoa=None, result_status="NON-DETECT"))
check("NULL total vs 0 total is not a difference", d == [], d)

# 4. street spelling vs a different street
d = diff_row(dict(STORED, street_name="Quidnet Rd"), EXPECTED)
check("street differing only in spelling is not a difference", d == [], d)
d = diff_row(dict(STORED, street_name="QUIDNET RD"), dict(EXPECTED, street_name="Quidnet"))
check("abbreviated vs bare decided street is not a difference", d == [], d)
d = diff_row(dict(STORED, street_name="Sesachacha Road"), EXPECTED)
check("a different street is reported", [x[0] for x in d] == ["street_name"], d)
d = diff_row(dict(STORED, street_name=None), EXPECTED)
check("missing stored street is reported", [x[0] for x in d] == ["street_name"], d)

# 5. missing row
d = diff_row(None, EXPECTED)
check("missing row is reported", d == [("row", "missing", "present")], d)

if failures:
    print(f"\n{failures} FAILED")
    sys.exit(1)
print("\nALL PASS")
