"""Offline test of the S-3 database identity guard; no database, no network.

Proves:
  1. The right address, name and row count pass; a wrong address, a wrong name,
     or a row count outside the sanity range refuses.
  2. Production and staging refuse each other, whatever their row counts.
  3. Growth doesn't trip it: production passes from today's size through the
     Dec 4 catch-up and far beyond, up to the top of the range.
  4. read_only=True refuses a writable session; both cursor row shapes work.
  5. Each kept script present in this checkout calls verify_db_identity, has no
     hard-coded database address, and retired scripts stop before running.
Constants are read from the real app/config.py.
Run: python3 tests/test_db_identity_offline.py
"""
import ast
import importlib.util
import io
import re
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CONFIG_SRC = (ROOT / "app/config.py").read_text()


def _config_constant(name):
    for node in ast.parse(CONFIG_SRC).body:
        if isinstance(node, ast.AnnAssign) and getattr(node.target, "id", None) == name:
            return ast.literal_eval(node.value)
    raise KeyError(name)


cfg = types.ModuleType("app.config")
for name in ("DB_ENV_FINGERPRINTS", "DB_ENV_ROW_RANGES", "DB_IDENTITY_NAME"):
    setattr(cfg, name, _config_constant(name))
sys.modules["app"] = types.ModuleType("app")
sys.modules["app.config"] = cfg
spec = importlib.util.spec_from_file_location("app.db_identity", ROOT / "app/db_identity.py")
dbi = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dbi)

failures = 0
def check(name, cond, detail=""):
    global failures
    print(("PASS " if cond else "FAIL ") + name + (f"  ({detail})" if detail and not cond else ""))
    failures += 0 if cond else 1

PROD, STAGING = cfg.DB_ENV_FINGERPRINTS["prod"], cfg.DB_ENV_FINGERPRINTS["staging"]
NAME = cfg.DB_IDENTITY_NAME
LOW, HIGH = cfg.DB_ENV_ROW_RANGES["prod"]


class Cursor:
    def __init__(self, row):
        self.row = row
    def execute(self, sql):
        self.sql = sql
    def fetchone(self):
        return self.row


def verify(row, env, **kw):
    try:
        dbi.verify_db_identity(Cursor(row), env, out=io.StringIO(), **kw)
        return "ok"
    except SystemExit as e:
        return str(e)


# 1. each part of the identity
check("right DB passes", verify((PROD, NAME, 414, "off"), "prod") == "ok")
check("wrong address refuses", "server address" in verify(("10.0.0.1", NAME, 414, "off"), "prod"))
check("no address refuses", "server address" in verify((None, NAME, 414, "off"), "prod"))
check("private-network IPv6 refuses", "server address" in verify(("fd12::1", NAME, 414, "off"), "prod"))
check("wrong database name refuses", "database" in verify((PROD, "other", 414, "off"), "prod"))
check("empty table refuses", "sanity range" in verify((PROD, NAME, 0, "off"), "prod"))
check("below range refuses", "sanity range" in verify((PROD, NAME, LOW - 1, "off"), "prod"))
check("above range refuses", "sanity range" in verify((PROD, NAME, HIGH + 1, "off"), "prod"))

# 2. environments refuse each other
for rows in (414, 466, 1000):
    check(f"staging DB refused as prod at {rows} rows", verify((STAGING, NAME, rows, "off"), "prod") != "ok")
    check(f"prod DB refused as staging at {rows} rows", verify((PROD, NAME, rows, "off"), "staging") != "ok")

# 3. growth
for env, addr in (("prod", PROD), ("staging", STAGING)):
    for rows in (414, 466, 600, 701, 1000, 2000, 5000, HIGH):
        check(f"{env} passes at {rows} rows", verify((addr, NAME, rows, "off"), env) == "ok")
check("range allows at least 10x today's production", HIGH >= 4140)

# 4. read-only and row shapes
check("read_only passes a read-only session", verify((PROD, NAME, 414, "on"), "prod", read_only=True) == "ok")
check("read_only refuses a writable session", "not read-only" in verify((PROD, NAME, 414, "off"), "prod", read_only=True))
dict_row = {"a": PROD, "b": NAME, "c": 414, "d": "on"}
check("dict-style cursor row works", verify(dict_row, "prod", read_only=True) == "ok")

# 5. scripts (private ones are absent from a public checkout, so only check what's here)
KEPT = ["scripts/backfill_pattern_a.py", "scripts/backfill_pattern_b.py",
        "scripts/find_inconsistent_pfas6.py", "scripts/partition_b_queue.py",
        "scripts/d8_seed_reparse.py", "scripts/d3_rehearsal_reset.py",
        "scripts/d3b_label_check.py", "scripts/d7_staging_check.py",
        "migrations/003_add_parcel_override.py", "migrations/004_add_older_report.py"]
ADDRESS = re.compile(r"\b10\.\d{1,3}\.\d{1,3}\.\d{1,3}\b")
for rel in KEPT:
    path = ROOT / rel
    if not path.exists():
        continue
    src = path.read_text()
    check(f"{rel} calls verify_db_identity", "verify_db_identity(" in src)
    check(f"{rel} has no hard-coded address", not ADDRESS.search(src), ADDRESS.findall(src))

for path in sorted((ROOT / "scripts/retired").glob("*.py")):
    body = ast.parse(path.read_text()).body
    stmt = body[1] if isinstance(body[0], ast.Expr) and isinstance(getattr(body[0].value, "value", None), str) else body[0]
    check(f"retired/{path.name} stops first", isinstance(stmt, ast.Raise))

if failures:
    print(f"\n{failures} FAILED")
    sys.exit(1)
print("\nALL PASS")
