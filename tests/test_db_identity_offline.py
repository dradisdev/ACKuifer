"""Offline test of the S-3 database identity guard; no database, no network.

Proves:
  1. The right public host, name and row count pass; a wrong, missing or
     private-network host, a wrong name, or a row count outside the sanity
     range refuses. A changed server address (Postgres restart) doesn't matter.
  2. Production and staging refuse each other, whatever their row counts.
  3. Growth doesn't trip it: production passes from today's size through the
     Dec 4 catch-up and far beyond, up to the top of the range.
  4. read_only=True refuses a writable session; both cursor row shapes work,
     and the host is read from a psycopg2 cursor, a SQLAlchemy Session and a
     SQLAlchemy Connection (from the underlying connection, not the config).
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
ADDR = "10.0.0.7"  # any server address; it is printed, never compared


class Info:
    def __init__(self, host):
        self.host = host
class DBAPIConn:
    def __init__(self, host):
        self.info = Info(host)


class Cursor:  # psycopg2 cursor
    def __init__(self, row, host):
        self.row, self.connection = row, DBAPIConn(host)
    def execute(self, sql):
        self.sql = sql
    def fetchone(self):
        return self.row


class Result:
    def __init__(self, row):
        self.row = row
    def one(self):
        return self.row
class PoolConn:
    def __init__(self, host):
        self.dbapi_connection = DBAPIConn(host)
class SAConnection:  # SQLAlchemy Connection
    def __init__(self, row, host):
        self.row, self.connection = row, PoolConn(host)
    def execute(self, stmt):
        return Result(self.row)
class SASession:  # SQLAlchemy Session
    def __init__(self, row, host):
        self._conn = SAConnection(row, host)
    def get_bind(self):
        return "configured engine (never used for the host)"
    def connection(self):
        return self._conn
    def execute(self, stmt):
        return Result(self._conn.row)


def verify(row, env, host, kind=Cursor, **kw):
    try:
        dbi.verify_db_identity(kind(row, host), env, out=io.StringIO(), **kw)
        return "ok"
    except SystemExit as e:
        return str(e)


# 1. each part of the identity
check("right DB passes", verify((ADDR, NAME, 414, "off"), "prod", PROD) == "ok")
check("host match ignores case", verify((ADDR, NAME, 414, "off"), "prod", PROD.upper()) == "ok")
check("wrong host refuses", "host" in verify((ADDR, NAME, 414, "off"), "prod", "other.proxy.rlwy.net"))
check("no host refuses", "host" in verify((ADDR, NAME, 414, "off"), "prod", None))
check("local socket refuses", "host" in verify((ADDR, NAME, 414, "off"), "prod", "/tmp"))
check("private-network host refuses", "host" in verify(("fd12::1", NAME, 414, "off"), "prod", "postgres.railway.internal"))
check("lookalike host refuses", "host" in verify((ADDR, NAME, 414, "off"), "prod", PROD + ".evil.example"))
for addr in ("10.226.63.254", "10.138.104.194", "10.1.2.3", None):
    check(f"server address {addr} doesn't matter", verify((addr, NAME, 414, "off"), "prod", PROD) == "ok")
check("wrong database name refuses", "database" in verify((ADDR, "other", 414, "off"), "prod", PROD))
check("empty table refuses", "sanity range" in verify((ADDR, NAME, 0, "off"), "prod", PROD))
check("below range refuses", "sanity range" in verify((ADDR, NAME, LOW - 1, "off"), "prod", PROD))
check("above range refuses", "sanity range" in verify((ADDR, NAME, HIGH + 1, "off"), "prod", PROD))
check("refusal names the host", "ABORT: host" in verify((ADDR, NAME, 414, "off"), "prod", STAGING))

# 2. environments refuse each other
check("environments have different hosts", PROD != STAGING)
for rows in (414, 466, 1000):
    check(f"staging DB refused as prod at {rows} rows", verify((ADDR, NAME, rows, "off"), "prod", STAGING) != "ok")
    check(f"prod DB refused as staging at {rows} rows", verify((ADDR, NAME, rows, "off"), "staging", PROD) != "ok")

# 3. growth
for env, host in (("prod", PROD), ("staging", STAGING)):
    for rows in (414, 466, 600, 701, 1000, 2000, 5000, HIGH):
        check(f"{env} passes at {rows} rows", verify((ADDR, NAME, rows, "off"), env, host) == "ok")
check("range allows at least 10x today's production", HIGH >= 4140)

# 4. read-only, row shapes, connection types
check("read_only passes a read-only session", verify((ADDR, NAME, 414, "on"), "prod", PROD, read_only=True) == "ok")
check("read_only refuses a writable session", "not read-only" in verify((ADDR, NAME, 414, "off"), "prod", PROD, read_only=True))
dict_row = {"a": ADDR, "b": NAME, "c": 414, "d": "on"}
check("dict-style cursor row works", verify(dict_row, "prod", PROD, read_only=True) == "ok")
for kind in (SASession, SAConnection):
    check(f"{kind.__name__} passes on its host", verify((ADDR, NAME, 414, "off"), "prod", PROD, kind) == "ok")
    check(f"{kind.__name__} refuses the other host", "host" in verify((ADDR, NAME, 414, "off"), "prod", STAGING, kind))
buf = io.StringIO()
dbi.verify_db_identity(Cursor((ADDR, NAME, 414, "off"), PROD), "prod", out=buf)
check("identity line prints host and address", f"host={PROD}" in buf.getvalue() and f"addr={ADDR}" in buf.getvalue())

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
    check(f"{rel} has no hard-coded host", "proxy.rlwy.net" not in src)

for path in sorted((ROOT / "scripts/retired").glob("*.py")):
    body = ast.parse(path.read_text()).body
    stmt = body[1] if isinstance(body[0], ast.Expr) and isinstance(getattr(body[0].value, "value", None), str) else body[0]
    check(f"retired/{path.name} stops first", isinstance(stmt, ast.Raise))

if failures:
    print(f"\n{failures} FAILED")
    sys.exit(1)
print("\nALL PASS")
