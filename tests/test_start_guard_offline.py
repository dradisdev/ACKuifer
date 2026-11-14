"""Offline test of the N-11 start guard: a deploy start doesn't crawl.

Proves:
  1. Production schedule "0 8 1,15 * *": a start at 08:00-09:00 UTC on the 1st
     or 15th crawls; any other time (another day, earlier, after the window) doesn't.
  2. --catchup / CATCHUP_RUN, --force / LASERFICHE_FORCE_RUN and --map always run.
  3. An unreadable schedule fails open (runs) rather than silently stopping crawls.
  4. The real runner exits without calling the scraper on a deploy start, and
     calls it on a scheduled start.
  5. The schedule in app/config.py is the production one.
Run: python3 tests/test_start_guard_offline.py
"""
import ast
import importlib.util
import logging
import sys
import types
from datetime import datetime, timezone
from pathlib import Path

logging.disable(logging.CRITICAL)
ROOT = Path(__file__).resolve().parent.parent
PROD_CRON = "0 8 1,15 * *"


def _mod(name, **attrs):
    m = types.ModuleType(name)
    m.__path__ = []
    for k, v in attrs.items():
        setattr(m, k, v)
    sys.modules[name] = m
    return m

def _load(name, rel):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod

class _S: pass
settings = _S()
settings.catchup_run = False
settings.laserfiche_force_run = False
settings.laserfiche_cron_schedule = PROD_CRON
settings.laserfiche_schedule_window_minutes = 60

_mod("app")
_mod("app.config", settings=settings)
_mod("app.scrapers")
CALLS = []
_mod("app.scrapers.laserfiche",
     run_laserfiche_scraper=lambda **kw: (CALLS.append(kw), {
         "status": "success", "new_docs_found": 0, "new_docs_parsed": 0,
         "parse_errors": 0, "skipped_seen": 0, "errors": []})[1])
guard = _load("app.scrapers.start_guard", "app/scrapers/start_guard.py")
runner = _load("app.scrapers.run_laserfiche", "app/scrapers/run_laserfiche.py")

failures = 0
def check(name, cond, detail=""):
    global failures
    print(("PASS " if cond else "FAIL ") + name + (f"  ({detail})" if detail and not cond else ""))
    failures += 0 if cond else 1

def utc(*a):
    return datetime(*a, tzinfo=timezone.utc)

def reason(now, **kw):
    return guard.start_reason(cron=PROD_CRON, window_minutes=60, now=now, **kw)

# 1. Schedule window
check("1a 08:00 on the 1st runs", reason(utc(2026, 12, 1, 8, 0)) is not None)
check("1b 08:59 on the 15th runs", reason(utc(2026, 12, 15, 8, 59)) is not None)
check("1c 09:00 on the 15th (window edge) runs", reason(utc(2026, 12, 15, 9, 0)) is not None)
check("1d 09:01 on the 15th skips", reason(utc(2026, 12, 15, 9, 1)) is None)
check("1e 07:59 on the 1st skips", reason(utc(2026, 12, 1, 7, 59)) is None)
check("1f 08:30 on the 2nd skips (Dec 2 promotion)", reason(utc(2026, 12, 2, 8, 30)) is None)
check("1g a typical deploy time skips", reason(utc(2026, 11, 14, 14, 48)) is None)
check("1h non-UTC 'now' is converted",
      reason(datetime(2026, 12, 1, 3, 30, tzinfo=timezone.utc).astimezone()) is None
      and reason(utc(2026, 12, 1, 8, 30).astimezone()) is not None)

# 2. Explicit runs
off = utc(2026, 11, 14, 14, 48)
check("2a catch-up runs off-schedule", reason(off, catchup=True) == "catch-up run")
check("2b force runs off-schedule", reason(off, force=True) == "forced run")
check("2c single-map runs off-schedule", (reason(off, map_filter="21") or "").startswith("single-map"))

# 3. Unreadable schedule fails open; cron day-field rules
check("3a range syntax fails open",
      guard.start_reason(cron="0 8 1-15 * *", window_minutes=60, now=off) is not None)
check("3b wrong field count fails open",
      guard.start_reason(cron="0 8 1,15 *", window_minutes=60, now=off) is not None)
check("3c day-of-month OR day-of-week (Mon) matches a Monday",
      guard.is_scheduled_start("0 8 1 * 1", utc(2026, 11, 16, 8, 10), 60))
check("3d Sunday as 7 matches a Sunday",
      guard.is_scheduled_start("0 8 * * 7", utc(2026, 11, 15, 8, 10), 60))

# 4. Real runner
def run(argv, now):
    CALLS.clear()
    real = guard.start_reason
    runner.start_reason = lambda **kw: real(now=now, **kw)
    sys.argv = ["run_laserfiche"] + argv
    try:
        runner.main()
    except SystemExit:
        pass
    runner.start_reason = real
    return len(CALLS)

check("4a deploy start: scraper not called", run([], utc(2026, 11, 14, 14, 48)) == 0)
check("4b scheduled start: scraper called", run([], utc(2026, 12, 15, 8, 2)) == 1)
settings.laserfiche_force_run = True
check("4c LASERFICHE_FORCE_RUN=1 off-schedule: scraper called", run([], off) == 1)
settings.laserfiche_force_run = False
settings.catchup_run = True
check("4d CATCHUP_RUN=1 off-schedule: scraper called as catch-up",
      run([], off) == 1 and CALLS[0]["catchup"] is True)
settings.catchup_run = False

# 5. Config holds the production schedule
tree = ast.parse((ROOT / "app/config.py").read_text())
cfg = {}
for node in ast.walk(tree):
    if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.value is not None:
        try:
            cfg[node.target.id] = ast.literal_eval(node.value)
        except ValueError:
            pass
check("5a config schedule matches production", cfg.get("laserfiche_cron_schedule") == PROD_CRON,
      cfg.get("laserfiche_cron_schedule"))
check("5b force run is off by default", cfg.get("laserfiche_force_run") is False)

print()
if failures:
    print(f"{failures} FAILED")
    sys.exit(1)
print("ALL PASS")
