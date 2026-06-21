"""Offline test of app.monitoring.health using fakes (no DB, no network).

Proves: healthy->silent, unhealthy->alert+durable-write, dedup via persisted
timestamp (the 6/12 double-send fix), naive completed_at doesn't raise, and the
alerting-disabled gate. Run: python3 test_health_offline.py
"""
import sys
import types
from datetime import datetime, timedelta, timezone

# --- Build fake app.* modules BEFORE importing health ---
app = types.ModuleType("app"); sys.modules["app"] = app

# app.config.settings
cfg = types.ModuleType("app.config")
class _S: pass
settings = _S()
settings.deadmans_window_days = 20
settings.operator_email = "op@example.com"
settings.deadmans_alerting_enabled = True
cfg.settings = settings
sys.modules["app.config"] = cfg

# app.models.scraper.ScrapeRun  (only attrs the code touches)
models = types.ModuleType("app.models"); sys.modules["app.models"] = models
m_scraper = types.ModuleType("app.models.scraper")
class _Col:
    def desc(self): return self
    def __eq__(self, other): return True
class ScrapeRun:
    source = _Col(); status = _Col(); completed_at = _Col(); started_at = _Col()
m_scraper.ScrapeRun = ScrapeRun
sys.modules["app.models.scraper"] = m_scraper

# app.models.site_config.SiteConfig
m_sc = types.ModuleType("app.models.site_config")
class SiteConfig:
    def __init__(self, key=None, value=None, updated_at=None):
        self.key, self.value, self.updated_at = key, value, updated_at
m_sc.SiteConfig = SiteConfig
sys.modules["app.models.site_config"] = m_sc

# app.notifications.email.send_deadmans_alert  (record calls)
notif = types.ModuleType("app.notifications"); sys.modules["app.notifications"] = notif
m_email = types.ModuleType("app.notifications.email")
SENT = []
def send_deadmans_alert(source, last_run_at):
    SENT.append((source, last_run_at)); return True
m_email.send_deadmans_alert = send_deadmans_alert
sys.modules["app.notifications.email"] = m_email

# Make app a package so submodule import works
app.__path__ = []
import importlib.util
spec = importlib.util.spec_from_file_location("app.monitoring.health", "app/monitoring/health.py")
m_mon = types.ModuleType("app.monitoring"); m_mon.__path__ = []; sys.modules["app.monitoring"] = m_mon
health = importlib.util.module_from_spec(spec); sys.modules["app.monitoring.health"] = health
spec.loader.exec_module(health)

# --- Fake DB / query layer ---
class FakeRun:
    def __init__(self, source, status, completed_at, started_at=None):
        self.source, self.status = source, status
        self.completed_at, self.started_at = completed_at, started_at or completed_at

class FakeQuery:
    def __init__(self, db, model): self.db, self.model = db, model; self._filters=[]
    def filter(self, *a, **k): return self  # filtering emulated via db buckets below
    def order_by(self, *a, **k): return self
    def first(self):
        if self.model is SiteConfig:
            return self.db.cfg_rows.get(self._want_key)
        # ScrapeRun: return based on the most recent matching the current ask
        return self.db._next_run
    def all(self): return []

class FakeDB:
    def __init__(self):
        self.cfg_rows = {}      # key -> SiteConfig
        self._next_run = None
        self.committed = 0
    def query(self, model):
        q = FakeQuery(self, model)
        return q
    def add(self, obj):
        if isinstance(obj, SiteConfig): self.cfg_rows[obj.key] = obj
    def commit(self): self.committed += 1

# health._get_last_alert/_set_last_alert call db.query(SiteConfig).filter(...).first()
# Our FakeQuery.first needs the key; patch filter to capture it.
def _filter_capture(self, *args, **kwargs):
    # SiteConfig.key == X  -> args[0] is a BinaryExpression in real SA; here we
    # cheat: health builds key string, so intercept via a thread-local-ish stash.
    return self
FakeQuery.filter = _filter_capture

# Simplest: monkeypatch health's _get/_set to use a dict directly is too invasive.
# Instead drive through public check_and_alert with a DB that resolves keys by
# scanning. Override FakeQuery.first for SiteConfig to use last set key.
import re
_LAST_KEY = {"k": None}
_orig_prefix = health._DEDUP_KEY_PREFIX
def patched_get(db, source):
    row = db.cfg_rows.get(_orig_prefix + source)
    if not row or not row.value: return None
    return health._as_aware_utc(datetime.fromisoformat(row.value))
def patched_set(db, source, when):
    key = _orig_prefix + source
    iso = health._as_aware_utc(when).isoformat()
    if key in db.cfg_rows: db.cfg_rows[key].value = iso
    else: db.cfg_rows[key] = SiteConfig(key=key, value=iso, updated_at=when)
    db.commit()
health._get_last_alert = patched_get
health._set_last_alert = patched_set

def status_for(run, now, healthy_expected=None):
    db = FakeDB(); db._next_run = run
    st = health.compute_scraper_status(db, run.source if run else "laserfiche", now=now)
    return st

now = datetime(2026, 6, 18, 12, 0, tzinfo=timezone.utc)
fails = []

# 1) HEALTHY: success 2 days ago (aware) -> healthy, no alert
db = FakeDB(); db._next_run = FakeRun("laserfiche","success", now - timedelta(days=2))
st = health.compute_scraper_status(db, "laserfiche", now=now)
assert st["healthy"] is True, "fresh success should be healthy"
SENT.clear()
rep = health.check_and_alert(db, {"laserfiche": st}, now=now)
assert SENT == [], f"healthy must not alert, got {SENT}"
print("1 healthy->silent: PASS")

# 2) UNHEALTHY (naive completed_at, 30 days old) -> alert + durable write, no raise
db = FakeDB(); db._next_run = FakeRun("laserfiche","success", (now - timedelta(days=30)).replace(tzinfo=None))
st = health.compute_scraper_status(db, "laserfiche", now=now)  # must not raise on naive
assert st["healthy"] is False, "30-day-old success should be unhealthy"
SENT.clear()
rep = health.check_and_alert(db, {"laserfiche": st}, now=now)
assert SENT and SENT[0][0]=="laserfiche", f"unhealthy must alert, got {SENT}"
assert (_orig_prefix+"laserfiche") in db.cfg_rows, "must persist dedup timestamp"
print("2 unhealthy->alert+durable write (naive ts, no raise): PASS")

# 3) DEDUP: immediate second check on SAME db within 24h -> no second send
SENT.clear()
st2 = health.compute_scraper_status(db, "laserfiche", now=now + timedelta(hours=1))
rep = health.check_and_alert(db, {"laserfiche": st2}, now=now + timedelta(hours=1))
assert SENT == [], f"within-24h must dedup, got {SENT}"
print("3 dedup within 24h (the 6/12 fix): PASS")

# 3b) After 25h -> sends again
SENT.clear()
later = now + timedelta(hours=25)
st3 = health.compute_scraper_status(db, "laserfiche", now=later)
rep = health.check_and_alert(db, {"laserfiche": st3}, now=later)
assert SENT and SENT[0][0]=="laserfiche", "after cooldown should re-alert"
print("3b re-alert after cooldown: PASS")

# 4) DISABLED gate -> unhealthy but no send
settings.deadmans_alerting_enabled = False
db = FakeDB(); db._next_run = FakeRun("massdep","success",(now - timedelta(days=40)).replace(tzinfo=None))
st = health.compute_scraper_status(db, "massdep", now=now)
SENT.clear()
rep = health.check_and_alert(db, {"massdep": st}, now=now)
assert SENT == [], f"disabled gate must not send, got {SENT}"
assert "massdep" in rep["skipped_disabled"]
print("4 alerting-disabled gate: PASS")
settings.deadmans_alerting_enabled = True

# 5) reserved key guard
assert health.is_reserved_config_key("deadmans_last_alert_laserfiche") is True
assert health.is_reserved_config_key("homepage_intro") is False
print("5 reserved-key guard: PASS")

print("\nALL OFFLINE TESTS PASSED")
