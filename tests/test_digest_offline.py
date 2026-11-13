"""Offline test of digest failure handling (D-9), using fakes (no DB, no network).

Proves:
  1. every send fails  -> results stay un-notified (retried next run), failure reported
  2. every send works  -> results marked notified
  3. partial failure   -> results marked notified (no re-send), failure reported
  4. engine records failed digests on the run's error_message, status unchanged
Run: python3 tests/test_digest_offline.py
"""
import importlib.util
import sys
import types
from datetime import datetime, timezone

# --- Fake app.* modules BEFORE loading email.py / engine.py ---
def _mod(name, **attrs):
    m = types.ModuleType(name)
    m.__path__ = []
    for k, v in attrs.items():
        setattr(m, k, v)
    sys.modules[name] = m
    return m

class _Col:
    """Stands in for a SQLAlchemy column in filter expressions."""
    def is_(self, other): return True
    def isnot(self, other): return True
    def in_(self, other): return True
    def __eq__(self, other): return True
    def __ne__(self, other): return True
    __hash__ = object.__hash__

class _S: pass
settings = _S()
settings.resend_api_key = "test-key"
settings.email_from = "alerts@example.com"
settings.base_url = "https://example.test"

_mod("app")
_mod("app.config", MCL=70, RETEST_WINDOW_DAYS=365, SMS_THRESHOLD=1000.0,
     NOTIFIABLE_RUN_STATUSES=("success",), settings=settings)

class PfasResult:
    notified_at = _Col(); hidden = _Col()
class SourceDiscoveryResult:
    notified_at = _Col(); hidden = _Col(); latitude = _Col(); longitude = _Col()
    geocode_review_needed = _Col(); medium = _Col()
class User:
    confirmed_at = _Col(); unsubscribed_at = _Col(); mobile = _Col()
class Subscription:
    neighborhood = _Col()
class ScrapeRun:
    id = _Col()

_mod("app.models")
_mod("app.models.results", PfasResult=PfasResult, SourceDiscoveryResult=SourceDiscoveryResult)
_mod("app.models.users", User=User, Subscription=Subscription)
_mod("app.models.scraper", ScrapeRun=ScrapeRun)
_mod("app.routers")
_mod("app.routers.api", _clean_sd_street_name=lambda s: s)
_mod("app.notifications")
_mod("app.notifications.tokens",
     generate_manage_token=lambda uid: "m", generate_unsubscribe_token=lambda uid: "u")
_mod("app.notifications.sms", send_sms_alert=lambda user, result, src: False)

def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod

email = _load("app.notifications.email", "app/notifications/email.py")
engine = _load("app.notifications.engine", "app/notifications/engine.py")

# Keep the test about notified_at, not HTML rendering.
email._result_to_email_row = lambda r, src: {"id": id(r)}
email._build_digest_html = lambda hood, rows, unsub, manage: "<html></html>"

# --- Fake Resend: fail for chosen recipients ---
FAIL_FOR = set()
SENDS = []
def fake_send(msg):
    to = msg["to"][0]
    SENDS.append(to)
    if to in FAIL_FOR:
        raise RuntimeError("API key is invalid")
    return {"id": "ok"}
email.resend.Emails.send = fake_send

# --- Fake DB ---
class FakeUser:
    def __init__(self, uid, addr):
        self.id, self.email, self.mobile, self.subscriptions = uid, addr, None, []

class FakeResult:
    def __init__(self, hood):
        self.neighborhood, self.notified_at, self.pfas6_sum = hood, None, 5.0

class FakeRunRow:
    def __init__(self):
        self.id, self.source, self.status, self.error_message = "run-1", "laserfiche", "success", None

class FakeQuery:
    def __init__(self, db, model): self.db, self.model = db, model
    def join(self, *a, **k): return self
    def filter(self, *a, **k): return self
    def first(self): return self.db.run if self.model is ScrapeRun else None
    def all(self):
        if self.model is User: return self.db.users
        if self.model is PfasResult: return self.db.results
        return []

class FakeDB:
    def __init__(self, users, results):
        self.users, self.results, self.run, self.commits = users, results, FakeRunRow(), 0
    def query(self, model): return FakeQuery(self, model)
    def commit(self): self.commits += 1

A, B = "a@example.com", "b@example.com"

def setup(fail):
    FAIL_FOR.clear(); FAIL_FOR.update(fail); SENDS.clear()
    results = [FakeResult("Madaket"), FakeResult("Madaket")]
    db = FakeDB([FakeUser(1, A), FakeUser(2, B)], results)
    return db, results

# 1) every send fails -> not marked, failure counted
db, results = setup({A, B})
sent, failed = email.send_neighborhood_digest("Madaket", results, [], db)
assert (sent, failed) == (0, 2), (sent, failed)
assert all(r.notified_at is None for r in results), "all-failed digest must leave notified_at NULL"
print("1 all sends fail -> results left un-notified for retry: PASS")

# 2) every send works -> marked
db, results = setup(set())
sent, failed = email.send_neighborhood_digest("Madaket", results, [], db)
assert (sent, failed) == (2, 0), (sent, failed)
assert all(isinstance(r.notified_at, datetime) for r in results), "successful digest must mark notified"
print("2 all sends succeed -> results marked notified: PASS")

# 3) partial failure -> marked (no duplicate to A), failure counted
db, results = setup({B})
sent, failed = email.send_neighborhood_digest("Madaket", results, [], db)
assert (sent, failed) == (1, 1), (sent, failed)
assert all(isinstance(r.notified_at, datetime) for r in results), "partial digest must mark notified"
print("3 partial failure -> marked notified, failure counted: PASS")

# 4) engine: all-failed digest is recorded on the run; status unchanged; retried next run
db, results = setup({A, B})
summary = engine.run_notifications("run-1", db)
assert summary["email_failures"] == 2 and summary["failed_digests"] == ["Madaket (2 of 2)"], summary
assert db.run.status == "success", "digest failure must not change run status"
assert db.run.error_message == "digest send failed: Madaket (2 of 2)", db.run.error_message
assert all(r.notified_at is None for r in results)
FAIL_FOR.clear()  # key fixed -> next run delivers the same results
summary = engine.run_notifications("run-1", db)
assert summary["emails"] == 2 and summary["email_failures"] == 0, summary
assert all(isinstance(r.notified_at, datetime) for r in results), "retry must deliver and mark"
print("4 engine records failure on run, next run retries and delivers: PASS")

# 4b) existing error_message (e.g. degraded note) is kept, failure appended
db, results = setup({A, B})
db.run.error_message = "1 short folder(s)"
engine.run_notifications("run-1", db)
assert db.run.error_message == "1 short folder(s); digest send failed: Madaket (2 of 2)", db.run.error_message
print("4b failure note appended to existing error_message: PASS")

print("\nALL DIGEST TESTS PASSED")
