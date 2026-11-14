"""Offline test of digest selection and failure handling (D-7, D-9); fakes only.

Proves:
  1. every send fails  -> results stay un-notified (retried next run), failure reported
  2. every send works  -> results marked notified
  3. partial failure   -> results marked notified (no re-send), failure reported
  4. engine records failed digests on the run's error_message, status unchanged
  D-7:
  5. new subscriber + old un-notified results -> no email, results marked notified
  6. result discovered after signup reaches them
  7. mixed subscribers -> each gets only results newer than their own signup
  8. SMS follows the same cutoff
  9. MassDEP results use their PDF's seen_documents discovery time
Run: python3 tests/test_digest_offline.py
"""
import importlib.util
import logging
import sys
import types
from datetime import datetime, timedelta, timezone

logging.disable(logging.CRITICAL)  # expected "send failed" logs would bury the PASS lines

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
class SeenDocument:
    doc_key = _Col()

_mod("app.models")
_mod("app.models.results", PfasResult=PfasResult, SourceDiscoveryResult=SourceDiscoveryResult)
_mod("app.models.users", User=User, Subscription=Subscription)
_mod("app.models.scraper", ScrapeRun=ScrapeRun, SeenDocument=SeenDocument)
_mod("app.routers")
_mod("app.routers.api", _clean_sd_street_name=lambda s: s)
_mod("app.notifications")
_mod("app.notifications.tokens",
     generate_manage_token=lambda uid: "m", generate_unsubscribe_token=lambda uid: "u")
SMS = []
_mod("app.notifications.sms",
     send_sms_alert=lambda user, result, src: (SMS.append((user.email, id(result))), True)[1])

def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod

email = _load("app.notifications.email", "app/notifications/email.py")
engine = _load("app.notifications.engine", "app/notifications/engine.py")

# Keep the test about selection and notified_at, not HTML rendering.
email._result_to_email_row = lambda r, src: {"id": id(r)}
BODIES = {}   # recipient -> row ids in their digest
def fake_html(hood, rows, unsub, manage):
    return rows
email._build_digest_html = fake_html

# --- Fake Resend: fail for chosen recipients ---
FAIL_FOR = set()
SENDS = []
def fake_send(msg):
    to = msg["to"][0]
    SENDS.append(to)
    BODIES[to] = sorted(row["id"] for row in msg["html"])
    if to in FAIL_FOR:
        raise RuntimeError("API key is invalid")
    return {"id": "ok"}
email.resend.Emails.send = fake_send

# --- Fake DB ---
T0 = datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc)   # a subscription date
EARLY, LATE = T0 - timedelta(days=30), T0 + timedelta(days=1)

class FakeSub:
    def __init__(self, hood, created_at): self.neighborhood, self.created_at = hood, created_at

class FakeUser:
    def __init__(self, uid, addr, subscribed=T0, hood="Madaket", mobile=None):
        self.id, self.email, self.mobile = uid, addr, mobile
        self.subscriptions = [FakeSub(hood, subscribed)]

class FakeResult:
    def __init__(self, hood, discovered_at=LATE, pfas6=5.0):
        self.neighborhood, self.notified_at, self.pfas6_sum = hood, None, pfas6
        self.discovered_at = discovered_at

class FakeSD(FakeResult):
    def __init__(self, hood, pdf, well):
        super().__init__(hood, discovered_at=None)
        del self.discovered_at          # SourceDiscoveryResult has no such column
        self.source_doc_url = f"{pdf}#{well}"

class FakeSeen:
    def __init__(self, key, discovered_at): self.doc_key, self.discovered_at = key, discovered_at

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
        if self.model is SourceDiscoveryResult: return self.db.sd_results
        if self.model is SeenDocument: return self.db.seen
        return []

class FakeDB:
    def __init__(self, users, results, sd_results=(), seen=()):
        self.users, self.results, self.run, self.commits = users, results, FakeRunRow(), 0
        self.sd_results, self.seen = list(sd_results), list(seen)
    def query(self, model): return FakeQuery(self, model)
    def commit(self): self.commits += 1

A, B = "a@example.com", "b@example.com"

def setup(fail):
    FAIL_FOR.clear(); FAIL_FOR.update(fail); SENDS.clear(); BODIES.clear(); SMS.clear()
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

# --- D-7: only results discovered after the subscriber signed up ---

# 5) new subscriber, only old un-notified results -> no email; marked notified
FAIL_FOR.clear(); SENDS.clear(); BODIES.clear()
old = [FakeResult("Madaket", discovered_at=EARLY), FakeResult("Madaket", discovered_at=EARLY)]
db = FakeDB([FakeUser(1, A, subscribed=T0)], old)
sent, failed = email.send_neighborhood_digest("Madaket", old, [], db)
assert (sent, failed) == (0, 0) and SENDS == [], f"old results must not be sent, got {SENDS}"
assert all(isinstance(r.notified_at, datetime) for r in old), "no-one-eligible results are marked notified"
print("5 new subscriber gets no email about older results: PASS")

# 6) a result discovered after signup reaches them
SENDS.clear(); BODIES.clear()
new = [FakeResult("Madaket", discovered_at=LATE)]
db = FakeDB([FakeUser(1, A, subscribed=T0)], new)
sent, failed = email.send_neighborhood_digest("Madaket", new, [], db)
assert (sent, failed) == (1, 0) and SENDS == [A], SENDS
assert BODIES[A] == [id(new[0])]
print("6 result discovered after signup reaches the subscriber: PASS")

# 7) mixed: A subscribed long ago, B just now -> A gets both, B only the newer one
SENDS.clear(); BODIES.clear()
r_mid = FakeResult("Madaket", discovered_at=T0)                       # between A and B signups
r_new = FakeResult("Madaket", discovered_at=T0 + timedelta(days=5))   # after both
db = FakeDB([FakeUser(1, A, subscribed=EARLY), FakeUser(2, B, subscribed=T0 + timedelta(days=2))],
            [r_mid, r_new])
sent, failed = email.send_neighborhood_digest("Madaket", [r_mid, r_new], [], db)
assert (sent, failed) == (2, 0), (sent, failed)
assert BODIES[A] == sorted([id(r_mid), id(r_new)]), BODIES
assert BODIES[B] == [id(r_new)], f"B must only get results after their signup, got {BODIES}"
print("7 mixed subscribers each get only results newer than their signup: PASS")

# 8) SMS follows the same cutoff (via engine): old high result -> no SMS; new one -> SMS
setup(set())
hi_old = FakeResult("Madaket", discovered_at=EARLY, pfas6=2000.0)
hi_new = FakeResult("Madaket", discovered_at=LATE, pfas6=2000.0)
user = FakeUser(1, A, subscribed=T0, mobile="+15085550100")
db = FakeDB([user], [hi_old, hi_new])
summary = engine.run_notifications("run-1", db)
assert SMS == [(A, id(hi_new))], f"SMS must skip results older than the signup, got {SMS}"
assert BODIES[A] == [id(hi_new)], BODIES
print("8 SMS follows the same signup cutoff: PASS")

# 9) MassDEP: discovery time comes from the PDF's seen_documents row
setup(set())
db = FakeDB([FakeUser(1, A, subscribed=T0)], [])
db.run.source = "massdep"
sd_old = FakeSD("Madaket", "https://eea.example/old.pdf", "MW-1")
sd_new = FakeSD("Madaket", "https://eea.example/new.pdf", "MW-2")
sd_unknown = FakeSD("Madaket", "https://eea.example/missing.pdf", "MW-3")
db.sd_results = [sd_old, sd_new, sd_unknown]
db.seen = [FakeSeen("https://eea.example/old.pdf", EARLY), FakeSeen("https://eea.example/new.pdf", LATE)]
engine.run_notifications("run-1", db)
assert BODIES[A] == [id(sd_new)], f"only the PDF found after signup counts; unknown time is not sent; got {BODIES}"
print("9 MassDEP uses its PDF's discovery time; unknown time is never sent: PASS")

print("\nALL DIGEST TESTS PASSED")
