"""Offline test of the D-3b "older report, newly added" label; fakes only.

Rule (redesign 2026-11-16): a Laserfiche result is labelled when its sample date
is more than OLDER_REPORT_MIN_AGE_DAYS (90) before it was discovered, whichever
run found it. Digest email and SMS only; never the map; never MassDEP.

Proves:
  1. The age rule: 91 days labelled, 90 and 89 not; no sample date or no
     discovery time means no label; the catch-up flag neither adds nor removes it.
  2. The map API and the popup template carry no label.
  3. The digest email shows the label on the old Laserfiche row only.
  4. The SMS follows the same rule (old Laserfiche: "Older report"; recent and
     MassDEP: "Sample date"), within 160 chars.
  5. --catchup or CATCHUP_RUN=1 still starts a catch-up run (recorded on the
     run row), but the scraper no longer sets older_report on results.
The label wording and the cutoff are read from the real app/config.py.
Run: python3 tests/test_older_report_offline.py
"""
import ast
import importlib.util
import logging
import sys
import types
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

logging.disable(logging.CRITICAL)
ROOT = Path(__file__).resolve().parent.parent

# --- The real label constants, read from app/config.py without importing it ---
def _config_constant(name):
    tree = ast.parse((ROOT / "app/config.py").read_text())
    for node in tree.body:
        if isinstance(node, ast.AnnAssign) and getattr(node.target, "id", None) == name:
            return ast.literal_eval(node.value)
    raise KeyError(name)

LABEL = _config_constant("OLDER_REPORT_LABEL")
SHORT = _config_constant("OLDER_REPORT_SHORT")
MIN_AGE = _config_constant("OLDER_REPORT_MIN_AGE_DAYS")


# --- Fake app.* modules ---
def _mod(name, **attrs):
    m = types.ModuleType(name)
    m.__path__ = []
    for k, v in attrs.items():
        setattr(m, k, v)
    sys.modules[name] = m
    return m

class _Col:
    def is_(self, o): return True
    def isnot(self, o): return True
    def in_(self, o): return True
    def __eq__(self, o): return True
    def __ne__(self, o): return True
    def __ge__(self, o): return True
    __hash__ = object.__hash__

class _S: pass
settings = _S()
settings.resend_api_key = ""
settings.email_from = "alerts@example.com"
settings.base_url = "https://example.test"
settings.twilio_account_sid = settings.twilio_auth_token = settings.twilio_from_number = ""
settings.catchup_run = False
settings.laserfiche_force_run = False
settings.laserfiche_cron_schedule = "0 8 1,15 * *"
settings.laserfiche_schedule_window_minutes = 60

class PfasResult:
    hidden = _Col(); neighborhood = _Col(); result_status = _Col(); sample_date = _Col()
class SourceDiscoveryResult:
    hidden = _Col(); latitude = _Col(); longitude = _Col(); geocode_review_needed = _Col()
    medium = _Col(); neighborhood = _Col(); result_status = _Col(); sample_date = _Col()
class SiteConfig:
    key = _Col()

def _no_db():
    raise RuntimeError("no database in offline tests")

_mod("app")
_mod("app.config", MCL=20.0, SMS_THRESHOLD=16.0, RETEST_WINDOW_DAYS=60,
     OLDER_REPORT_LABEL=LABEL, OLDER_REPORT_SHORT=SHORT,
     OLDER_REPORT_MIN_AGE_DAYS=MIN_AGE, STREET_SUFFIXES=_config_constant("STREET_SUFFIXES"),
     STREET_NAME_CORRECTIONS=_config_constant("STREET_NAME_CORRECTIONS"),
     STREET_NAME_DISPLAY_CORRECTIONS=_config_constant("STREET_NAME_DISPLAY_CORRECTIONS"),
     settings=settings)
_mod("app.database", get_db=lambda: None, SessionLocal=_no_db)
_mod("app.models")
_mod("app.models.results", PfasResult=PfasResult, SourceDiscoveryResult=SourceDiscoveryResult)
_mod("app.models.site_config", SiteConfig=SiteConfig)
_mod("app.models.users", User=object, Subscription=object)
_mod("app.geo")
_mod("app.geo.parcel_lookup",
     lookup_parcel=lambda m, p: (41.28, -70.05), parcel_street_name=lambda m, p: None)
_mod("app.routers")
_mod("app.notifications")
_mod("app.scrapers")
CALLS = []
_mod("app.scrapers.laserfiche",
     run_laserfiche_scraper=lambda **kw: (CALLS.append(kw), {
         "status": "success", "new_docs_found": 0, "new_docs_parsed": 0,
         "parse_errors": 0, "skipped_seen": 0, "errors": []})[1])

def _load(name, rel):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod

_load("app.geo.street_names", "app/geo/street_names.py")
api = _load("app.routers.api", "app/routers/api.py")
email = _load("app.notifications.email", "app/notifications/email.py")
sms = _load("app.notifications.sms", "app/notifications/sms.py")
_load("app.scrapers.start_guard", "app/scrapers/start_guard.py")
runner = _load("app.scrapers.run_laserfiche", "app/scrapers/run_laserfiche.py")


# --- Fake results ---
FOUND = datetime(2026, 11, 15, 16, 0, tzinfo=timezone.utc)

def ago(days):
    return FOUND.date() - timedelta(days=days)

class LF:
    def __init__(self, doc_id, street, age_days, catchup_flag=False, pfas6=25.0, status="DETECT"):
        self.id = doc_id; self.laserfiche_doc_id = doc_id; self.street_name = street
        self.sample_date = ago(age_days) if age_days is not None else None
        self.discovered_at = FOUND
        self.older_report = catchup_flag  # legacy column: must be ignored
        self.pfas6_sum = pfas6; self.result_status = status; self.neighborhood = "Polpis"
        self.map_number = "21"; self.parcel_number = "1"; self.parcel_override = None
        self.j_qualifier_present = False; self.hidden = False

class SD:
    older_report = True  # must be ignored: MassDEP results are never labelled
    def __init__(self):
        self.id = 7; self.sample_location = "Polpis Rd well"; self.source_doc_url = "https://example.test/a.pdf#1"
        self.sample_date = ago(2000); self.discovered_at = FOUND
        self.pfas6_sum = 25.0; self.result_status = "DETECT"
        self.neighborhood = "Polpis"; self.latitude = 41.3; self.longitude = -70.0

def want(r):
    return LABEL.format(date=r.sample_date.strftime("%b %d, %Y"))

failures = 0
def check(name, cond, detail=""):
    global failures
    print(("PASS " if cond else "FAIL ") + name + (f"  ({detail})" if detail and not cond else ""))
    failures += 0 if cond else 1


# 1. Age rule
check("1a cutoff is 90 days", MIN_AGE == 90, MIN_AGE)
d91, d90, d89 = LF(1, "A", MIN_AGE + 1), LF(2, "B", MIN_AGE), LF(3, "C", MIN_AGE - 1)
check("1b 91 days: labelled with its sample date", api.older_report_label(d91) == want(d91),
      api.older_report_label(d91))
check("1c 90 days: no label", api.older_report_label(d90) is None)
check("1d 89 days: no label", api.older_report_label(d89) is None)
check("1e no sample date: no label", api.older_report_label(LF(4, "D", None)) is None)
nodisc = LF(5, "E", 400); nodisc.discovered_at = None
check("1f no discovery time: no label", api.older_report_label(nodisc) is None)
check("1g catch-up result only 30 days old: no label",
      api.older_report_label(LF(6, "F", 30, catchup_flag=True)) is None)
normal_old = LF(7, "G", 120, catchup_flag=False)
check("1h normal-run result 120 days old: labelled", api.older_report_label(normal_old) == want(normal_old))
may26 = LF(412666, "Sesachacha Road", None); may26.sample_date = date(2026, 5, 26)
check("1i wording matches FINISH_PLAN",
      api.older_report_label(may26) == LABEL.format(date="May 26, 2026")
      and LABEL.format(date="May 26, 2026").startswith("Older report, newly added: sampled May 26, 2026."))

# 2. Map: no label anywhere
class FakeQuery:
    def __init__(self, rows): self.rows = rows
    def filter(self, *a, **k): return self
    def all(self): return self.rows
    def first(self): return None
class FakeDB:
    def query(self, model):
        return FakeQuery({PfasResult: [LF(10, "Old Road", 200, True), LF(11, "New Lane", 30)],
                          SourceDiscoveryResult: [SD()]}.get(model, []))

api_rows = api.get_results(neighborhood=None, status=None, days=None, db=FakeDB())
check("2a map API returns rows", len(api_rows) == 3, len(api_rows))
check("2b map API has no label fields",
      all("older_report" not in r and "older_report_label" not in r for r in api_rows))
tpl = (ROOT / "app/templates/map.html").read_text()
check("2c popup template has no label", "older_report" not in tpl and "popup-older" not in tpl)

# 3. Digest email
old, new = LF(20, "Old Road", 120, catchup_flag=False), LF(21, "New Lane", 30, catchup_flag=True)
rows = [email._result_to_email_row(old, "laserfiche"), email._result_to_email_row(new, "laserfiche"),
        email._result_to_email_row(SD(), "massdep")]
html = email._build_digest_html("Polpis", rows, "https://u", "https://m")
check("3a digest shows the label once", html.count("Older report, newly added") == 1)
i_label, i_old, i_new = html.find("Older report"), html.find("Old Road"), html.find("New Lane")
check("3b label sits on the old row", i_old < i_label and (i_new < i_old or i_label < i_new),
      f"old={i_old} label={i_label} new={i_new}")
check("3c recent catch-up row and MassDEP row unlabelled",
      rows[1]["older_report_label"] is None and rows[2]["older_report_label"] is None)

# 4. SMS (through send_sms_alert, Twilio not configured, so nothing is sent)
class U: mobile = "+15555550100"
def sms_text(result, source):
    seen = []
    real = sms._build_sms_message
    sms._build_sms_message = lambda *a, **k: (seen.append(real(*a, **k)), seen[-1])[1]
    try:
        sms.send_sms_alert(U(), result, source)
    finally:
        sms._build_sms_message = real
    return seen[0]

m_old, m_new, m_sd = sms_text(old, "laserfiche"), sms_text(new, "laserfiche"), sms_text(SD(), "massdep")
check("4a SMS for an old report says so", f"{SHORT}, sampled" in m_old, m_old)
check("4b SMS for a recent catch-up result is unchanged", "Sample date" in m_new and SHORT not in m_new, m_new)
check("4c SMS for MassDEP is unchanged", "Sample date" in m_sd and SHORT not in m_sd, m_sd)
check("4d SMS stays within 160 chars", all(len(m) <= 160 for m in (m_old, m_new, m_sd)))

# 5. Runner flag: still starts a catch-up run, no longer labels
def run(argv, env_flag):
    CALLS.clear(); settings.catchup_run = env_flag
    sys.argv = ["run_laserfiche"] + argv
    try:
        runner.main()
    except SystemExit:
        pass
    return CALLS[0]["catchup"]

check("5a a normal (forced) run is not a catch-up", run(["--force"], False) is False)
check("5b --catchup makes a catch-up run", run(["--catchup"], False) is True)
check("5c CATCHUP_RUN=1 makes a catch-up run", run([], True) is True)
src = (ROOT / "app/scrapers/laserfiche.py").read_text()
check("5d scraper records catchup on the run", "catchup=catchup," in src)
check("5e scraper no longer sets older_report on results", "older_report=" not in src)

print()
if failures:
    print(f"{failures} FAILED")
    sys.exit(1)
print("ALL PASS")
