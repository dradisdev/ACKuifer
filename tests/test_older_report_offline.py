"""Offline test of the D-3b catch-up label ("older report, newly added"); fakes only.

Proves:
  1. A catch-up result gets the label, with its sample date; a normal result and
     a MassDEP result get none.
  2. The map API returns the label for a catch-up result and None otherwise.
  3. The digest email shows the label on the catch-up row only.
  4. The SMS for a catch-up result says so instead of "Sample date".
  5. --catchup or CATCHUP_RUN=1 makes the crawler run as a catch-up; neither
     means a normal run.
The label wording is read from the real app/config.py, so the test checks it too.
Run: python3 tests/test_older_report_offline.py
"""
import ast
import importlib.util
import logging
import sys
import types
from datetime import date
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
     OLDER_REPORT_LABEL=LABEL, OLDER_REPORT_SHORT=SHORT, settings=settings)
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

api = _load("app.routers.api", "app/routers/api.py")
email = _load("app.notifications.email", "app/notifications/email.py")
sms = _load("app.notifications.sms", "app/notifications/sms.py")
_load("app.scrapers.start_guard", "app/scrapers/start_guard.py")
runner = _load("app.scrapers.run_laserfiche", "app/scrapers/run_laserfiche.py")


# --- Fake results ---
class LF:
    def __init__(self, doc_id, street, older, pfas6=0.0, status="NON-DETECT"):
        self.id = doc_id; self.laserfiche_doc_id = doc_id; self.street_name = street
        self.older_report = older; self.sample_date = date(2026, 5, 26)
        self.pfas6_sum = pfas6; self.result_status = status; self.neighborhood = "Polpis"
        self.map_number = "21"; self.parcel_number = "1"; self.parcel_override = None
        self.j_qualifier_present = False; self.hidden = False

class SD:
    older_report = True  # must be ignored: only Laserfiche results are labelled
    def __init__(self):
        self.id = 7; self.sample_location = "Polpis Rd well"; self.source_doc_url = "https://example.test/a.pdf#1"
        self.sample_date = date(2026, 5, 26); self.pfas6_sum = 0.0; self.result_status = "NON-DETECT"
        self.neighborhood = "Polpis"; self.latitude = 41.3; self.longitude = -70.0

old = LF(412666, "Catchup Road", older=True)
new = LF(500001, "Normal Lane", older=False)
WANT = LABEL.format(date="May 26, 2026")

failures = 0
def check(name, cond, detail=""):
    global failures
    print(("PASS " if cond else "FAIL ") + name + (f"  ({detail})" if detail and not cond else ""))
    failures += 0 if cond else 1


# 1. Label helper
check("1a catch-up result is labelled with its sample date", api.older_report_label(old) == WANT,
      api.older_report_label(old))
check("1b normal result has no label", api.older_report_label(new) is None)
check("1c wording matches FINISH_PLAN", WANT.startswith("Older report, newly added: sampled May 26, 2026."), WANT)

# 2. Map API
class FakeQuery:
    def __init__(self, rows): self.rows = rows
    def filter(self, *a, **k): return self
    def all(self): return self.rows
    def first(self): return None
class FakeDB:
    def query(self, model):
        return FakeQuery({PfasResult: [old, new], SourceDiscoveryResult: [SD()]}.get(model, []))

out = {r["street_name"]: r for r in api.get_results(neighborhood=None, status=None, days=None, db=FakeDB())}
check("2a API labels the catch-up result",
      out["Catchup Road"]["older_report"] is True and out["Catchup Road"]["older_report_label"] == WANT)
check("2b API: normal result unlabelled",
      out["Normal Lane"]["older_report"] is False and out["Normal Lane"]["older_report_label"] is None)
sd_rows = [r for r in out.values() if r["source"] == "massdep"]
check("2c API: MassDEP result unlabelled", sd_rows and sd_rows[0]["older_report_label"] is None)

# 3. Digest email
rows = [email._result_to_email_row(old, "laserfiche"), email._result_to_email_row(new, "laserfiche"),
        email._result_to_email_row(SD(), "massdep")]
html = email._build_digest_html("Polpis", rows, "https://u", "https://m")
check("3a digest shows the label once", html.count("Older report, newly added: sampled May 26, 2026.") == 1)
idx_label, idx_old, idx_new = html.find("Older report"), html.find("Catchup Road"), html.find("Normal Lane")
check("3b label sits on the catch-up row", idx_old < idx_label and (idx_new < idx_old or idx_label < idx_new),
      f"old={idx_old} label={idx_label} new={idx_new}")
check("3c MassDEP row unlabelled", rows[2]["older_report_label"] is None)

# 4. SMS
m_old = sms._build_sms_message("Polpis", 25.0, "Catchup Road", date(2026, 5, 26), "https://d", older_report=True)
m_new = sms._build_sms_message("Polpis", 25.0, "Normal Lane", date(2026, 5, 26), "https://d")
check("4a SMS for catch-up result says older report", f"{SHORT}, sampled 05/26/26." in m_old, m_old)
check("4b SMS for normal result unchanged", "Sample date 05/26/26." in m_new and SHORT not in m_new, m_new)
check("4c SMS stays within 160 chars", len(m_old) <= 160 and len(m_new) <= 160)

# 5. Runner flag
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

# The scraper wires the flag into the run row and every inserted result.
src = (ROOT / "app/scrapers/laserfiche.py").read_text()
check("5d scraper records catchup on the run and older_report on results",
      "catchup=catchup," in src and 'older_report=bool(stats.get("catchup", False))' in src)

print()
if failures:
    print(f"{failures} FAILED")
    sys.exit(1)
print("ALL PASS")
