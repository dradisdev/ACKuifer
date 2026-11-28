"""Offline test of the privacy page and its links (F-3). No database, no network.

Proves:
  1. GET /privacy renders, with every section, the contact address and the last-updated date
  2. every public page template links to the privacy page (admin pages don't)
  3. the digest, confirmation and reconfirmation emails link to BASE_URL + the privacy path
Run: python3 tests/test_privacy_offline.py
"""
import os
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
os.chdir(ROOT)  # the routers load templates from app/templates
sys.path.insert(0, str(ROOT))

# A dummy database URL: the engine is created lazily and never connects here.
os.environ["DATABASE_URL"] = "postgresql://offline:offline@127.0.0.1:1/offline"
os.environ.pop("RAILWAY_ENVIRONMENT_NAME", None)

import logging
logging.disable(logging.CRITICAL)

from fastapi.testclient import TestClient  # noqa: E402

from app.config import CONTACT_EMAIL, PRIVACY_LAST_UPDATED, PRIVACY_PATH, settings  # noqa: E402
from app.main import app  # noqa: E402
from app.notifications import email  # noqa: E402
from app.routers import map as map_router, signup as signup_router  # noqa: E402

failures = []


def check(name, ok, detail=""):
    print(("PASS" if ok else "FAIL") + f": {name}" + (f" ({detail})" if detail and not ok else ""))
    if not ok:
        failures.append(name)


# 1. The page itself
resp = TestClient(app).get(PRIVACY_PATH)
check("GET /privacy returns 200", resp.status_code == 200, resp.status_code)
page = resp.text
for heading in ("What we collect", "What we use it for", "Who handles it", "Cookies",
                "Unsubscribing", "Questions"):
    check(f"privacy page has section '{heading}'", f"<h3>{heading}</h3>" in page)
check("privacy page says neighborhoods, not streets",
      "neighborhoods you chose" in page and "street names" not in page)
check("privacy page names Plausible and feedback", "Plausible" in page and "feedback" in page)
check("privacy page shows the contact address", f'mailto:{CONTACT_EMAIL}' in page)
check("privacy page shows the last-updated date", PRIVACY_LAST_UPDATED in page)

# 2. Every public page links to it. New templates count as public unless listed here.
NOT_PUBLIC = {"admin.html", "admin_login.html", "index-old.html", "privacy.html"}
public = sorted(p.name for p in (ROOT / "app/templates").glob("*.html") if p.name not in NOT_PUBLIC)
check("found the 8 public templates", len(public) == 8, public)
link = f'href="{PRIVACY_PATH}"'
for env_name, templates in (("map", map_router.templates), ("signup", signup_router.templates)):
    for name in public:
        html = templates.get_template(name).render(request=None)
        check(f"{name} links to privacy ({env_name} router)", link in html)
for name in ("admin.html", "admin_login.html"):
    html = (ROOT / "app/templates" / name).read_text()
    check(f"{name} has no privacy link (admin)", "privacy_path" not in html)

# 3. Emails
email_link = f'href="{settings.base_url}{PRIVACY_PATH}"'
digest = email._build_digest_html("Madaket", [], "https://u.test/unsub", "https://u.test/manage")
check("digest email links to privacy", email_link in digest)
reconfirm = email._build_reconfirmation_html("a@example.com", "https://u.test/re", "https://u.test/unsub")
check("reconfirmation email links to privacy", email_link in reconfirm)

sent = []
email.resend.Emails.send = lambda params: sent.append(params) or {"id": "fake"}
settings.resend_api_key = "offline-test-key"
user = types.SimpleNamespace(id="00000000-0000-0000-0000-000000000001", email="a@example.com",
                             subscriptions=[types.SimpleNamespace(neighborhood="Madaket")])
ok = email.send_confirmation_email(user, db=None)
check("confirmation email sent (fake)", ok and len(sent) == 1)
check("confirmation email links to privacy", bool(sent) and email_link in sent[0]["html"])

print(f"\n{len(failures)} failed" if failures else "\nAll privacy checks passed")
sys.exit(1 if failures else 0)
