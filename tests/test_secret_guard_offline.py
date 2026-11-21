"""Offline test of the S-1 secret guard; no database, no network.

Proves:
  1. Unset, "changeme" (any case, with spaces) and the .env.example placeholders
     count as unusable; a real value passes.
  2. On Railway (RAILWAY_ENVIRONMENT_NAME set) an unusable value stops the
     service (SystemExit 1); without it, the guard only warns and carries on.
  3. web needs ADMIN_PASSWORD and SECRET_KEY; laserfiche-cron and massdep-cron
     need SECRET_KEY only; healthcheck-cron needs nothing and never stops.
  4. Every run prints exactly one status line saying "enforcing" or "warning
     only", the service name and what it checks, and never prints a value.
  5. web, laserfiche-cron, massdep-cron and healthcheck-cron call the guard with
     their own name first thing at startup (web before the routers load).
  6. app/config.py has no "changeme" default left.
Constants are read from the real app/config.py.
Run: python3 tests/test_secret_guard_offline.py
"""
import ast
import importlib.util
import io
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
for name in ("PLACEHOLDER_SECRET_VALUES", "SECRET_GUARD_ENFORCE_ENV_VAR", "SECRET_GUARD_REQUIRED"):
    setattr(cfg, name, _config_constant(name))
sys.modules["app"] = types.ModuleType("app")
sys.modules["app.config"] = cfg
spec = importlib.util.spec_from_file_location("app.secret_guard", ROOT / "app/secret_guard.py")
sg = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sg)

failures = 0
def check(name, cond, detail=""):
    global failures
    print(("PASS " if cond else "FAIL ") + name + (f"  ({detail})" if detail and not cond else ""))
    failures += 0 if cond else 1

class S:
    def __init__(self, admin_password="", secret_key=""):
        self.admin_password = admin_password
        self.secret_key = secret_key

REAL_PW, REAL_KEY = "correct-horse-battery", "k9f2-real-secret-value-x7"
RAILWAY = {"RAILWAY_ENVIRONMENT_NAME": "staging"}

def run(service, settings, environ):
    out = io.StringIO()
    try:
        bad = sg.enforce_secrets(service, settings=settings, environ=environ, out=out)
        return "ran", bad, out.getvalue()
    except SystemExit as e:
        return f"exit {e.code}", None, out.getvalue()

# 1. what counts as unusable
for value in ("", "   ", "changeme", "CHANGEME", " changeme ", "your_admin_password_here",
              "your_secret_key_here"):
    check(f"{value!r} is unusable", sg.insecure_settings(S(secret_key=value), ["secret_key"]) == ["SECRET_KEY"])
check("None is unusable", sg.insecure_settings(S(secret_key=None), ["secret_key"]) == ["SECRET_KEY"])
check("a real value passes", sg.insecure_settings(S(REAL_PW, REAL_KEY), ["admin_password", "secret_key"]) == [])

# 2 + 3. per service, on and off Railway
CASES = [
    # service, settings, environ, expected outcome
    ("web", S(REAL_PW, REAL_KEY), RAILWAY, "ran"),
    ("web", S("changeme", REAL_KEY), RAILWAY, "exit 1"),
    ("web", S(REAL_PW, ""), RAILWAY, "exit 1"),
    ("web", S("changeme", "changeme"), {}, "ran"),
    ("laserfiche-cron", S("", REAL_KEY), RAILWAY, "ran"),
    ("laserfiche-cron", S(REAL_PW, "changeme"), RAILWAY, "exit 1"),
    ("laserfiche-cron", S("", ""), {}, "ran"),
    ("massdep-cron", S("", REAL_KEY), RAILWAY, "ran"),
    ("massdep-cron", S(REAL_PW, "your_secret_key_here"), RAILWAY, "exit 1"),
    ("healthcheck-cron", S("", ""), RAILWAY, "ran"),
    ("healthcheck-cron", S("changeme", "changeme"), RAILWAY, "ran"),
]
for service, settings, environ, want in CASES:
    got, _, text = run(service, settings, environ)
    where = "Railway" if environ else "local"
    check(f"{service} {where} pw={settings.admin_password!r} key={settings.secret_key!r} -> {want}",
          got == want, f"got {got}")

got, bad, _ = run("web", S("changeme", ""), {})
check("locally an unusable value is reported, not fatal", got == "ran" and bad == ["ADMIN_PASSWORD", "SECRET_KEY"], bad)

# 4. the status line
_, _, text = run("web", S(REAL_PW, REAL_KEY), RAILWAY)
lines = text.strip().splitlines()
check("one status line when healthy", len(lines) == 1, lines)
check("status line says enforcing, environment, service and checks",
      "enforcing (Railway environment staging)" in text and "[web]" in text
      and "ADMIN_PASSWORD, SECRET_KEY: ok" in text, text)
_, _, text = run("massdep-cron", S("", REAL_KEY), {})
check("status line says warning only off Railway", "warning only" in text and "[massdep-cron]" in text
      and "checks SECRET_KEY: ok" in text, text)
_, _, text = run("healthcheck-cron", S(), RAILWAY)
check("healthcheck line says it checks nothing", "checks nothing (not needed): ok" in text, text)
_, _, text = run("web", S(REAL_PW, "changeme"), RAILWAY)
check("refusal names the setting", "refusing to start" in text and "SECRET_KEY" in text, text)
for svc, st in (("web", S(REAL_PW, REAL_KEY)), ("laserfiche-cron", S(REAL_PW, REAL_KEY))):
    _, _, text = run(svc, st, RAILWAY)
    check(f"{svc}: no secret value printed", REAL_PW not in text and REAL_KEY not in text, text)

# 5. wired into each entry point, first thing
def first_call_in_main(rel):
    tree = ast.parse((ROOT / rel).read_text())
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "main":
            stmt = node.body[0]
            if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call):
                c = stmt.value
                if getattr(c.func, "id", None) == "enforce_secrets":
                    return c.args[0].value
    return None

for rel, svc in (("app/scrapers/run_laserfiche.py", "laserfiche-cron"),
                 ("app/scrapers/run_massdep.py", "massdep-cron"),
                 ("app/scrapers/run_healthcheck.py", "healthcheck-cron")):
    got = first_call_in_main(rel)
    check(f"{rel}: main() starts with enforce_secrets({svc!r})", got == svc, got)

main_src = (ROOT / "app/main.py").read_text()
guard_at = main_src.find('enforce_secrets("web")')
routers_at = main_src.find("from app.routers import")
check("app/main.py calls enforce_secrets('web') before loading the routers",
      guard_at != -1 and routers_at != -1 and guard_at < routers_at)

# 6. no fail-open default left
check('app/config.py has no "changeme" default', '= "changeme"' not in CONFIG_SRC)
check("every service in SECRET_GUARD_REQUIRED is covered",
      set(cfg.SECRET_GUARD_REQUIRED) == {"web", "laserfiche-cron", "massdep-cron", "healthcheck-cron"})

if failures:
    print(f"\n{failures} FAILED")
    sys.exit(1)
print("\nALL PASS")
