"""S-1: refuse to start on Railway when a needed secret is unset or a placeholder.

Each service calls enforce_secrets(<service>) first thing at startup. It always
writes one line to stderr saying whether it is enforcing (on Railway) or only
warning (local development), and which settings it checks, so the Railway logs
show it working on every service. Values are never printed.

On Railway, a missing or placeholder value stops the service (exit 1): web fails
its /health check and Railway keeps the previous deployment; a cron exits with an
error before doing any work.
"""

import os
import sys
from typing import Mapping, Optional

from app.config import (
    PLACEHOLDER_SECRET_VALUES,
    SECRET_GUARD_ENFORCE_ENV_VAR,
    SECRET_GUARD_REQUIRED,
)

_PLACEHOLDERS = {v.strip().lower() for v in PLACEHOLDER_SECRET_VALUES}


def insecure_settings(settings, names) -> list[str]:
    """Names (upper-case env var form) whose value is unset or a placeholder."""
    bad = []
    for name in names:
        value = getattr(settings, name, None)
        if value is None or str(value).strip().lower() in _PLACEHOLDERS:
            bad.append(name.upper())
    return bad


def enforce_secrets(service: str, settings=None, environ: Optional[Mapping] = None,
                    out=None) -> list[str]:
    """Check this service's secrets; exit on Railway if any is unusable.

    Returns the list of unusable names (only when not exiting).
    """
    if settings is None:
        from app.config import settings as app_settings
        settings = app_settings
    environ = os.environ if environ is None else environ
    out = sys.stderr if out is None else out

    required = SECRET_GUARD_REQUIRED[service]
    env_name = environ.get(SECRET_GUARD_ENFORCE_ENV_VAR)
    enforcing = bool(env_name)
    bad = insecure_settings(settings, required)

    mode = f"enforcing (Railway environment {env_name})" if enforcing else \
        f"warning only ({SECRET_GUARD_ENFORCE_ENV_VAR} not set: local development)"
    checked = ", ".join(n.upper() for n in required) or "nothing (not needed)"
    status = "ok" if not bad else "UNSET OR PLACEHOLDER: " + ", ".join(bad)
    print(f"Secret guard [{service}]: {mode}; checks {checked}: {status}", file=out, flush=True)

    if bad and enforcing:
        print(f"Secret guard [{service}]: refusing to start. Set a real value for "
              f"{', '.join(bad)} on this service.", file=out, flush=True)
        raise SystemExit(1)
    return bad
