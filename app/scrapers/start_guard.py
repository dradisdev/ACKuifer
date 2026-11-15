"""N-11: tell a scheduled crawl start from a deploy start.

Railway runs the same start command for a cron trigger and for every deploy
(code push, variable change), so the process can't ask why it was started.
The crawler therefore goes by the clock: it runs only within a short window
after a time in its cron schedule, or when explicitly asked to (catch-up run,
force flag, single-map CLI run). Anything else is a deploy start, and the
crawler exits at once instead of starting a ~12h crawl.

The schedule lives in two places: the Railway dashboard (what actually fires)
and settings.laserfiche_cron_schedule (what this guard accepts). Keep them the
same. If they drift, scheduled runs are skipped and the dead man's switch
alerts on the missing run.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)


def _parse_field(field: str, lo: int, hi: int) -> Optional[set]:
    """Parse one cron field: '*', a number, or a comma list of numbers.
    Returns None for '*' (any value). Raises ValueError for anything else."""
    if field == "*":
        return None
    values = set()
    for part in field.split(","):
        v = int(part)  # ValueError on ranges/steps, which this guard doesn't support
        if not lo <= v <= hi:
            raise ValueError(f"{v} out of range {lo}-{hi}")
        values.add(v)
    return values


def _matches(values: Optional[set], v: int) -> bool:
    return values is None or v in values


def is_scheduled_start(cron: str, now: datetime, window_minutes: int) -> bool:
    """True if a time in `cron` (UTC, 5 fields) falls within the last
    `window_minutes` before `now`. Supports '*', numbers and comma lists.
    Raises ValueError if the expression is outside that subset."""
    fields = cron.split()
    if len(fields) != 5:
        raise ValueError(f"expected 5 cron fields, got {len(fields)}: {cron!r}")
    minute, hour, dom, month, dow = (
        _parse_field(fields[0], 0, 59),
        _parse_field(fields[1], 0, 23),
        _parse_field(fields[2], 1, 31),
        _parse_field(fields[3], 1, 12),
        _parse_field(fields[4], 0, 7),
    )
    if dow is not None:
        dow = {d % 7 for d in dow}  # cron allows 7 for Sunday

    now = now.astimezone(timezone.utc).replace(second=0, microsecond=0)
    for back in range(window_minutes + 1):
        t = now - timedelta(minutes=back)
        cron_dow = (t.weekday() + 1) % 7  # cron: Sunday=0
        if dom is not None and dow is not None:
            day_ok = t.day in dom or cron_dow in dow  # cron ORs the two day fields
        else:
            day_ok = _matches(dom, t.day) and _matches(dow, cron_dow)
        if _matches(minute, t.minute) and _matches(hour, t.hour) and day_ok and _matches(month, t.month):
            return True
    return False


def start_reason(
    *,
    cron: str,
    window_minutes: int,
    now: Optional[datetime] = None,
    catchup: bool = False,
    force: bool = False,
    map_filter: Optional[str] = None,
) -> Optional[str]:
    """Why this crawl should run, or None if it is a deploy start to skip."""
    if catchup:
        return "catch-up run"
    if force:
        return "forced run"
    if map_filter:
        return f"single-map run (map {map_filter})"
    now = now or datetime.now(timezone.utc)
    try:
        if is_scheduled_start(cron, now, window_minutes):
            return f"scheduled run ({cron})"
    except ValueError as e:
        # Fail open: an unreadable schedule must never silently stop crawls.
        logger.warning("Can't read laserfiche cron schedule %r (%s); running anyway", cron, e)
        return "schedule unreadable, running anyway"
    return None
