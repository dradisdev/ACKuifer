"""S-3: the one identity check for scripts that take --expect-env.

The real identity is the server address plus the database name. The row count is
only a wide sanity range that catches an empty or freshly restored database, so
normal growth (such as the Dec 4 catch-up) never makes the guard refuse.

The address is the one seen through Railway's public proxy, so a guarded script
run from inside Railway (private network, IPv6) refuses. That is deliberate:
these scripts run from the operator's laptop.

Works with a SQLAlchemy Session or Connection, or a psycopg2 cursor.
"""

import sys
from typing import Optional

from app.config import DB_ENV_FINGERPRINTS, DB_ENV_ROW_RANGES, DB_IDENTITY_NAME

_IDENTITY_SQL = (
    "SELECT host(inet_server_addr()), current_database(), "
    "(SELECT count(*) FROM pfas_results), current_setting('transaction_read_only')"
)


def identity_problem(expect_env: str, addr: Optional[str], name: Optional[str],
                     rows: Optional[int]) -> Optional[str]:
    """Return why this is not the named environment, or None when it is."""
    expected_addr = DB_ENV_FINGERPRINTS[expect_env]
    low, high = DB_ENV_ROW_RANGES[expect_env]
    if addr != expected_addr:
        return f"server address {addr} is not {expect_env} ({expected_addr})"
    if name != DB_IDENTITY_NAME:
        return f"database {name} is not {DB_IDENTITY_NAME}"
    if rows is None or not low <= rows <= high:
        return f"pfas_results has {rows} rows, outside the sanity range {low}-{high}"
    return None


def _fetch_identity(target) -> tuple:
    if hasattr(target, "fetchone"):  # psycopg2 cursor
        target.execute(_IDENTITY_SQL)
        row = target.fetchone()
        return tuple(row.values()) if hasattr(row, "values") else tuple(row)
    from sqlalchemy import text
    return tuple(target.execute(text(_IDENTITY_SQL)).one())


def verify_db_identity(target, expect_env: str, read_only: bool = False, out=None) -> dict:
    """Print the connected DB's identity; exit unless it is expect_env.

    With read_only=True, also exit unless the session is read-only.
    """
    out = sys.stdout if out is None else out
    addr, name, rows, session_read_only = _fetch_identity(target)
    print(f"=== DB IDENTITY === expect-env={expect_env} addr={addr} db={name} "
          f"pfas_results={rows} read-only={session_read_only}", file=out, flush=True)
    problem = identity_problem(expect_env, addr, name, rows)
    if problem:
        raise SystemExit(f"ABORT: {problem}. Refusing to run; nothing done.")
    if read_only and session_read_only != "on":
        raise SystemExit("ABORT: this session is not read-only. Refusing to run; nothing done.")
    print(f"    identity OK: {expect_env}", file=out, flush=True)
    return {"addr": addr, "db": name, "rows": rows, "read_only": session_read_only}
