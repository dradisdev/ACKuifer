"""S-3: the one identity check for scripts that take --expect-env.

The real identity is the public host the connection actually used (S-3b) plus
the database name. The host is Railway's TCP proxy for that environment; unlike
the server address, it survives a Postgres restart. The server address is still
printed, for the record, but decides nothing. The row count is only a wide
sanity range that catches an empty or freshly restored database, so normal
growth (such as the Dec 4 catch-up) never makes the guard refuse.

A guarded script run over Railway's private network uses another host, so it
refuses. That is deliberate: these scripts run from the operator's laptop.

Works with a SQLAlchemy Session or Connection, or a psycopg2 cursor.
"""

import sys
from typing import Optional

from app.config import DB_ENV_FINGERPRINTS, DB_ENV_ROW_RANGES, DB_IDENTITY_NAME

_IDENTITY_SQL = (
    "SELECT host(inet_server_addr()), current_database(), "
    "(SELECT count(*) FROM pfas_results), current_setting('transaction_read_only')"
)


def identity_problem(expect_env: str, host: Optional[str], name: Optional[str],
                     rows: Optional[int]) -> Optional[str]:
    """Return why this is not the named environment, or None when it is."""
    expected_host = DB_ENV_FINGERPRINTS[expect_env]
    low, high = DB_ENV_ROW_RANGES[expect_env]
    if (host or "").lower() != expected_host:
        return f"host {host} is not {expect_env} ({expected_host})"
    if name != DB_IDENTITY_NAME:
        return f"database {name} is not {DB_IDENTITY_NAME}"
    if rows is None or not low <= rows <= high:
        return f"pfas_results has {rows} rows, outside the sanity range {low}-{high}"
    return None


def _dbapi_connection(target):
    """The psycopg2 connection this target's queries actually run on."""
    if hasattr(target, "fetchone"):            # psycopg2 cursor
        return target.connection
    if hasattr(target, "get_bind"):            # SQLAlchemy Session
        target = target.connection()
    return target.connection.dbapi_connection  # SQLAlchemy Connection


def connection_host(target) -> Optional[str]:
    """Host the connection used (psycopg2 reports what it actually connected to)."""
    return _dbapi_connection(target).info.host


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
    host = connection_host(target)
    print(f"=== DB IDENTITY === expect-env={expect_env} host={host} addr={addr} db={name} "
          f"pfas_results={rows} read-only={session_read_only}", file=out, flush=True)
    problem = identity_problem(expect_env, host, name, rows)
    if problem:
        raise SystemExit(f"ABORT: {problem}. Refusing to run; nothing done.")
    if read_only and session_read_only != "on":
        raise SystemExit("ABORT: this session is not read-only. Refusing to run; nothing done.")
    print(f"    identity OK: {expect_env}", file=out, flush=True)
    return {"host": host, "addr": addr, "db": name, "rows": rows, "read_only": session_read_only}
