"""D-3b: add the catch-up label columns.

    pfas_results.older_report  boolean NOT NULL DEFAULT false
    scrape_runs.catchup        boolean NOT NULL DEFAULT false

Additive, no backfill, safe to re-run (IF NOT EXISTS). Old code ignores the new
columns, so run this BEFORE deploying the code that uses them.

Staging first:
    set -a && . ~/.ackuifer/staging.env && set +a
    python3 migrations/004_add_older_report.py --expect-env staging

Production only after the staging rehearsal (override DATABASE_URL per command):
    DATABASE_URL="<prod url>" python3 migrations/004_add_older_report.py --expect-env prod

Add --dry-run to check identity and print the plan without writing.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text  # noqa: E402

from app.database import engine  # noqa: E402

# DB identity fingerprints (internal address, database name, pfas_results range).
# Refreshed 2026-11-13; see S-3 if the internal addresses change again.
FINGERPRINTS = {
    "staging": {"addr": "10.239.90.16", "db": "railway", "rows": (400, 700)},
    "prod": {"addr": "10.226.63.254", "db": "railway", "rows": (350, 700)},
}

STATEMENTS = [
    "ALTER TABLE pfas_results ADD COLUMN IF NOT EXISTS older_report BOOLEAN NOT NULL DEFAULT false",
    "ALTER TABLE scrape_runs ADD COLUMN IF NOT EXISTS catchup BOOLEAN NOT NULL DEFAULT false",
]

COLUMNS = [("pfas_results", "older_report"), ("scrape_runs", "catchup")]


def main() -> int:
    p = argparse.ArgumentParser(description="Add older_report / catchup columns (D-3b)")
    p.add_argument("--expect-env", required=True, choices=sorted(FINGERPRINTS))
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    fp = FINGERPRINTS[args.expect_env]

    with engine.connect() as conn:
        addr = str(conn.execute(text("SELECT inet_server_addr()")).scalar())
        name = conn.execute(text("SELECT current_database()")).scalar()
        rows = conn.execute(text("SELECT count(*) FROM pfas_results")).scalar()
    print(f"=== DB IDENTITY: addr={addr} db={name} pfas_results={rows} (expect {args.expect_env})")
    lo, hi = fp["rows"]
    if addr != fp["addr"] or name != fp["db"] or not (lo <= rows <= hi):
        sys.exit(f"ABORT: not {args.expect_env} (expected {fp['addr']}/{fp['db']}, {lo}-{hi} rows). Nothing done.")
    print(f"    identity OK: {args.expect_env}")

    if args.dry_run:
        print("Dry run; would execute:")
        for s in STATEMENTS:
            print("   ", s)
        return 0

    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '10s'"))
        for s in STATEMENTS:
            conn.execute(text(s))

    with engine.connect() as conn:
        for table, col in COLUMNS:
            found = conn.execute(text(
                "SELECT data_type, column_default FROM information_schema.columns "
                "WHERE table_name = :t AND column_name = :c"), {"t": table, "c": col}).first()
            if not found:
                sys.exit(f"ERROR: {table}.{col} missing after migration")
            print(f"    {table}.{col}: {found[0]} default {found[1]}")
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
