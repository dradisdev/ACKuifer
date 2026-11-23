"""Add nullable parcel_override column to pfas_results.

Additive, no backfill, safe to re-run (IF NOT EXISTS). Staging first, then
production after validation. --expect-env is required (S-3):

    python3 migrations/003_add_parcel_override.py --expect-env staging --dry-run
    python3 migrations/003_add_parcel_override.py --expect-env staging
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text  # noqa: E402

from app.config import DB_ENV_FINGERPRINTS  # noqa: E402
from app.database import engine  # noqa: E402
from app.db_identity import verify_db_identity  # noqa: E402

STATEMENT = "ALTER TABLE pfas_results ADD COLUMN IF NOT EXISTS parcel_override VARCHAR"


def main() -> int:
    p = argparse.ArgumentParser(description="Add parcel_override to pfas_results")
    p.add_argument("--expect-env", required=True, choices=sorted(DB_ENV_FINGERPRINTS))
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    with engine.connect() as conn:
        verify_db_identity(conn, args.expect_env)
    if args.dry_run:
        print("Dry run; would execute:\n   ", STATEMENT)
        return 0
    with engine.begin() as conn:
        conn.execute(text(STATEMENT))
    print("Done: parcel_override column present on pfas_results.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
