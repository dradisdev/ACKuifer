"""Add nullable parcel_override column to pfas_results.
Additive, no backfill, safe to re-run (IF NOT EXISTS). Run against staging
first (DATABASE_URL=<staging-url> python3 migrations/003_add_parcel_override.py),
then production after validation.
"""
from sqlalchemy import text
from app.database import engine

print("Target engine URL:", engine.url)  # confirm host before trusting result
with engine.begin() as conn:
    conn.execute(text(
        "ALTER TABLE pfas_results "
        "ADD COLUMN IF NOT EXISTS parcel_override VARCHAR"
    ))
print("Done: parcel_override column present on pfas_results.")
