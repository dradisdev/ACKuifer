-- Migration: Add hidden column to result tables
-- Date: 2026-03-24
-- Description: Allows admins to hide individual records from the public map.

ALTER TABLE pfas_results ADD COLUMN IF NOT EXISTS hidden BOOLEAN DEFAULT FALSE;
ALTER TABLE source_discovery_results ADD COLUMN IF NOT EXISTS hidden BOOLEAN DEFAULT FALSE;
