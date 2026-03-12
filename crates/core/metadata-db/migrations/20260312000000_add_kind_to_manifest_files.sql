-- =============================================================
-- Migration: Add kind column to manifest_files
-- =============================================================
-- Adds a TEXT kind column to manifest_files to store the dataset
-- kind (e.g., evm-rpc, derived, static) for each manifest file.
-- Defaults to 'unknown' for existing rows.
-- =============================================================

ALTER TABLE manifest_files ADD COLUMN kind TEXT NOT NULL DEFAULT 'unknown';
