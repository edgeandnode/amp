-- =============================================================
-- Migration: Add detail column to jobs_status
-- =============================================================
-- Adds a JSONB detail column to jobs_status for storing
-- structured error information (error code, message, chain)
-- when jobs fail.
-- =============================================================

ALTER TABLE jobs_status ADD COLUMN IF NOT EXISTS detail JSONB;
