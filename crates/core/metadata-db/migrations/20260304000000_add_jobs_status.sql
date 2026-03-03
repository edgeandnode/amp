-- =============================================================
-- Migration: Add jobs_status projection table
-- =============================================================
-- 1. Create jobs_status table (one row per job, latest state)
-- 2. Backfill from jobs table
-- 3. Add foreign key constraint
-- 4. Add indexes for common query patterns
-- =============================================================

-- 1. Create jobs_status table
CREATE TABLE IF NOT EXISTS jobs_status (
    job_id      BIGINT      NOT NULL,
    node_id     TEXT        NOT NULL,
    status      TEXT        NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT jobs_status_pkey PRIMARY KEY (job_id)
);

-- 2. Backfill from existing jobs
INSERT INTO jobs_status (job_id, node_id, status, updated_at)
SELECT id, node_id, status, updated_at
FROM jobs
ON CONFLICT (job_id) DO NOTHING;

-- 3. Add foreign key
ALTER TABLE jobs_status
    ADD CONSTRAINT fk_jobs_status_job_id
    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE;

-- 4. Add indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_jobs_status_status ON jobs_status (status);
CREATE INDEX IF NOT EXISTS idx_jobs_status_node_id_status ON jobs_status (node_id, status);
