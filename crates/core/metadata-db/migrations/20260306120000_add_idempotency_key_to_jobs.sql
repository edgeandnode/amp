-- Add idempotency_key column to jobs table for deduplication
ALTER TABLE jobs ADD COLUMN idempotency_key TEXT;

-- Backfill existing rows with a legacy prefix to satisfy NOT NULL constraint
UPDATE jobs SET idempotency_key = 'legacy:' || id::TEXT;

-- Make the column non-nullable after backfill
ALTER TABLE jobs ALTER COLUMN idempotency_key SET NOT NULL;

-- Add unique constraint for idempotency key deduplication
ALTER TABLE jobs ADD CONSTRAINT jobs_idempotency_key_unique UNIQUE (idempotency_key);

-- Backfill job_events detail from jobs.descriptor for the latest SCHEDULED event per job
UPDATE job_events je
SET detail = j.descriptor
FROM jobs j
WHERE je.job_id = j.id
  AND je.event_type = 'SCHEDULED'
  AND je.detail IS NULL
  AND j.descriptor IS NOT NULL
  AND je.id = (
    SELECT id FROM job_events
    WHERE job_id = j.id AND event_type = 'SCHEDULED'
    ORDER BY id DESC
    LIMIT 1
  );


-- Make descriptor nullable (it is stored in job_events.detail, not directly in jobs)
ALTER TABLE jobs ALTER COLUMN descriptor DROP NOT NULL;
