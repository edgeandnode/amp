-- Drop redundant columns from jobs; 

-- status/node_id/updated_at live in jobs_status
ALTER TABLE jobs DROP COLUMN IF EXISTS status;
ALTER TABLE jobs DROP COLUMN IF EXISTS node_id;
ALTER TABLE jobs DROP COLUMN IF EXISTS updated_at;

-- descriptor lives in job_events.detail
ALTER TABLE jobs DROP COLUMN IF EXISTS descriptor;
