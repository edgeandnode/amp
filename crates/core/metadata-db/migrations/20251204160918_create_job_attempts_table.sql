-- Create job_attempts table to track each scheduling attempt
CREATE TABLE job_attempts (
    job_id BIGINT NOT NULL,
    retry_index INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
    completed_at TIMESTAMPTZ,
    
    PRIMARY KEY (job_id, retry_index),
    CONSTRAINT fk_job_attempts_job_id 
        FOREIGN KEY (job_id) 
        REFERENCES jobs(id) 
        ON DELETE CASCADE
);

-- Index for efficient queries by job_id
CREATE INDEX idx_job_attempts_job_id ON job_attempts(job_id);

COMMENT ON TABLE job_attempts IS 'Tracks each scheduling attempt for jobs, including retries';
COMMENT ON COLUMN job_attempts.job_id IS 'Foreign key to jobs table';
COMMENT ON COLUMN job_attempts.retry_index IS 'Attempt number: 0 = initial attempt, 1+ = retries';
COMMENT ON COLUMN job_attempts.created_at IS 'When this attempt was scheduled';
COMMENT ON COLUMN job_attempts.completed_at IS 'When this attempt completed (NULL if ongoing)';
