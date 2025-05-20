-- Add status column to jobs table. See JobStatus enum
ALTER TABLE jobs
ADD COLUMN status TEXT NOT NULL DEFAULT 'SCHEDULED'; 
