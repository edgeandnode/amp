-- Add retry_count column to jobs table for automatic retry functionality
ALTER TABLE jobs
ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0;
