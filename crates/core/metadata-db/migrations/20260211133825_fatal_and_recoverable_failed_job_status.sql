-- Replace FAILED job statuses with FAILED_RECOVERABLE. The ones that should have been FAILED_FATAL will be marked
-- as such on the next retry.

UPDATE jobs SET status = 'FAILED_RECOVERABLE' WHERE status = 'FAILED';
