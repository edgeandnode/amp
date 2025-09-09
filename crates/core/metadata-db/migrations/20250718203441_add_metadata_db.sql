-- Add migration script here
ALTER TABLE file_metadata ADD COLUMN IF NOT EXISTS footer BYTEA;

DELETE FROM file_metadata WHERE footer IS NULL;

ALTER TABLE file_metadata ALTER COLUMN footer SET NOT NULL;