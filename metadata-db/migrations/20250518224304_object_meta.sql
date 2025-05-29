ALTER TABLE file_metadata ADD COLUMN IF NOT EXISTS object_size BIGINT;
ALTER TABLE file_metadata ADD COLUMN IF NOT EXISTS object_e_tag TEXT;
ALTER TABLE file_metadata ADD COLUMN IF NOT EXISTS object_version TEXT;