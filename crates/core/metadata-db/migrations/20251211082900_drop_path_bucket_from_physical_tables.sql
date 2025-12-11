-- Drop path and bucket columns from physical_tables
-- These columns were write-only and not used in production code

-- Drop the unique constraint first
ALTER TABLE physical_tables DROP CONSTRAINT IF EXISTS unique_bucket_path;

-- Drop the columns
ALTER TABLE physical_tables DROP COLUMN IF EXISTS path;
ALTER TABLE physical_tables DROP COLUMN IF EXISTS bucket;
