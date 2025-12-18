-- Rename url column to path in physical_tables and transform existing data
-- Extract the last 3 path segments from URLs (dataset-name/table-name/revision-uuid)

-- Pre-validation: fail if any URL has fewer than 3 path components
-- (DO block required because RAISE EXCEPTION is PL/pgSQL, not standard SQL)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM physical_tables
        WHERE array_length(string_to_array(rtrim(url, '/'), '/'), 1) < 3
    ) THEN
        RAISE EXCEPTION 'Found URLs with fewer than 3 path components';
    END IF;
END $$;

-- Update the data to extract last 3 path segments
-- For a URL like 's3://bucket/prefix/dataset-name/table-name/revision-uuid/',
-- this extracts 'dataset-name/table-name/revision-uuid' (without trailing slash)
UPDATE physical_tables
SET url = (regexp_match(rtrim(url, '/'), '([^/]+/[^/]+/[^/]+)$'))[1];

-- Then rename the column
-- The unique constraint name will be auto-updated by PostgreSQL
ALTER TABLE physical_tables RENAME COLUMN url TO path;
