-- Break out footer from file_metadata into separate file_footer_cache table
-- and denormalize file_name to file_path
-- This migration:
-- 1. Creates a new table for footer data and migrates existing data
-- 2. Renames file_name to file_path and populates with full file paths

-- Create the new file_footer_cache table
CREATE TABLE file_footer_cache (
    file_id BIGINT PRIMARY KEY REFERENCES file_metadata(id) ON DELETE CASCADE,
    footer BYTEA NOT NULL
);

-- Populate the new table with existing footer data
INSERT INTO file_footer_cache (file_id, footer)
SELECT id, footer
FROM file_metadata
WHERE footer IS NOT NULL;

-- Drop the footer column from file_metadata
ALTER TABLE file_metadata DROP COLUMN footer;

-- Add new file_path column (with temporary default to allow NOT NULL)
ALTER TABLE file_metadata ADD COLUMN file_path TEXT NOT NULL DEFAULT '';

-- Populate file_path with full path by joining url from physical_tables with file_name
UPDATE file_metadata fm
SET file_path = RTRIM(pt.url, '/') || '/' || fm.file_name
FROM physical_tables pt
WHERE fm.location_id = pt.id;

-- Drop old unique index on (location_id, file_name)
DROP INDEX IF EXISTS unique_file_name_per_location_id;

-- Create new unique index on (location_id, file_path)
CREATE UNIQUE INDEX unique_file_path_per_location_id
ON file_metadata (location_id, file_path);

-- Drop the old file_name column
ALTER TABLE file_metadata DROP COLUMN file_name;

-- Drop the temporary default constraint
ALTER TABLE file_metadata ALTER COLUMN file_path DROP DEFAULT;

-- Update gc_manifest to use the new denormalized file_path from file_metadata
UPDATE gc_manifest gm
SET file_path = fm.file_path
FROM file_metadata fm
WHERE gm.file_id = fm.id;
