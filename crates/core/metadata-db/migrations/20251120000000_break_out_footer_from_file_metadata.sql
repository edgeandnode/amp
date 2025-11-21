-- Break out footer from file_metadata into separate file_footer_cache table
-- and denormalize file_name to file_path
-- This migration:
-- 1. Creates a new table for footer data and migrates existing data
-- 2. Renames file_name to file_path and populates with full file paths

-- Create the new file_footer_cache table
CREATE TABLE IF NOT EXISTS file_footer_cache (
    file_id BIGINT PRIMARY KEY REFERENCES file_metadata(id) ON DELETE CASCADE,
    footer BYTEA NOT NULL
);

-- Populate the new table with existing footer data
INSERT INTO file_footer_cache (file_id, footer)
SELECT id, footer
FROM file_metadata;

-- Add new file_path column (with temporary default to allow NOT NULL)
ALTER TABLE file_metadata ADD COLUMN IF NOT EXISTS file_path TEXT NOT NULL DEFAULT '';

-- Populate file_path with full path by joining url from physical_tables with file_name
UPDATE file_metadata fm
SET file_path = RTRIM(pt.url, '/') || '/' || fm.file_name
FROM physical_tables pt
WHERE fm.location_id = pt.id;

-- Create new unique index on (location_id, file_path)
CREATE UNIQUE INDEX IF NOT EXISTS unique_file_path_per_location_id
ON file_metadata (location_id, file_path);

-- Drop the temporary default constraint
ALTER TABLE file_metadata ALTER COLUMN file_path DROP DEFAULT;

-- Create `gc_manifest.file_name` column (with temporary default to allow NOT NULL)
ALTER TABLE gc_manifest
    ADD COLUMN IF NOT EXISTS file_name TEXT NOT NULL DEFAULT '';

-- Move gc_manifest.file_path (which currently stores file_name values) to gc_manifest.file_name
-- Note: gc_manifest.file_path is currently storing file_name values (see 20250804171053_add_gc_manifest.sql)
UPDATE gc_manifest
SET file_name = file_path;

-- Drop the temporary default constraint
ALTER TABLE gc_manifest ALTER COLUMN file_name DROP DEFAULT;

-- Update gc_manifest.file_path to use the new denormalized file_path from file_metadata
UPDATE gc_manifest gm
SET file_path = fm.file_path
FROM file_metadata fm
WHERE gm.file_id = fm.id;
