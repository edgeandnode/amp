-- Drop the footer column from file_metadata
ALTER TABLE file_metadata DROP COLUMN footer;

-- Remove the foreign key constraint from footer_cache entirely
-- This allows us to delete from file_metadata eagerly during compaction
-- while keeping footer_cache entries until the Collector runs
ALTER TABLE footer_cache DROP CONSTRAINT footer_cache_file_id_fkey;

-- Remove the foreign key constraint from gc_manifest to file_metadata
-- This allows us to delete from file_metadata while keeping gc_manifest entries
-- for garbage collection tracking
ALTER TABLE gc_manifest DROP CONSTRAINT gc_manifest_file_id_fkey;

-- Remove the CHECK constraint on gc_manifest.expiration
-- This allows inserting entries with any expiration time
ALTER TABLE gc_manifest DROP CONSTRAINT gc_manifest_expiration_check;
