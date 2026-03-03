-- Drop obsolete unique index on file_metadata range boundaries
--
-- This index was created in the initial migration but has several issues:
-- 1. Uses incorrect JSON paths (metadata->>'range_start') that don't match
--    the actual nested structure (metadata->'ranges'->0->'numbers'->>'start')
-- 2. Contains only NULL values and provides no uniqueness enforcement
-- 3. Not used by any queries in the codebase (no performance benefit)
-- 4. Will become obsolete with multi-network support, where files contain
--    multiple block ranges from different networks
--
-- Actual uniqueness is enforced by the unique_file_name_per_location_id index,
-- which prevents duplicate file names within the same location. Since file names
-- are content-addressed and include block ranges, this already prevents duplicates.

DROP INDEX IF EXISTS unique_range_boundaries_per_dataset_version_table;
