-- Migration: Rename locations to physical_table and replace namespace/dataset/version with manifest_hash
--
-- This migration transforms the locations table to use manifest hashes for dataset identification.
-- The registry (manifest_files, dataset_manifests, tags) tables handle the mapping from
-- namespace/dataset/version to manifest hashes.
--
-- Changes:
-- 1. Drop old unique index on (dataset, dataset_version, tbl)
-- 2. Rename table: locations → physical_table
-- 3. Drop columns: dataset, dataset_version
-- 4. Add column: manifest_hash (TEXT)
-- 5. Rename column: tbl → table_name
-- 6. Create new unique index on (manifest_hash, table_name) WHERE active

-- Step 1: Drop the old partial unique index
DROP INDEX IF EXISTS unique_active_per_dataset_version_table;

-- Step 2: Rename the table
ALTER TABLE locations RENAME TO physical_tables;

-- Step 3: Drop the dataset and dataset_version columns
ALTER TABLE physical_tables DROP COLUMN dataset;
ALTER TABLE physical_tables DROP COLUMN dataset_version;

-- Step 4: Add the manifest_hash column
-- Using TEXT type to match the manifest_files.hash column (SHA256 hex string)
ALTER TABLE physical_tables ADD COLUMN manifest_hash TEXT NOT NULL;

-- Labels for the dataset name under which this was created
ALTER TABLE physical_tables ADD COLUMN dataset_namespace TEXT NOT NULL;
ALTER TABLE physical_tables ADD COLUMN dataset_name TEXT NOT NULL;

-- Step 5: Rename tbl column to table_name
ALTER TABLE physical_tables RENAME COLUMN tbl TO table_name;

-- Step 6: Create new partial unique index to ensure only one active row per (manifest_hash, table_name)
CREATE UNIQUE INDEX unique_active_per_manifest_table
ON physical_tables (manifest_hash, table_name)
WHERE active;
