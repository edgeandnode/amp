SET bytea_output = 'hex';

DROP INDEX unique_range_boundaries_per_dataset_version_table;

ALTER TABLE file_metadata 
    DROP COLUMN IF EXISTS metadata;

ALTER TABLE file_metadata
    ADD COLUMN IF NOT EXISTS metadata BYTEA;
DELETE FROM file_metadata WHERE metadata IS NULL;

ALTER TABLE file_metadata
    ALTER COLUMN metadata SET NOT NULL;

ALTER TABLE file_metadata
    ADD COLUMN IF NOT EXISTS metadata_hash BYTEA GENERATED ALWAYS AS (sha512(metadata)) STORED;