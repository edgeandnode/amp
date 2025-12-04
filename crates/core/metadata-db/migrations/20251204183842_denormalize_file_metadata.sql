-- Add url column to file_metadata table
ALTER TABLE file_metadata ADD COLUMN url TEXT;

-- Update url column based on url in physical_tables where location_id = physical_tables.id
UPDATE file_metadata
SET url = pt.url
FROM physical_tables pt
WHERE file_metadata.location_id = pt.id;

-- Update url column as NOT NULL
DELETE FROM file_metadata WHERE url IS NULL;
ALTER TABLE file_metadata ALTER COLUMN url SET NOT NULL;
