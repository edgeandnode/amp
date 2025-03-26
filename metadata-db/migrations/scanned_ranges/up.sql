CREATE TABLE scanned_ranges (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    location_id BIGINT REFERENCES locations(vid) ON DELETE CASCADE NOT NULL,
    file_name TEXT NOT NULL
    metadata JSONB NOT NULL
);

CREATE UNIQUE INDEX unique_file_name_per_location_id
ON scanned_ranges (location_id, file_name);

CREATE UNIQUE INDEX unique_range_boundaries_per_dataset_version_table
ON scanned_ranges (location_id, (metadata->>'range_start')::BIGINT, (metadata->>'range_end')::BIGINT);