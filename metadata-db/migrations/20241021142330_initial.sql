CREATE TABLE IF NOT EXISTS locations (
    vid BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    created_at TIMESTAMP DEFAULT (now() AT TIME ZONE 'utc') NOT NULL,
    dataset TEXT NOT NULL,
    dataset_version TEXT NOT NULL,
    view TEXT NOT NULL,
    location TEXT NOT NULL,
    active BOOLEAN NOT NULL,

    -- Unique constraint on (dataset, dataset_version, view, location)
    CONSTRAINT unique_dataset_version_view_location UNIQUE (dataset, dataset_version, view, location)
);

-- Partial index to ensure only one active row per (dataset, dataset_version, view)
CREATE UNIQUE INDEX unique_active_per_dataset_version_view
ON locations (dataset, dataset_version, view)
WHERE active;
