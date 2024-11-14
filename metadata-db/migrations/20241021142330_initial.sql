CREATE TABLE IF NOT EXISTS locations (
    vid BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    created_at TIMESTAMP DEFAULT (now() AT TIME ZONE 'utc') NOT NULL,

    dataset TEXT NOT NULL,
    dataset_version TEXT NOT NULL,
    view TEXT NOT NULL,

    bucket TEXT,
    path TEXT NOT NULL,
    url TEXT NOT NULL UNIQUE,

    active BOOLEAN NOT NULL,

    CONSTRAINT unique_bucket_path UNIQUE (bucket, path)
);

-- Partial index to ensure only one active row per (dataset, dataset_version, view)
CREATE UNIQUE INDEX unique_active_per_dataset_version_view
ON locations (dataset, dataset_version, view)
WHERE active;
