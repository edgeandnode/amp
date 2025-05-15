CREATE TABLE workers (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    node_id TEXT UNIQUE NOT NULL,
    last_heartbeat TIMESTAMP NOT NULL
);

CREATE TABLE jobs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    node_id TEXT NOT NULL REFERENCES workers(node_id),
    descriptor JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS locations (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    created_at TIMESTAMP DEFAULT (now() AT TIME ZONE 'utc') NOT NULL,
    dataset TEXT NOT NULL,
    dataset_version TEXT NOT NULL,
    tbl TEXT NOT NULL,
    bucket TEXT,
    path TEXT NOT NULL,
    url TEXT NOT NULL UNIQUE,

    active BOOLEAN NOT NULL,
    writer BIGINT REFERENCES jobs(id) ON DELETE SET NULL,
    CONSTRAINT unique_bucket_path UNIQUE (bucket, path)
);

-- Partial index to ensure only one active row per (dataset, dataset_version, tbl)
CREATE UNIQUE INDEX unique_active_per_dataset_version_table ON locations (dataset, dataset_version, tbl)
WHERE
    active;

CREATE TABLE IF NOT EXISTS file_metadata (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    location_id BIGINT REFERENCES locations(id) ON DELETE CASCADE NOT NULL,
    file_name TEXT NOT NULL,
    nozzle_meta JSONB NOT NULL,
    object_last_modified TIMESTAMP NOT NULL,
    object_size BIGINT NOT NULL,
    object_e_tag TEXT,
    object_version TEXT
);

CREATE UNIQUE INDEX unique_range_boundaries_per_dataset_version_table
ON file_metadata (location_id, (nozzle_meta->>'range_start'), (nozzle_meta->>'range_end'));