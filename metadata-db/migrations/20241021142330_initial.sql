CREATE TABLE workers (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    node_id TEXT UNIQUE NOT NULL,
    last_heartbeat TIMESTAMP NOT NULL
);

CREATE TABLE scheduled_operators (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    node_id TEXT NOT NULL REFERENCES workers(node_id),
    operator TEXT NOT NULL,
    CONSTRAINT unique_scheduled_operators UNIQUE (node_id, operator)
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
    locked_by BIGINT REFERENCES scheduled_operators(id) ON DELETE SET NULL,
    CONSTRAINT unique_bucket_path UNIQUE (bucket, path)
);

-- Partial index to ensure only one active row per (dataset, dataset_version, tbl)
CREATE UNIQUE INDEX unique_active_per_dataset_version_table ON locations (dataset, dataset_version, tbl)
WHERE
    active;
