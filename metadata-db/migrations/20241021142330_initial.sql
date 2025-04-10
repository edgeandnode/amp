CREATE TABLE workers (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    node_id TEXT UNIQUE NOT NULL,
    last_heartbeat TIMESTAMP NOT NULL
);

CREATE TABLE operators (
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
    writer BIGINT REFERENCES operators(id) ON DELETE SET NULL,
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
    -- Should we break this out into a separate table, separate columns, or keep it as JSONB?
    object_metadata JSONB,
    -- Should we break this out into a separate table, separate columns, or keep it as JSONB?
    nozzle_metadata JSONB NOT NULL
);

CREATE UNIQUE INDEX unique_file_name_per_location_id
ON file_metadata (location_id, file_name);

CREATE UNIQUE INDEX unique_range_boundaries_per_dataset_version_table
ON file_metadata (location_id, (nozzle_metadata->>'range_start'), (nozzle_metadata->>'range_end'));

CREATE TYPE NOZZLE_TIMESTAMP AS (
    secs    BIGINT,
    nanos   INTEGER
);

CREATE OR REPLACE FUNCTION as_nozzle_ts(ts TIMESTAMP) RETURNS NOZZLE_TIMESTAMP AS $$
DECLARE
    secs BIGINT;
    nanos INTEGER;
BEGIN
    secs := EXTRACT(EPOCH FROM ts)::BIGINT;
    nanos := ((EXTRACT(EPOCH FROM ts)::DOUBLE PRECISION - secs) * 1000 * 1000 * 1000)::INTEGER;
    RETURN ROW(secs, nanos);
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS file_metadata_v2 (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    location_id     BIGINT REFERENCES locations(id) ON DELETE CASCADE NOT NULL,
    file_name       TEXT NOT NULL,
    range_start     BIGINT NOT NULL,
    range_end       BIGINT NOT NULL,
    file_size       BIGINT NOT NULL,
    data_size       BIGINT NOT NULL,
    metadata_hint   BIGINT NOT NULL,
    etag            TEXT,
    version         TEXT,
    created_at      NOZZLE_TIMESTAMP DEFAULT (as_nozzle_ts(now() AT TIME ZONE 'utc')) NOT NULL,
    last_modified   NOZZLE_TIMESTAMP NOT NULL
);

CREATE FUNCTION default_last_modified() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.last_modified IS NULL THEN
        NEW.last_modified := ROW((NEW.created_at).secs, (NEW.created_at).nanos);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_last_modified
AFTER INSERT OR UPDATE ON file_metadata_v2
FOR EACH ROW
    EXECUTE PROCEDURE default_last_modified();

CREATE UNIQUE INDEX unique_file_name_per_location_id_v2
ON file_metadata_v2 (location_id, file_name);

CREATE UNIQUE INDEX unique_range_boundaries_per_dataset_version_table_v2
ON file_metadata_v2 (location_id, range_start, range_end);