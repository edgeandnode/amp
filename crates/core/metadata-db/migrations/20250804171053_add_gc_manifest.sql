create table gc_manifest (
    location_id BIGINT NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
    file_id     BIGINT NOT NULL PRIMARY KEY REFERENCES file_metadata(id) ON DELETE CASCADE,
    file_path   TEXT NOT NULL UNIQUE,
    expiration  TIMESTAMP NOT NULL CHECK (expiration > now()),
    UNIQUE (location_id, file_id)
);

CREATE INDEX idx_gc_manifest ON gc_manifest (location_id, expiration) INCLUDE (file_path);
