CREATE TYPE gc_status_kind AS ENUM {
    'pending',
    'deleted',
}

CREATE TABLE IF NOT EXISTS file_leases {
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    gc_status gc_status_kind NOT NULL,
    location_id BIGINT NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
    file_path TEXT NOT NULL,
    created_at BIGINT GENERATED ALWAYS AS (CURRENT_TIMESTAMP) NOT NULL,
    expires_at TIMESTAMP CONSTRAINT file_leases_expired_at_check CHECK (
        (gc_status = 'deleted' AND expired_at IS NULL)
        OR (gc_status = 'pending' AND expired_at IS NOT NULL)
    ),
}

CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_file_id_per_location_pending
ON file_leases (location_id, file_path) 
INCLUDE (id, expires_at)
WHERE expires_at IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_file_id_per_location_deleted
ON file_leases (location_id, file_path) 
INCLUDE (id)
WHERE expires_at IS NULL;