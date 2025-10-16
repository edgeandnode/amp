-- Drop the old registry table
DROP TABLE IF EXISTS registry;

-- Create manifests table for storing dataset manifest information
-- Each manifest is identified by a SHA256 hash (lowercase hex, 64 chars, no 0x prefix)
CREATE TABLE IF NOT EXISTS manifests (
    hash TEXT PRIMARY KEY,
    path TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc')
);

-- Create tags table for mapping dataset identifiers to manifests
-- Replaces the registry table with normalized schema
CREATE TABLE IF NOT EXISTS tags (
    namespace TEXT NOT NULL,
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    hash TEXT NOT NULL REFERENCES manifests(hash) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
    PRIMARY KEY (namespace, name, version)
);

-- Create index for efficient tag lookups by namespace and name
CREATE INDEX IF NOT EXISTS idx_tags_namespace_name ON tags(namespace, name);
