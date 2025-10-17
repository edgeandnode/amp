-- Drop the old registry table
DROP TABLE IF EXISTS registry;

-- Create manifests table for storing dataset manifest information
-- Each manifest is identified by a SHA256 hash (lowercase hex, 64 chars, no 0x prefix)
CREATE TABLE IF NOT EXISTS manifest_files (
    hash TEXT PRIMARY KEY,
    path TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc')
);

-- Create dataset_manifests junction table for many-to-many relationship
-- Links manifests to datasets, allowing one manifest to belong to multiple datasets
CREATE TABLE IF NOT EXISTS dataset_manifests (
    namespace TEXT NOT NULL,
    name TEXT NOT NULL,
    hash TEXT NOT NULL REFERENCES manifest_files(hash) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
    PRIMARY KEY (namespace, name, hash)
);

-- Create tags table for versioning dataset manifests
-- Tags are optional - manifests can exist in datasets without tags
CREATE TABLE IF NOT EXISTS tags (
    namespace TEXT NOT NULL,
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
    PRIMARY KEY (namespace, name, version),
    FOREIGN KEY (namespace, name, hash) REFERENCES dataset_manifests(namespace, name, hash) ON DELETE CASCADE
);

-- Create indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_dataset_manifests_hash ON dataset_manifests(hash);
CREATE INDEX IF NOT EXISTS idx_dataset_manifests_dataset ON dataset_manifests(namespace, name);
CREATE INDEX IF NOT EXISTS idx_tags_dataset ON tags(namespace, name);
