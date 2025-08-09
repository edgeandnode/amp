CREATE TABLE IF NOT EXISTS registry (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset TEXT NOT NULL,
    version TEXT NOT NULL,
    manifest TEXT NOT NULL,
    owner TEXT NOT NULL,
    UNIQUE(dataset, version)
);