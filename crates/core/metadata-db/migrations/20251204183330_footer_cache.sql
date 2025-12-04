
CREATE TABLE IF NOT EXISTS footer_cache (
    file_id BIGINT REFERENCES file_metadata(id) ON DELETE CASCADE NOT NULL,
    footer BYTEA NOT NULL,
    PRIMARY KEY (file_id)
);

INSERT INTO footer_cache (file_id, footer)
SELECT id, footer FROM file_metadata
WHERE footer IS NOT NULL;