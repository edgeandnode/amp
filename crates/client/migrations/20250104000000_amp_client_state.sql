-- Create table for amp-client transactional stream state storage
--
-- This table stores the persistent state for amp-client transactional streams,
-- enabling crash recovery and exactly-once semantics.
--
-- Each row represents the state of a single transactional stream, identified by stream_id.
CREATE TABLE amp_client_state (
    -- Unique identifier for the stream (e.g., query fingerprint or user-defined ID)
    stream_id TEXT PRIMARY KEY,

    -- Next transaction ID to be assigned (monotonically increasing)
    next_transaction_id BIGINT NOT NULL DEFAULT 0,

    -- Timestamp of last state update
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for monitoring stale streams
CREATE INDEX idx_amp_client_state_updated_at ON amp_client_state(updated_at);

-- Create table for buffer entries (watermarks)
--
-- Each watermark in the buffer is stored as a separate row for better query performance
-- and relational integrity.
CREATE TABLE amp_client_buffer_entries (
    -- Reference to the stream
    stream_id TEXT NOT NULL REFERENCES amp_client_state(stream_id) ON DELETE CASCADE,

    -- Transaction ID for this watermark
    transaction_id BIGINT NOT NULL,

    -- Block ranges for this watermark stored as JSONB
    -- Format: Array of BlockRange objects
    block_ranges JSONB NOT NULL,

    -- Primary key on stream_id + transaction_id
    PRIMARY KEY (stream_id, transaction_id)
);

-- Index for efficient ordering by transaction_id within a stream
CREATE INDEX idx_amp_client_buffer_entries_stream_tx_id ON amp_client_buffer_entries(stream_id, transaction_id);
