-- Fix heartbeat_at column type to match other timestamp columns and Rust DateTime<Utc> type
ALTER TABLE workers
  ALTER COLUMN heartbeat_at TYPE TIMESTAMPTZ
  USING heartbeat_at AT TIME ZONE 'UTC';
