-- Add metadata columns to workers table and rename last_heartbeat
--
-- info: JSONB column containing worker metadata (build info, system info, etc.)
-- created_at: Timestamp when the worker was first registered (set once)
-- registered_at: Timestamp when the worker was last registered (updated on every re-registration)
-- heartbeat_at: Renamed from last_heartbeat for clarity (updated on heartbeat)

ALTER TABLE workers
  ADD COLUMN info JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
  ADD COLUMN registered_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now());

ALTER TABLE workers
  RENAME COLUMN last_heartbeat TO heartbeat_at;
