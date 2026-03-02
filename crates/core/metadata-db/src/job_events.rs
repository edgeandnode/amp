//! Job event log operations for the append-only lifecycle event table.
//!
//! This module provides the public API for recording and querying job lifecycle
//! events in the `job_events` table. Every state transition (scheduled, running,
//! completed, error, fatal, stopped) is appended as an immutable event, forming
//! a complete audit trail for each job.
//!
//! Retry attempts are derived from counting `SCHEDULED` events for a given job —
//! no dedicated attempts table is required.

use sqlx::types::chrono::{DateTime, Utc};

use crate::{
    Error,
    db::Executor,
    jobs::{JobId, JobStatus},
    workers::WorkerNodeId,
};

pub(crate) mod sql;

#[cfg(test)]
mod tests {
    mod it_job_events;
}

/// Represents a job attempt derived from SCHEDULED events in job_events
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct JobEvent {
    pub job_id: JobId,
    pub retry_index: i32,
    pub created_at: DateTime<Utc>,
}

/// Register a new event in the job event log
#[tracing::instrument(skip(exe), err)]
pub async fn register<'c, E>(
    exe: E,
    job_id: impl Into<JobId> + std::fmt::Debug,
    node_id: &WorkerNodeId<'_>,
    status: JobStatus,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    sql::insert(exe, job_id.into(), node_id, status)
        .await
        .map_err(Error::Database)
}

/// Get all scheduling attempts for a job
///
/// Returns attempts derived from SCHEDULED events, ordered by retry_index ascending.
#[tracing::instrument(skip(exe), err)]
pub async fn get_attempts_for_job<'c, E>(
    exe: E,
    job_id: impl Into<JobId> + std::fmt::Debug,
) -> Result<Vec<JobEvent>, Error>
where
    E: Executor<'c>,
{
    sql::get_attempts_for_job(exe, job_id.into())
        .await
        .map_err(Error::Database)
}
