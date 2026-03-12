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
    jobs::{JobDescriptorRawOwned, JobId, JobStatus},
    workers::WorkerNodeId,
};

mod details;
pub(crate) mod sql;

pub use details::{EventDetail, EventDetailOwned};

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
    status: impl Into<JobStatus> + std::fmt::Debug,
    detail: impl Into<Option<EventDetail<'_>>> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    let status = status.into();
    let detail: Option<EventDetail<'_>> = detail.into();

    sql::insert(exe, job_id.into(), node_id, status, detail.as_ref())
        .await
        .map_err(Error::Database)
}

/// Count the number of scheduling attempts for a job
#[tracing::instrument(skip(exe), err)]
pub async fn get_attempt_count<'c, E>(
    exe: E,
    job_id: impl Into<JobId> + std::fmt::Debug,
) -> Result<i32, Error>
where
    E: Executor<'c>,
{
    sql::get_attempt_count(exe, job_id.into())
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

/// Get the most recent descriptor from a job's SCHEDULED events.
///
/// Returns `None` if no SCHEDULED event with a detail exists for the job.
#[tracing::instrument(skip(exe), err)]
pub async fn get_latest_descriptor<'c, E>(
    exe: E,
    job_id: impl Into<JobId> + std::fmt::Debug,
) -> Result<Option<JobDescriptorRawOwned>, Error>
where
    E: Executor<'c>,
{
    sql::get_latest_descriptor(exe, job_id.into())
        .await
        .map_err(Error::Database)
}

/// Get latest job descriptor for each of the given jobs
///
/// Returns one `(job_id, detail)` pair per job, using the most recent
/// SCHEDULED event in `job_events`.
#[tracing::instrument(skip(exe), err)]
pub async fn list_latest_descriptors<'c, E>(
    exe: E,
    job_ids: &[JobId],
) -> Result<Vec<(JobId, JobDescriptorRawOwned)>, Error>
where
    E: Executor<'c>,
{
    sql::list_latest_descriptors(exe, job_ids)
        .await
        .map_err(Error::Database)
}

/// Get latest job descriptor for each of the given jobs
///
/// Returns one `(job_id, detail)` pair per job, using the most recent
/// SCHEDULED event in `job_events`.
#[tracing::instrument(skip(exe), err)]
pub async fn get_job_detail<'c, E>(
    exe: E,
    job_id: impl Into<JobId> + std::fmt::Debug,
    status: impl Into<JobStatus> + std::fmt::Debug,
) -> Result<Option<EventDetailOwned>, Error>
where
    E: Executor<'c>,
{
    sql::get_job_detail(exe, job_id.into(), status.into())
        .await
        .map_err(Error::Database)
}

#[cfg(test)]
mod tests {
    mod it_job_errors;
    mod it_job_events;
}
