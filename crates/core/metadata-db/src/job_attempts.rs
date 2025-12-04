//! Job attempts tracking module
//!
//! This module provides functionality for tracking each scheduling attempt for jobs,
//! including the initial scheduling and all subsequent retries.

pub(crate) mod sql;

pub use self::sql::JobAttempt;
use crate::{db::Executor, error::Error, jobs::JobId};

/// Insert a new job attempt record
///
/// Records that a job was scheduled or rescheduled with the given retry_index.
/// The created_at timestamp is set automatically to the current time.
///
/// - `retry_index = 0` represents the initial scheduling attempt
/// - `retry_index = 1, 2, 3...` represent subsequent retries
#[tracing::instrument(skip(exe), err)]
pub async fn insert_attempt<'c, E>(
    exe: E,
    job_id: impl Into<JobId> + std::fmt::Debug,
    retry_index: i32,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    sql::insert_attempt(exe, job_id.into(), retry_index)
        .await
        .map_err(Error::Database)
}

/// Mark an attempt as completed
///
/// Sets the completed_at timestamp for the given job attempt.
/// This should be called when a job reaches a terminal state (completed, stopped, or failed).
#[tracing::instrument(skip(exe), err)]
pub async fn mark_attempt_completed<'c, E>(
    exe: E,
    job_id: impl Into<JobId> + std::fmt::Debug,
    retry_index: i32,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    sql::mark_attempt_completed(exe, job_id.into(), retry_index)
        .await
        .map_err(Error::Database)
}

/// Get all attempts for a given job
///
/// Returns attempts ordered by retry_index ascending (0, 1, 2, ...).
#[tracing::instrument(skip(exe), err)]
pub async fn get_attempts_for_job<'c, E>(
    exe: E,
    job_id: impl Into<JobId> + std::fmt::Debug,
) -> Result<Vec<JobAttempt>, Error>
where
    E: Executor<'c>,
{
    sql::get_attempts_for_job(exe, job_id.into())
        .await
        .map_err(Error::Database)
}
