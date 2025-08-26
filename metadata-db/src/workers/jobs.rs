//! Metadata DB worker nodes job queue

use sqlx::types::{
    JsonValue,
    chrono::{DateTime, Utc},
};

use super::{WorkerNodeId, job_id::JobId};

/// Insert a new job into the queue
///
/// The job will be set as the default [`JobStatus`], which is [`JobStatus::Scheduled`],
/// and will be assigned to the given worker node.
pub async fn register<'c, E>(
    exe: E,
    node_id: &WorkerNodeId,
    descriptor: &str,
) -> Result<JobId, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let query = indoc::indoc! {r#"
        INSERT INTO jobs (node_id, descriptor, status, created_at, updated_at)
        VALUES ($1, $2::jsonb, $3, (timezone('UTC', now())), (timezone('UTC', now())))
        RETURNING id
    "#};
    let res = sqlx::query_scalar(query)
        .bind(node_id)
        .bind(descriptor)
        .bind(JobStatus::default())
        .fetch_one(exe)
        .await?;
    Ok(res)
}

/// Update the status of a job with multiple possible expected original states
///
/// This function will only update the job status if the job exists and currently has
/// one of the expected original statuses. If the job doesn't exist, returns `UpdateJobStatusError::NotFound`.
/// If the job exists but has a different status than any of the expected ones, returns `UpdateJobStatusError::StateConflict`.
pub async fn update_status_if_any_state<'c, E>(
    exe: E,
    id: &JobId,
    expected_statuses: &[JobStatus],
    new_status: JobStatus,
) -> Result<(), JobStatusUpdateError>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    /// Internal structure to hold the result of the update operation
    #[derive(Debug, sqlx::FromRow)]
    struct UpdateResult {
        updated_id: Option<JobId>,
        original_status: Option<JobStatus>,
    }

    let query = indoc::indoc! {r#"
        WITH target_job AS (
            SELECT id, status
            FROM jobs
            WHERE id = $1
        ),
        target_job_update AS (
            UPDATE jobs
            SET status = $3, updated_at = timezone('UTC', now())
            WHERE id = $1 AND status = ANY($2)
            RETURNING id
        )
        SELECT
            target_job_update.id AS updated_id,
            target_job.status AS original_status
        FROM target_job
        LEFT JOIN target_job_update ON target_job.id = target_job_update.id
    "#};

    let result: Option<UpdateResult> = sqlx::query_as(query)
        .bind(id)
        .bind(expected_statuses)
        .bind(new_status)
        .fetch_optional(exe)
        .await
        .map_err(JobStatusUpdateError::Database)?;

    match result {
        Some(UpdateResult {
            updated_id: Some(_),
            ..
        }) => Ok(()),
        Some(UpdateResult {
            updated_id: None,
            original_status: Some(status),
        }) => Err(JobStatusUpdateError::StateConflict {
            expected: expected_statuses.to_vec(),
            actual: status,
        }),
        _ => Err(JobStatusUpdateError::NotFound),
    }
}

/// Error type for conditional job status updates
#[derive(Debug, thiserror::Error)]
pub enum JobStatusUpdateError {
    #[error("Job not found")]
    NotFound,

    #[error("Job state conflict: expected one of {expected:?}, but found {actual}")]
    StateConflict {
        expected: Vec<JobStatus>,
        actual: JobStatus,
    },

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}

/// Get a job by its ID
pub async fn get_by_id<'c, E>(exe: E, id: &JobId) -> Result<Option<Job>, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT id, node_id, status, descriptor
        FROM jobs
        WHERE id = $1
    "#};
    let res = sqlx::query_as(query).bind(id).fetch_optional(exe).await?;
    Ok(res)
}

/// Get a job by ID with full details including timestamps
pub async fn get_by_id_with_details<'c, E>(
    exe: E,
    id: &JobId,
) -> Result<Option<JobWithDetails>, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            id,
            node_id,
            status,
            descriptor,
            created_at,
            updated_at
        FROM jobs
        WHERE id = $1
    "#};
    let res = sqlx::query_as(query).bind(id).fetch_optional(exe).await?;
    Ok(res)
}

/// Get jobs for a given worker node with any of the specified statuses
pub async fn get_by_node_id_and_statuses<'c, E, const N: usize>(
    exe: E,
    node_id: &WorkerNodeId,
    statuses: [JobStatus; N],
) -> Result<Vec<Job>, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            id,
            node_id,
            status,
            descriptor
        FROM jobs
        WHERE node_id = $1 AND status = ANY($2)
        ORDER BY id ASC
    "#};
    let res = sqlx::query_as(query)
        .bind(node_id)
        .bind(statuses)
        .fetch_all(exe)
        .await?;
    Ok(res)
}

/// List the first page of jobs
///
/// Returns a paginated list of jobs ordered by ID in descending order (newest first).
/// This function is used to fetch the initial page when no cursor is available.
pub async fn list_first_page<'c, E>(exe: E, limit: i64) -> Result<Vec<JobWithDetails>, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            id,
            node_id,
            status,
            descriptor,
            created_at,
            updated_at
        FROM jobs
        ORDER BY id DESC
        LIMIT $1
    "#};

    let res = sqlx::query_as(query).bind(limit).fetch_all(exe).await?;
    Ok(res)
}

/// List subsequent pages of jobs using cursor-based pagination
///
/// Returns a paginated list of jobs with IDs less than the provided cursor,
/// ordered by ID in descending order (newest first). This implements cursor-based
/// pagination for efficient traversal of large job lists.
pub async fn list_next_page<'c, E>(
    exe: E,
    limit: i64,
    last_job_id: JobId,
) -> Result<Vec<JobWithDetails>, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT 
            id, 
            node_id, 
            status, 
            descriptor,
            created_at,
            updated_at
        FROM jobs
        WHERE id < $2
        ORDER BY id DESC
        LIMIT $1
    "#};

    let res = sqlx::query_as(query)
        .bind(limit)
        .bind(last_job_id)
        .fetch_all(exe)
        .await?;
    Ok(res)
}

/// Represents a job with its metadata and associated node.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Job {
    /// Unique identifier for the job
    pub id: JobId,

    /// ID of the worker node this job is scheduled for
    pub node_id: WorkerNodeId,

    /// Current status of the job
    pub status: JobStatus,

    /// Job description
    #[sqlx(rename = "descriptor")]
    pub desc: JsonValue,
}

/// Represents a job with its metadata and associated node.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct JobWithDetails {
    /// Unique identifier for the job
    pub id: JobId,

    /// ID of the worker node this job is scheduled for
    pub node_id: WorkerNodeId,

    /// Current status of the job
    pub status: JobStatus,

    /// Job descriptor
    #[sqlx(rename = "descriptor")]
    pub desc: JsonValue,

    /// Job creation timestamp
    pub created_at: DateTime<Utc>,

    /// Job last update timestamp
    pub updated_at: DateTime<Utc>,
}

/// Represents the current status of a job
///
/// This status is used to track the progress of a job, but it is not
/// guaranteed to be up to date. It is responsibility of the caller to
/// confirm the status of the job before proceeding.
///
/// The status is stored as a `TEXT` column in the database. If the fetched
/// status is not one of the valid values in the enum, the `UNKNOWN` status is
/// returned.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum JobStatus {
    /// Job is being scheduled.
    ///
    /// This is the initial state of a job.
    ///
    /// The scheduler has added the job to the queue, but the job has not
    /// yet been picked up by the worker node.
    #[default]
    Scheduled,

    /// Job is running
    ///
    /// The job has been picked up by the worker node and is being executed.
    Running,

    /// Job has finished successfully
    ///
    /// This is a terminal state.
    Completed,

    /// Job has stopped
    ///
    /// The worker node has stopped the job as requested by the scheduler.
    ///
    /// This is a terminal state.
    Stopped,

    /// Job has been requested to stop
    ///
    /// The scheduler has requested the job to stop. The worker will stop
    /// the job as soon as possible.
    StopRequested,

    /// Job is stopping
    ///
    /// The worker node acknowledged the stop request and will stop the job
    /// as soon as possible.
    Stopping,

    /// Job has failed
    ///
    /// An error occurred while running the job.
    ///
    /// This is a terminal state.
    Failed,

    /// Unknown status
    ///
    /// This is an invalid status, and should never happen. Although
    /// it is possible to happen if the worker node version is different
    /// from the version of the scheduler.
    Unknown,
}

impl JobStatus {
    /// Convert the [`JobStatus`] to a string
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Scheduled => "SCHEDULED",
            Self::Running => "RUNNING",
            Self::Completed => "COMPLETED",
            Self::Stopped => "STOPPED",
            Self::StopRequested => "STOP_REQUESTED",
            Self::Stopping => "STOPPING",
            Self::Failed => "FAILED",
            Self::Unknown => "UNKNOWN",
        }
    }

    /// Returns true if the job status is terminal (cannot be changed further)
    ///
    /// Terminal states are final states where the job lifecycle has ended:
    /// - `Completed`: Job finished successfully
    /// - `Stopped`: Job was stopped by request
    /// - `Failed`: Job encountered an error and failed
    ///
    /// Non-terminal states can still transition to other states
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Stopped | Self::Failed)
    }
}

impl std::str::FromStr for JobStatus {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Use `eq_ignore_ascii_case` to make the comparison case-insensitive
        match s {
            s if s.eq_ignore_ascii_case("SCHEDULED") => Ok(Self::Scheduled),
            s if s.eq_ignore_ascii_case("RUNNING") => Ok(Self::Running),
            s if s.eq_ignore_ascii_case("COMPLETED") => Ok(Self::Completed),
            s if s.eq_ignore_ascii_case("STOPPED") => Ok(Self::Stopped),
            s if s.eq_ignore_ascii_case("STOP_REQUESTED") => Ok(Self::StopRequested),
            s if s.eq_ignore_ascii_case("STOPPING") => Ok(Self::Stopping),
            s if s.eq_ignore_ascii_case("FAILED") => Ok(Self::Failed),
            _ => Ok(Self::Unknown), // Default to Unknown for Infallible
        }
    }
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl sqlx::Type<sqlx::Postgres> for JobStatus {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        sqlx::postgres::PgTypeInfo::with_name("TEXT")
    }
}

impl sqlx::postgres::PgHasArrayType for JobStatus {
    fn array_type_info() -> sqlx::postgres::PgTypeInfo {
        sqlx::postgres::PgTypeInfo::with_name("TEXT[]")
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for JobStatus {
    fn decode(
        value: <sqlx::Postgres as sqlx::Database>::ValueRef<'r>,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let value: &str = sqlx::Decode::<sqlx::Postgres>::decode(value)?;
        // Since FromStr::Err is Infallible, unwrap is safe.
        Ok(value.parse().unwrap())
    }
}

impl<'q> sqlx::Encode<'q, sqlx::Postgres> for JobStatus {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Postgres as sqlx::Database>::ArgumentBuffer<'q>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        sqlx::Encode::<sqlx::Postgres>::encode_by_ref(&self.as_str(), buf)
    }
}
