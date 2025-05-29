//! Metadata DB worker nodes job queue

use sqlx::{types::JsonValue, Postgres};

use super::WorkerNodeId;

/// Insert a new job into the queue
///
/// The job will be set as the default [`JobStatus`], which is [`JobStatus::Scheduled`],
/// and will be assigned to the given worker node.
pub async fn register_job<'c, E>(
    exe: E,
    node_id: &WorkerNodeId,
    descriptor: &str,
) -> Result<JobId, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = Postgres>,
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

/// Update the status of a job
///
/// This function will set the job status to the given [`JobStatus`] and update the
/// `updated_at` timestamp.
pub async fn update_job_status<'c, E>(
    exe: E,
    id: JobId,
    status: JobStatus,
) -> Result<(), sqlx::Error>
where
    E: sqlx::Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        UPDATE jobs 
        SET status = $1, updated_at = (timezone('UTC', now())) 
        WHERE id = $2
    "#};
    sqlx::query(query)
        .bind(status)
        .bind(id)
        .execute(exe)
        .await?;
    Ok(())
}

/// Get a job by its ID
pub async fn get_job<'c, E>(exe: E, id: &JobId) -> Result<Option<Job>, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT id, node_id, status, descriptor
        FROM jobs
        WHERE id = $1
    "#};
    let res = sqlx::query_as(query).bind(id).fetch_optional(exe).await?;
    Ok(res)
}

/// Get the job descriptor for a given job ID
pub async fn get_job_descriptor<'c, E>(exe: E, id: &JobId) -> Result<Option<String>, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT descriptor::text
        FROM jobs
        WHERE id = $1
    "#};
    let res = sqlx::query_scalar(query)
        .bind(id)
        .fetch_optional(exe)
        .await?;
    Ok(res)
}

/// Get all job IDs scheduled for a given worker node
pub async fn get_job_ids_for_node<'c, E>(
    exe: E,
    node_id: &WorkerNodeId,
) -> Result<Vec<JobId>, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT id
        FROM jobs
        WHERE node_id = $1
        ORDER BY id ASC
    "#};
    let res = sqlx::query_scalar(query)
        .bind(node_id)
        .fetch_all(exe)
        .await?;
    Ok(res)
}

/// Get all active job IDs for a given worker node
///
/// A job is considered active if it's in a non-terminal state.
pub async fn get_active_job_ids_for_node<'c, E>(
    exe: E,
    node_id: &WorkerNodeId,
) -> Result<Vec<JobId>, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
            SELECT id
            FROM jobs
            WHERE node_id = $1 AND status = ANY($2)
            ORDER BY id ASC
        "#,
    };
    let res = sqlx::query_scalar(query)
        .bind(node_id)
        .bind([
            JobStatus::Scheduled,
            JobStatus::Running,
            JobStatus::StopRequested,
            JobStatus::Stopping,
        ])
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

impl Job {
    /// Check if the job is active
    ///
    /// A job is active if it's in a non-terminal state.
    pub fn is_active(&self) -> bool {
        matches!(
            self.status,
            JobStatus::Scheduled
                | JobStatus::Running
                | JobStatus::StopRequested
                | JobStatus::Stopping
        )
    }

    /// Check if the job is completed
    pub fn is_completed(&self) -> bool {
        matches!(self.status, JobStatus::Completed)
    }
}

/// A unique identifier for a job
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    sqlx::Type,
    serde::Serialize,
    serde::Deserialize,
)]
#[repr(transparent)]
#[serde(transparent)]
#[sqlx(transparent)]
pub struct JobId(i64);

impl JobId {
    /// Convert the [`JobId`] to an `i64`
    pub fn to_i64(self) -> i64 {
        self.0
    }
}

impl AsRef<i64> for JobId {
    fn as_ref(&self) -> &i64 {
        &self.0
    }
}

impl From<JobId> for i64 {
    fn from(id: JobId) -> Self {
        id.0
    }
}

impl std::fmt::Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
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
#[non_exhaustive]
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
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl serde::Serialize for JobStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for JobStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s: &str = serde::Deserialize::deserialize(deserializer)?;
        // Since FromStr::Err is Infallible, unwrap is safe.
        Ok(s.parse().unwrap())
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
