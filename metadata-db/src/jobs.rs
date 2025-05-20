//! Worker node jobs metadata

use sqlx::types::JsonValue;

use super::workers::WorkerNodeId;

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

    /// Job is stopping
    ///
    /// The scheduler has requested the job to stop. The worker will stop
    /// the job as soon as possible.
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
    /// Check if the job is running
    ///
    /// This includes both **scheduled** and **running** jobs.
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Scheduled | Self::Running)
    }

    /// Check if the job is completed
    pub fn is_completed(&self) -> bool {
        matches!(self, Self::Completed)
    }

    /// Check if the job is stopped
    ///
    /// This includes both **stopped** and **stopping** jobs.
    pub fn is_stopped(&self) -> bool {
        matches!(self, Self::Stopping | Self::Stopped)
    }

    /// Check if the job has failed
    pub fn is_failed(&self) -> bool {
        matches!(self, Self::Failed)
    }

    /// Check if the job is in an unknown state
    pub fn is_unknown(&self) -> bool {
        matches!(self, Self::Unknown)
    }

    /// Convert the [`JobStatus`] to a string
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Scheduled => "SCHEDULED",
            Self::Running => "RUNNING",
            Self::Completed => "COMPLETED",
            Self::Stopped => "STOPPED",
            Self::Stopping => "STOPPING",
            Self::Failed => "FAILED",
            Self::Unknown => "UNKNOWN",
        }
    }

    /// Convert a string to a [`JobStatus`]
    ///
    /// This function is case-insensitive.
    pub fn from_str(s: &str) -> Self {
        // Use `eq_ignore_ascii_case` to make the comparison case-insensitive
        match s {
            s if s.eq_ignore_ascii_case("SCHEDULED") => Self::Scheduled,
            s if s.eq_ignore_ascii_case("RUNNING") => Self::Running,
            s if s.eq_ignore_ascii_case("COMPLETED") => Self::Completed,
            s if s.eq_ignore_ascii_case("STOPPED") => Self::Stopped,
            s if s.eq_ignore_ascii_case("STOPPING") => Self::Stopping,
            s if s.eq_ignore_ascii_case("FAILED") => Self::Failed,
            _ => Self::Unknown,
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
        Ok(JobStatus::from_str(s))
    }
}

impl sqlx::Type<sqlx::Postgres> for JobStatus {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        sqlx::postgres::PgTypeInfo::with_name("TEXT")
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for JobStatus {
    fn decode(
        value: <sqlx::Postgres as sqlx::Database>::ValueRef<'r>,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let value: &str = sqlx::Decode::<sqlx::Postgres>::decode(value)?;
        Ok(JobStatus::from_str(value))
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
