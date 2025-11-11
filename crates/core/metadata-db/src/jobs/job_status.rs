//! Job status enumeration and related implementations

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

    /// Returns an array of all terminal job statuses
    ///
    /// These are the statuses that represent completed job lifecycles
    /// and can be safely deleted from the system.
    pub fn terminal_statuses() -> [JobStatus; 3] {
        [Self::Completed, Self::Stopped, Self::Failed]
    }

    /// Returns an array of all non-terminal (active) job statuses
    ///
    /// These are the statuses that represent jobs still in progress
    /// and should be monitored or managed.
    pub fn non_terminal_statuses() -> [JobStatus; 4] {
        [
            Self::Scheduled,
            Self::Running,
            Self::StopRequested,
            Self::Stopping,
        ]
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
