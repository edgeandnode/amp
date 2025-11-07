//! Job status enumeration for the Worker service.
//!
//! This module provides a DTO for job status that decouples the Worker service
//! from the metadata-db `JobStatus` type.

/// Represents the current status of a job in the Worker service.
///
/// This DTO mirrors the metadata-db `JobStatus` enum but provides a stable
/// interface that can evolve independently of the database schema.
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
    #[must_use]
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
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Stopped | Self::Failed)
    }

    /// Returns an array of all terminal job status
    ///
    /// These are the statuses that represent completed job lifecycles
    /// and can be safely deleted from the system.
    #[must_use]
    pub fn terminal_statuses() -> [JobStatus; 3] {
        [Self::Completed, Self::Stopped, Self::Failed]
    }
}

impl From<metadata_db::JobStatus> for JobStatus {
    fn from(status: metadata_db::JobStatus) -> Self {
        match status {
            metadata_db::JobStatus::Scheduled => Self::Scheduled,
            metadata_db::JobStatus::Running => Self::Running,
            metadata_db::JobStatus::Completed => Self::Completed,
            metadata_db::JobStatus::Stopped => Self::Stopped,
            metadata_db::JobStatus::StopRequested => Self::StopRequested,
            metadata_db::JobStatus::Stopping => Self::Stopping,
            metadata_db::JobStatus::Failed => Self::Failed,
            metadata_db::JobStatus::Unknown => Self::Unknown,
        }
    }
}

impl From<JobStatus> for metadata_db::JobStatus {
    fn from(status: JobStatus) -> Self {
        match status {
            JobStatus::Scheduled => Self::Scheduled,
            JobStatus::Running => Self::Running,
            JobStatus::Completed => Self::Completed,
            JobStatus::Stopped => Self::Stopped,
            JobStatus::StopRequested => Self::StopRequested,
            JobStatus::Stopping => Self::Stopping,
            JobStatus::Failed => Self::Failed,
            JobStatus::Unknown => Self::Unknown,
        }
    }
}

impl std::str::FromStr for JobStatus {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            s if s.eq_ignore_ascii_case("SCHEDULED") => Ok(Self::Scheduled),
            s if s.eq_ignore_ascii_case("RUNNING") => Ok(Self::Running),
            s if s.eq_ignore_ascii_case("COMPLETED") => Ok(Self::Completed),
            s if s.eq_ignore_ascii_case("STOPPED") => Ok(Self::Stopped),
            s if s.eq_ignore_ascii_case("STOP_REQUESTED") => Ok(Self::StopRequested),
            s if s.eq_ignore_ascii_case("STOPPING") => Ok(Self::Stopping),
            s if s.eq_ignore_ascii_case("FAILED") => Ok(Self::Failed),
            _ => Ok(Self::Unknown),
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
        let s = String::deserialize(deserializer)?;
        // Since FromStr::Err is Infallible, unwrap is safe
        Ok(s.parse().unwrap())
    }
}
