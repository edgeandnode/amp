//! Job notification types for worker coordination

use super::id::JobId;

/// The payload of a job notification (without `node_id`, which is in the wrapper)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Notification {
    pub job_id: JobId,
    pub action: Action,
}

impl Notification {
    /// Create a new start action
    #[must_use]
    pub fn start(job_id: JobId) -> Self {
        Self {
            job_id,
            action: Action::Start,
        }
    }

    /// Create a new stop action
    #[must_use]
    pub fn stop(job_id: JobId) -> Self {
        Self {
            job_id,
            action: Action::Stop,
        }
    }

    /// Create a new resume action
    #[must_use]
    pub fn resume(job_id: JobId) -> Self {
        Self {
            job_id,
            action: Action::Resume,
        }
    }
}

/// Job notification actions
///
/// These actions coordinate the jobs state and the write lock on the output table locations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub enum Action {
    /// Start the job
    ///
    /// Fetch the job descriptor from the Metadata DB job queue and start the job.
    Start,

    /// Stop the job
    ///
    /// Stop the job: mark the job as stopped and release the locations by deleting
    /// the row from the `jobs` table.
    Stop,

    /// Resume the job
    ///
    /// Resume the job: Fetch the job from the Metadata DB job queue and resume the job
    Resume,
}

impl Action {
    /// Returns the string representation of the action
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Start => "START",
            Self::Stop => "STOP",
            Self::Resume => "RESUME",
        }
    }
}

impl std::fmt::Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_str().fmt(f)
    }
}

impl std::str::FromStr for Action {
    type Err = Box<dyn std::error::Error + Send + Sync>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            s if s.eq_ignore_ascii_case("START") => Ok(Self::Start),
            s if s.eq_ignore_ascii_case("STOP") => Ok(Self::Stop),
            s if s.eq_ignore_ascii_case("RESUME") => Ok(Self::Resume),
            _ => Err(format!("Invalid action variant: {s}").into()),
        }
    }
}

impl serde::Serialize for Action {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for Action {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s: &str = serde::Deserialize::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_action_string_parsing_succeeds() {
        // Uppercase
        let start_upper: Result<Action, _> = "START".parse();
        assert!(
            matches!(start_upper, Ok(Action::Start)),
            "Expected START to parse as Action::Start"
        );

        let stop_upper: Result<Action, _> = "STOP".parse();
        assert!(
            matches!(stop_upper, Ok(Action::Stop)),
            "Expected STOP to parse as Action::Stop"
        );

        // Test case-insensitive parsing
        let start_lower: Result<Action, _> = "start".parse();
        assert!(
            matches!(start_lower, Ok(Action::Start)),
            "Expected start to parse as Action::Start"
        );

        let stop_lower: Result<Action, _> = "stop".parse();
        assert!(
            matches!(stop_lower, Ok(Action::Stop)),
            "Expected stop to parse as Action::Stop"
        );
    }

    #[test]
    fn invalid_action_string_parsing_fails() {
        // Completely invalid strings
        let invalid_upper: Result<Action, _> = "INVALID".parse();
        assert!(invalid_upper.is_err(), "Expected INVALID to fail parsing");

        let invalid_lower: Result<Action, _> = "invalid".parse();
        assert!(invalid_lower.is_err(), "Expected invalid to fail parsing");

        // Empty string
        let empty: Result<Action, _> = "".parse();
        assert!(empty.is_err(), "Expected empty string to fail parsing");

        // Similar but wrong action names
        let pause_upper: Result<Action, _> = "PAUSE".parse();
        assert!(pause_upper.is_err(), "Expected PAUSE to fail parsing");

        let run_upper: Result<Action, _> = "RUN".parse();
        assert!(run_upper.is_err(), "Expected RUN to fail parsing");

        let begin_lower: Result<Action, _> = "begin".parse();
        assert!(begin_lower.is_err(), "Expected begin to fail parsing");

        let end_lower: Result<Action, _> = "end".parse();
        assert!(end_lower.is_err(), "Expected end to fail parsing");

        // Numeric strings
        let numeric: Result<Action, _> = "123".parse();
        assert!(numeric.is_err(), "Expected 123 to fail parsing");

        // Extended action names
        let start_now_upper: Result<Action, _> = "START_NOW".parse();
        assert!(
            start_now_upper.is_err(),
            "Expected START_NOW to fail parsing"
        );

        let stop_now_upper: Result<Action, _> = "STOP_NOW".parse();
        assert!(stop_now_upper.is_err(), "Expected STOP_NOW to fail parsing");
    }
}
