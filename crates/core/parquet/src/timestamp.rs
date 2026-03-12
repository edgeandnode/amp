use std::time::{Duration, SystemTime};

/// A Unix epoch timestamp stored as a [`Duration`] since `UNIX_EPOCH`.
#[derive(Debug, Clone, Copy, Default, serde::Serialize, serde::Deserialize)]
pub struct Timestamp(pub Duration);

impl Timestamp {
    /// Returns the current system time as a [`Timestamp`].
    ///
    /// # Panics
    /// Panics if the system clock is before `UNIX_EPOCH`.
    pub fn now() -> Self {
        Timestamp(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap(),
        )
    }
}
