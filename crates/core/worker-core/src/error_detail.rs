//! Structured error detail payload for job failures.

/// Structured error detail persisted as JSONB on job failure.
///
/// Serialized via `serde` and converted to [`metadata_db::job_events::EventDetail`]
/// at the service boundary.
///
/// The type parameter `T` allows job implementations to attach domain-specific
/// context (e.g. dataset reference, manifest hash). Defaults to `()` when no
/// extra context is needed.
#[derive(Debug, serde::Serialize)]
pub struct ErrorDetailPayload<T: serde::Serialize + serde::de::DeserializeOwned = ()> {
    /// Error code identifying the specific error variant
    /// (e.g. `"GET_DATASET"`, `"PARTITION_TASK"`).
    pub error_code: String,
    /// Human-readable error description from `Display` impl.
    pub error_message: String,
    /// Optional structured context accompanying the error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_context: Option<ErrorContext<T>>,
}

/// Contextual information accompanying an error detail.
///
/// Contains the error's causal chain, retry metadata, and an extensible
/// `extra` field for job-specific data flattened into the JSON output.
#[derive(Debug, serde::Serialize)]
pub struct ErrorContext<T: serde::Serialize + serde::de::DeserializeOwned = ()> {
    /// Causal error chain from `std::error::Error::source()` traversal,
    /// ordered from immediate cause to root cause.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub stack_trace: Vec<String>,
    /// Retry attempt index at the time of failure.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt_index: Option<i32>,
    /// Job-specific extra context, flattened into the surrounding JSON object.
    ///
    /// Defaults to `()` (serializes to no fields). Job implementations supply
    /// a concrete type with `#[serde(skip_serializing_if)]` on its fields.
    #[serde(flatten)]
    pub extra: T,
}
