//! Structured error detail payload for job failures.
//!
//! Also defines [`ErrorDetailsProvider`], a trait for errors that contribute
//! structured key-value details. Each error layer provides only its own context
//! via [`ErrorDetailsProvider::error_details`], and
//! [`collect_error_details`] walks the chain to merge them.

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

/// Trait for errors that contribute structured details to error payloads.
///
/// Each error layer contributes only its own context via [`error_details`](Self::error_details).
/// The [`detail_source`](Self::detail_source) method chains to inner errors so a collector can
/// walk the typed chain without downcasting.
pub trait ErrorDetailsProvider {
    /// Key-value pairs this error layer contributes.
    ///
    /// Only this layer's own context — never aggregate from inner errors.
    fn error_details(&self) -> serde_json::Map<String, serde_json::Value> {
        serde_json::Map::new()
    }

    /// Next error in the chain that also provides details.
    fn detail_source(&self) -> Option<&dyn ErrorDetailsProvider> {
        None
    }
}

/// Build a details map containing `block_range_start` and `block_range_end` entries
/// for the given optional block numbers. Returns an empty map when both are `None`.
pub fn block_range_details(
    start: Option<u64>,
    end: Option<u64>,
) -> serde_json::Map<String, serde_json::Value> {
    let mut map = serde_json::Map::new();
    if let Some(start) = start {
        map.insert("block_range_start".into(), serde_json::Value::from(start));
    }
    if let Some(end) = end {
        map.insert("block_range_end".into(), serde_json::Value::from(end));
    }
    map
}

/// Walk the [`ErrorDetailsProvider`] chain and merge all detail maps.
///
/// First occurrence wins — the detail closest to the failure site takes priority
/// when the same key appears at multiple levels.
pub fn collect_error_details(
    provider: &dyn ErrorDetailsProvider,
) -> serde_json::Map<String, serde_json::Value> {
    let mut details = serde_json::Map::new();
    let mut current: Option<&dyn ErrorDetailsProvider> = Some(provider);
    while let Some(p) = current {
        for (k, v) in p.error_details() {
            details.entry(k).or_insert(v);
        }
        current = p.detail_source();
    }
    details
}
