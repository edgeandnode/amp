/// Extension trait for classifying errors as retryable or fatal.
///
/// The primary method is [`is_retryable`](RetryableErrorExt::is_retryable), which returns `true`
/// when the error is transient and the operation may succeed on retry. The default for
/// [`is_fatal`](RetryableErrorExt::is_fatal) is the logical inverse, so implementors only need to
/// define `is_retryable`.
///
/// **Fail-safe convention:** new or forgotten variants default to *non-retryable* (fatal).
/// This prevents infinite retry loops when a new error variant is added but `is_retryable`
/// is not updated to cover it.
pub trait RetryableErrorExt: std::error::Error {
    /// Returns `true` if the error is transient and the operation may succeed on retry.
    fn is_retryable(&self) -> bool;

    /// Returns `true` if the error is permanent and the operation should not be retried.
    fn is_fatal(&self) -> bool {
        !self.is_retryable()
    }
}
