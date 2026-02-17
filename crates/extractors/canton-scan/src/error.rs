//! Error types for the Canton Scan API extractor.

use crate::client;

/// Errors that can occur when creating or using a Canton Scan API provider.
#[derive(Debug, thiserror::Error)]
pub enum ProviderError {
    /// Failed to create the client
    #[error("failed to create Canton Scan API client")]
    Client(#[source] client::Error),
}
