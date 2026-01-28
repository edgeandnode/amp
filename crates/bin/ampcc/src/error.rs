//! Application-level errors for ampcc.

use crate::amp_registry::AmpRegistryError;

/// Application-level errors for the ampcc TUI.
#[derive(Debug, thiserror::Error)]
pub enum AmpccError {
    /// Access token is required but was not provided.
    ///
    /// This error occurs when attempting to access authenticated endpoints
    /// (owned datasets) without providing an access token. The caller should
    /// ensure the user is logged in before calling owned dataset methods.
    #[error("Authentication required: access token not provided")]
    Unauthenticated,

    /// Failed to fetch datasets from the AMP registry.
    ///
    /// This wraps errors from the registry client when fetching the dataset list.
    #[error("Failed to fetch datasets from registry")]
    FetchRegistryDatasets(#[source] AmpRegistryError),

    /// Failed to search datasets in the AMP registry.
    ///
    /// This wraps errors from the registry client when searching datasets.
    /// Includes the search term for context.
    #[error("Failed to search datasets in registry with term '{0}'")]
    SearchRegistryDatasets(String, #[source] AmpRegistryError),
}
