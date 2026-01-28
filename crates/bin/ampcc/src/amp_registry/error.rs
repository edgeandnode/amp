use datasets_common::reference::Reference;

#[derive(Debug, thiserror::Error)]
pub enum AmpRegistryError {
    /// The access token is invalid or expired.
    ///
    /// This error is returned when an authenticated endpoint returns a 401 status code.
    /// The caller should refresh the token and retry the request.
    #[error("Unauthorized: access token is invalid or expired")]
    Unauthorized,
    /// Failure occurred while fetching the Datasets list from the amp-registry-api
    ///
    /// - Error could be an internal-server error (5xx) from the amp-registry-api if the service is down.
    /// - Error could also be a bad request error (400) if the query params are invalid.
    ///     - for example: using a limit value that exceeds the max limit
    #[error("Failure fetching Amp Registry Dataset list: {0}")]
    FetchDatasets(#[source] reqwest::Error),
    /// Failure occurred while searching the Datasets list from the amp-registry-api
    ///
    /// - Error could be an internal-server error (5xx) from the amp-registry-api if the service is down.
    /// - Error could also be a bad request error (400) if the query params are invalid.
    ///     - for example: the search term is not present, or invalid
    #[error("Failure searching Amp Registry Dataset list with search term: {0}. {1}")]
    SearchDatasets(String, #[source] reqwest::Error),
    /// Failure occurred while fetching the authenticated users owned Datasets list from the amp-registry-api
    ///
    /// - Error could be an internal-server error (5xx) from the amp-registry-api if the service is down.
    /// - Error could also be a bad request error (400) if the query params are invalid.
    ///     - for example: using a limit value that exceeds the max limit
    #[error("Failure fetching Owned Amp Registry Dataset list: {0}")]
    FetchOwnedDatasets(#[source] reqwest::Error),
    /// Failure occurred while searching the authenticated users owned Datasets list from the amp-registry-api
    ///
    /// - Error could be an internal-server error (5xx) from the amp-registry-api if the service is down.
    /// - Error could also be a bad request error (400) if the query params are invalid.
    ///     - for example: the search term is not present, or invalid
    #[error("Failure searching Amp Registry Dataset list with search term: {0}. {1}")]
    SearchOwnedDatasets(String, #[source] reqwest::Error),
    /// Failure occurred while fetching the Dataset Revision Manifest from the amp-registry-api
    ///
    /// - Error could be an internal-server error (5xx) from the amp-registry-api if the service is down.
    /// - Error could also be a bad request error (400) if the path params are invalid
    ///     - for example: if the namespace is an invalid namespace
    /// - If the status is a 404, no manifest exists for the given Dataset namespace, name and revision
    #[error("Failure fetching Dataset: {0} Manifest: {1}")]
    FetchDatasetRevisionManifest(Reference, #[source] reqwest::Error),
}
