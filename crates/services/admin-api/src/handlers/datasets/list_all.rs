use amp_datasets_registry::error::ListAllDatasetsError;
use axum::{Json, extract::State, http::StatusCode};
use datasets_common::{name::Name, namespace::Namespace, version::Version};

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `GET /datasets` endpoint
///
/// Returns all registered datasets across all namespaces with their version information.
///
/// ## Response
/// - **200 OK**: Successfully retrieved all datasets
/// - **500 Internal Server Error**: Database query error
///
/// ## Error Codes
/// - `LIST_ALL_DATASETS_ERROR`: Failed to list all datasets from dataset store
///
/// ## Behavior
/// This endpoint returns a comprehensive list of all datasets registered in the system,
/// grouped by namespace and name. For each dataset, it includes:
/// - The latest semantic version (if any versions are tagged)
/// - All available semantic versions in descending order
///
/// The response does not include special tags ("latest", "dev") as these are system-managed
/// and can be queried via the versions endpoint for specific datasets.
///
/// Results are ordered by namespace then by name (lexicographical).
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/datasets",
        tag = "datasets",
        operation_id = "list_all_datasets",
        responses(
            (status = 200, description = "Successfully retrieved all datasets", body = DatasetsResponse),
            (status = 500, description = "Internal server error", body = ErrorResponse)
        )
    )
)]
pub async fn handler(State(ctx): State<Ctx>) -> Result<Json<DatasetsResponse>, ErrorResponse> {
    // Query all tags from dataset store
    let tags = ctx
        .dataset_store
        .list_all_datasets()
        .await
        .map_err(Error::ListAllDatasets)?;

    // Group tags by (namespace, name) and aggregate versions
    let mut datasets_map: std::collections::HashMap<(String, String), Vec<Version>> =
        std::collections::HashMap::new();

    for tag in tags {
        let namespace_str = tag.namespace.into_inner();
        let name_str = tag.name.into_inner();
        let version_str = tag.version.into_inner();

        // Parse version string into Version type
        if let Ok(version) = version_str.parse::<Version>() {
            datasets_map
                .entry((namespace_str.clone(), name_str.clone()))
                .or_default()
                .push(version);
        }
    }

    // Convert map to DatasetSummary list
    let mut datasets: Vec<DatasetSummary> = datasets_map
        .into_iter()
        .map(|((namespace_str, name_str), mut versions)| {
            // Sort versions descending
            versions.sort();
            versions.reverse();

            let latest_version = versions.first().cloned();

            DatasetSummary {
                namespace: namespace_str
                    .parse()
                    .expect("valid namespace from database"),
                name: name_str.parse().expect("valid name from database"),
                latest_version,
                versions,
            }
        })
        .collect();

    // Sort datasets by namespace then name
    datasets.sort_by(|a, b| {
        a.namespace
            .as_str()
            .cmp(b.namespace.as_str())
            .then_with(|| a.name.as_str().cmp(b.name.as_str()))
    });

    Ok(Json(DatasetsResponse { datasets }))
}

/// Response for listing all datasets
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct DatasetsResponse {
    /// List of all datasets across all namespaces
    pub datasets: Vec<DatasetSummary>,
}

/// Summary information for a single dataset
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct DatasetSummary {
    /// Dataset namespace
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub namespace: Namespace,
    /// Dataset name
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub name: Name,
    /// Latest semantic version (if any)
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "utoipa", schema(value_type = Option<String>))]
    pub latest_version: Option<Version>,
    /// All semantic versions (sorted descending)
    #[cfg_attr(feature = "utoipa", schema(value_type = Vec<String>))]
    pub versions: Vec<Version>,
}

/// Errors that can occur when listing datasets
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Dataset store operation error when listing all datasets
    ///
    /// This occurs when:
    /// - Failed to query all datasets from the dataset store
    /// - Database connection issues
    /// - Internal database errors
    #[error("Failed to list all datasets: {0}")]
    ListAllDatasets(#[source] ListAllDatasetsError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::ListAllDatasets(_) => "LIST_ALL_DATASETS_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::ListAllDatasets(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
