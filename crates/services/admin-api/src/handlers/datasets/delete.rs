use axum::{
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use dataset_store::DeleteManifestError;
use datasets_common::{name::Name, namespace::Namespace};
use futures::stream::{FuturesUnordered, StreamExt};
use monitoring::logging;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `DELETE /datasets/{namespace}/{name}` endpoint
///
/// Removes all manifest links and version tags for a dataset.
///
/// ## Response
/// - **204 No Content**: Dataset successfully deleted (or didn't exist)
/// - **400 Bad Request**: Invalid path parameters
/// - **500 Internal Server Error**: Database operation error
///
/// ## Error Codes
/// - `INVALID_PATH`: Invalid namespace or name in path parameters
/// - `UNLINK_DATASET_MANIFESTS_ERROR`: Failed to unlink dataset manifests from dataset store
///
/// ## Behavior
/// This endpoint deletes all metadata for a dataset including:
/// - All manifest links in the dataset_manifests table
/// - All version tags (cascaded automatically via foreign key constraint)
/// - Orphaned manifest files (manifests not referenced by any other dataset)
///
/// This operation is fully idempotent - it returns 204 even if the dataset
/// doesn't exist. Manifests that are still referenced by other datasets are
/// preserved.
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        delete,
        path = "/datasets/{namespace}/{name}",
        tag = "datasets",
        operation_id = "delete_dataset",
        params(
            ("namespace" = String, Path, description = "Dataset namespace"),
            ("name" = String, Path, description = "Dataset name")
        ),
        responses(
            (status = 204, description = "Dataset successfully deleted"),
            (status = 400, description = "Invalid path parameters", body = ErrorResponse),
            (status = 500, description = "Internal server error", body = ErrorResponse)
        )
    )
)]
pub async fn handler(
    path: Result<Path<(Namespace, Name)>, PathRejection>,
    State(ctx): State<Ctx>,
) -> Result<StatusCode, ErrorResponse> {
    let (namespace, name) = match path {
        Ok(Path((namespace, name))) => (namespace, name),
        Err(err) => {
            tracing::debug!(error = %err, error_source = logging::error_source(&err), "invalid path parameters");
            return Err(Error::InvalidPath(err).into());
        }
    };

    tracing::debug!(
        namespace=%namespace,
        name=%name,
        "deleting dataset"
    );

    // Step 1: Unlink all manifests from the dataset (idempotent)
    // This will delete all dataset_manifests links (which cascades to delete tags)
    let unlinked_hashes = ctx
        .dataset_store
        .unlink_dataset_manifests(&namespace, &name)
        .await
        .map_err(Error::UnlinkDatasetManifests)?;

    tracing::debug!(
        namespace=%namespace,
        name=%name,
        unlinked_count=%unlinked_hashes.len(),
        "Unlinked dataset manifests"
    );

    // Delete each unlinked manifest concurrently (if any)
    // Each manifest deletion runs in its own transaction
    if unlinked_hashes.is_empty() {
        tracing::debug!(
            namespace=%namespace,
            name=%name,
            "No manifests to delete, dataset deletion completed"
        );
        return Ok(StatusCode::NO_CONTENT);
    }

    let deletion_futures = FuturesUnordered::new();
    for hash in unlinked_hashes {
        let store = ctx.dataset_store.clone();

        deletion_futures.push(async move {
            if let Err(err) = store.delete_manifest(&hash).await {
                match err {
                    DeleteManifestError::ManifestLinked => {
                        // No-op
                    }
                    DeleteManifestError::TransactionBegin(err) => {
                        tracing::warn!(
                            manifest_hash=%hash,
                            error=%err, error_source = logging::error_source(&err),
                            "Failed to begin transaction for manifest deletion"
                        );
                    }
                    DeleteManifestError::MetadataDbCheckLinks(err) => {
                        tracing::warn!(
                            manifest_hash=%hash,
                            error=%err, error_source = logging::error_source(&err),
                            "Failed to check remaining manifest links"
                        );
                    }
                    DeleteManifestError::MetadataDbDelete(err) => {
                        tracing::warn!(
                            manifest_hash=%hash,
                            error=%err, error_source = logging::error_source(&err),
                            "Failed to delete manifest from metadata database"
                        );
                    }
                    DeleteManifestError::ObjectStoreError(err) => {
                        tracing::warn!(
                            manifest_hash=%hash,
                            error=%err, error_source = logging::error_source(&err),
                            "Failed to delete manifest file from object store, keeping in database"
                        );
                    }
                    DeleteManifestError::TransactionCommit(err) => {
                        tracing::warn!(
                            manifest_hash=%hash,
                            error=%err, error_source = logging::error_source(&err),
                            "Failed to commit manifest deletion transaction"
                        );
                    }
                }
                return;
            }

            tracing::debug!(manifest_hash=%hash, "Manifest deleted successfully");
        });
    }

    // Wait for all deletions to complete
    deletion_futures.collect::<Vec<_>>().await;

    tracing::debug!(
        namespace=%namespace,
        name=%name,
        "Dataset deletion completed"
    );

    Ok(StatusCode::NO_CONTENT)
}

/// Errors that can occur when deleting a dataset
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid path parameters
    ///
    /// This occurs when:
    /// - The namespace or name in the URL path is invalid
    /// - Path parameter parsing fails
    #[error("Invalid path parameters: {0}")]
    InvalidPath(#[source] PathRejection),
    /// Dataset store operation error when unlinking dataset manifests
    ///
    /// This occurs when:
    /// - Failed to delete dataset manifest links from database
    /// - Database connection or transaction issues
    #[error("Failed to unlink dataset manifests: {0}")]
    UnlinkDatasetManifests(#[source] dataset_store::UnlinkDatasetManifestsError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidPath(_) => "INVALID_PATH",
            Error::UnlinkDatasetManifests(_) => "UNLINK_DATASET_MANIFESTS_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidPath(_) => StatusCode::BAD_REQUEST,
            Error::UnlinkDatasetManifests(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
