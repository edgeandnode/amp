//! Manifests prune handler

use axum::{Json, extract::State, http::StatusCode};
use dataset_store::DeleteManifestError;
use futures::stream::{FuturesUnordered, StreamExt};
use monitoring::logging;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `DELETE /manifests` endpoint
///
/// Deletes all orphaned manifests (manifests with no dataset links).
/// This is a bulk cleanup operation for removing unused manifests and reclaiming storage space.
///
/// ## Response
/// - **200 OK**: Returns JSON with count of deleted manifests
/// - **500 Internal Server Error**: Database error
///
/// ## Error Codes
/// - `LIST_ORPHANED_MANIFESTS_ERROR`: Failed to list orphaned manifests
///
/// ## Pruning Process
/// This handler:
/// 1. Queries the metadata database for all manifests not linked to any datasets
/// 2. Deletes each orphaned manifest concurrently from both object store and metadata database
/// 3. Logs individual deletion failures but continues processing remaining manifests
/// 4. Returns the count of successfully deleted manifests
///
/// Individual manifest deletion failures are logged as warnings but don't fail the entire operation,
/// allowing partial cleanup even if some manifests cannot be removed.
/// The operation is idempotent - safe to call repeatedly.
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        delete,
        path = "/manifests",
        tag = "manifests",
        operation_id = "manifests_prune",
        responses(
            (status = 200, description = "Orphaned manifests pruned successfully", body = PruneResponse),
            (status = 500, description = "Internal server error", body = crate::handlers::error::ErrorResponse)
        )
    )
)]
pub async fn handler(State(ctx): State<Ctx>) -> Result<Json<PruneResponse>, ErrorResponse> {
    tracing::debug!("pruning orphaned manifests");

    // List all orphaned manifests
    let orphaned_hashes = ctx
        .dataset_store
        .list_orphaned_manifests()
        .await
        .map_err(|err| {
            tracing::error!(
                error = %err, error_source = logging::error_source(&err),
                "failed to list orphaned manifests"
            );
            Error(err)
        })?;

    if orphaned_hashes.is_empty() {
        tracing::debug!("no orphaned manifests found");
        return Ok(Json(PruneResponse { deleted_count: 0 }));
    }

    tracing::debug!(
        orphaned_count = orphaned_hashes.len(),
        "found orphaned manifests, deleting concurrently"
    );

    // Delete each orphaned manifest concurrently
    let deletion_futures = FuturesUnordered::new();
    for hash in &orphaned_hashes {
        let store = ctx.dataset_store.clone();
        let hash = hash.clone();

        deletion_futures.push(async move {
            if let Err(err) = store.delete_manifest(&hash).await {
                match err {
                    DeleteManifestError::ManifestLinked => {
                        // Manifest was linked after we listed orphans (race condition)
                        tracing::debug!(
                            manifest_hash = %hash,
                            "manifest became linked, skipping deletion"
                        );
                    }
                    DeleteManifestError::TransactionBegin(err) => {
                        tracing::warn!(
                            manifest_hash = %hash,
                            error = %err, error_source = logging::error_source(&err),
                            "failed to begin transaction for manifest deletion"
                        );
                    }
                    DeleteManifestError::MetadataDbCheckLinks(err) => {
                        tracing::warn!(
                            manifest_hash = %hash,
                            error = %err, error_source = logging::error_source(&err),
                            "failed to check remaining manifest links"
                        );
                    }
                    DeleteManifestError::MetadataDbDelete(err) => {
                        tracing::warn!(
                            manifest_hash = %hash,
                            error = %err, error_source = logging::error_source(&err),
                            "failed to delete manifest from metadata database"
                        );
                    }
                    DeleteManifestError::ObjectStoreError(err) => {
                        tracing::warn!(
                            manifest_hash = %hash,
                            error = %err, error_source = logging::error_source(&err),
                            "failed to delete manifest file from object store"
                        );
                    }
                    DeleteManifestError::TransactionCommit(err) => {
                        tracing::warn!(
                            manifest_hash = %hash,
                            error = %err, error_source = logging::error_source(&err),
                            "failed to commit manifest deletion transaction"
                        );
                    }
                }
                return false; // Deletion failed
            }

            tracing::debug!(manifest_hash = %hash, "manifest deleted successfully");
            true // Deletion succeeded
        });
    }

    // Wait for all deletions to complete and count successes
    let results: Vec<bool> = deletion_futures.collect().await;
    let deleted_count = results.iter().filter(|&&success| success).count();

    tracing::info!(
        total_orphaned = orphaned_hashes.len(),
        deleted_count = deleted_count,
        "orphaned manifest pruning completed"
    );

    Ok(Json(PruneResponse { deleted_count }))
}

/// Response payload for manifest pruning operation
///
/// Contains the count of successfully deleted orphaned manifests.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct PruneResponse {
    /// Number of orphaned manifests successfully deleted
    pub deleted_count: usize,
}

/// Error for manifest pruning operations.
///
/// This error occurs when failing to list orphaned manifests, which can happen when:
/// - Database connection is lost
/// - Failed to query orphaned manifests
/// - Database errors during query
#[derive(Debug, thiserror::Error)]
#[error("failed to list orphaned manifests")]
pub struct Error(#[source] pub dataset_store::ListOrphanedManifestsError);

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        "LIST_ORPHANED_MANIFESTS_ERROR"
    }

    fn status_code(&self) -> StatusCode {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}
