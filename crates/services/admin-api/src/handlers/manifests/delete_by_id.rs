//! Manifests delete by ID handler

use amp_datasets_registry::error::DeleteManifestError;
use axum::{
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use datasets_common::hash::Hash;
use monitoring::logging;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `DELETE /manifests/{hash}` endpoint
///
/// Deletes a manifest from both object store and metadata database.
/// **Manifests linked to datasets cannot be deleted** (returns 409 Conflict).
///
/// This endpoint is idempotent: deleting a non-existent manifest returns success (204 No Content).
///
/// ## Path Parameters
/// - `hash`: Manifest content hash to delete
///
/// ## Response
/// - **204 No Content**: Manifest successfully deleted (or already deleted)
/// - **400 Bad Request**: Invalid manifest hash format
/// - **409 Conflict**: Manifest is linked to datasets and cannot be deleted
/// - **500 Internal Server Error**: Store or database error
///
/// ## Error Codes
/// - `INVALID_HASH`: Invalid hash format
/// - `MANIFEST_LINKED`: Manifest is linked to datasets and cannot be deleted
/// - `MANIFEST_DELETE_ERROR`: Failed to delete manifest
///
/// ## Deletion Flow
/// This handler:
/// 1. Validates and extracts the manifest hash from the URL path
/// 2. Checks if the manifest is linked to any datasets
/// 3. If linked: Returns 409 Conflict error (deletion not allowed)
/// 4. If not linked:
///    - Deletes manifest record from metadata database
///    - Deletes manifest file from object store
///    - Treats "not found" as success (idempotent behavior)
/// 5. Returns 204 No Content on success
///
/// ## Safety Notes
/// - Only unlinked manifests can be deleted (no dataset dependencies)
/// - To delete a linked manifest, first remove all dataset associations
/// - Deletion is permanent and cannot be undone
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        delete,
        path = "/manifests/{hash}",
        tag = "manifests",
        operation_id = "manifests_delete",
        params(
            ("hash" = String, Path, description = "Manifest content hash")
        ),
        responses(
            (status = 204, description = "Manifest successfully deleted (or already deleted)"),
            (status = 400, description = "Invalid hash", body = crate::handlers::error::ErrorResponse),
            (status = 409, description = "Manifest linked to datasets", body = crate::handlers::error::ErrorResponse),
            (status = 500, description = "Internal server error", body = crate::handlers::error::ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<Hash>, PathRejection>,
) -> Result<StatusCode, ErrorResponse> {
    let hash = match path {
        Ok(Path(hash)) => hash,
        Err(err) => {
            tracing::debug!(error = %err, error_source = logging::error_source(&err), "invalid manifest hash in path");
            return Err(Error::InvalidHash(err).into());
        }
    };

    // Delete manifest using dataset store
    match ctx.dataset_store.delete_manifest(&hash).await {
        Ok(()) => {
            tracing::info!(
                manifest_hash = %hash,
                "successfully deleted manifest"
            );
        }
        Err(DeleteManifestError::ManifestLinked) => {
            tracing::debug!(
                manifest_hash = %hash,
                "manifest is linked to datasets, cannot delete"
            );
            return Err(Error::ManifestLinked { hash }.into());
        }
        Err(DeleteManifestError::TransactionBegin(err)) => {
            tracing::error!(
                manifest_hash = %hash,
                error = %err, error_source = logging::error_source(&err),
                "failed to begin transaction"
            );
            return Err(Error::TransactionBeginError(err).into());
        }
        Err(DeleteManifestError::MetadataDbCheckLinks(err)) => {
            tracing::error!(
                manifest_hash = %hash,
                error = %err, error_source = logging::error_source(&err),
                "failed to check if manifest is linked"
            );
            return Err(Error::CheckLinksError(err).into());
        }
        Err(DeleteManifestError::MetadataDbDelete(err)) => {
            tracing::error!(
                manifest_hash = %hash,
                error = %err, error_source = logging::error_source(&err),
                "failed to delete manifest from metadata database"
            );
            return Err(Error::MetadataDbDeleteError(err).into());
        }
        Err(DeleteManifestError::ObjectStoreError(err)) => {
            tracing::error!(
                manifest_hash = %hash,
                error = %err, error_source = logging::error_source(&err),
                "failed to delete manifest from object store"
            );
            return Err(Error::ObjectStoreDeleteError(err).into());
        }
        Err(DeleteManifestError::TransactionCommit(err)) => {
            tracing::error!(
                manifest_hash = %hash,
                error = %err, error_source = logging::error_source(&err),
                "failed to commit transaction"
            );
            return Err(Error::TransactionCommitError(err).into());
        }
    }

    Ok(StatusCode::NO_CONTENT)
}

/// Errors that can occur during manifest deletion
///
/// This enum represents all possible error conditions that can occur
/// when handling a `DELETE /manifests/{hash}` request.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The manifest hash in the URL path is invalid
    ///
    /// This occurs when:
    /// - The hash contains invalid characters or doesn't follow hash format conventions
    /// - The hash is empty or malformed
    /// - Path parameter extraction fails for the hash (deserialization error)
    #[error("invalid manifest hash: {0}")]
    InvalidHash(PathRejection),

    /// The manifest is linked to one or more datasets
    ///
    /// This occurs when:
    /// - The manifest is currently referenced by one or more dataset versions
    /// - Attempting to delete a manifest that has active dataset dependencies
    /// - Manifests must be unlinked from all datasets before deletion is allowed
    #[error("manifest '{hash}' is linked to datasets and cannot be deleted")]
    ManifestLinked { hash: Hash },

    /// Failed to begin transaction
    ///
    /// This occurs when:
    /// - Database connection is not available or lost
    /// - Transaction isolation level cannot be established
    /// - Database resource limits prevent new transactions
    #[error("failed to begin transaction: {0}")]
    TransactionBeginError(#[source] metadata_db::Error),

    /// Failed to check if manifest is linked to datasets
    ///
    /// This occurs when:
    /// - Database query to check manifest references fails
    /// - SQL execution errors during link validation
    /// - Database connection is lost during the check
    #[error("failed to check if manifest is linked to datasets: {0}")]
    CheckLinksError(#[source] metadata_db::Error),

    /// Failed to delete manifest from metadata database
    ///
    /// This occurs when:
    /// - Database delete operation fails
    /// - Database constraints prevent deletion
    /// - Connection is lost during the delete operation
    #[error("failed to delete manifest from metadata database: {0}")]
    MetadataDbDeleteError(#[source] metadata_db::Error),

    /// Failed to delete manifest from object store
    ///
    /// This occurs when:
    /// - Object store is not accessible or connection fails
    /// - Delete permissions are insufficient
    /// - Network errors prevent deletion from remote storage
    /// - File does not exist in object store (non-critical if DB delete succeeded)
    #[error("failed to delete manifest from object store: {0}")]
    ObjectStoreDeleteError(#[source] amp_datasets_registry::manifests::DeleteError),

    /// Failed to commit transaction
    ///
    /// This occurs when:
    /// - Database transaction commit fails after all operations succeed
    /// - Connection is lost during commit
    /// - Database lock conflicts prevent commit
    /// - Transaction timeout occurs
    #[error("failed to commit transaction: {0}")]
    TransactionCommitError(#[source] metadata_db::Error),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidHash(_) => "INVALID_HASH",
            Error::ManifestLinked { .. } => "MANIFEST_LINKED",
            Error::TransactionBeginError(_) => "MANIFEST_DELETE_TRANSACTION_BEGIN_ERROR",
            Error::CheckLinksError(_) => "MANIFEST_DELETE_CHECK_LINKS_ERROR",
            Error::MetadataDbDeleteError(_) => "MANIFEST_DELETE_METADATA_DB_ERROR",
            Error::ObjectStoreDeleteError(_) => "MANIFEST_DELETE_OBJECT_STORE_ERROR",
            Error::TransactionCommitError(_) => "MANIFEST_DELETE_TRANSACTION_COMMIT_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidHash(_) => StatusCode::BAD_REQUEST,
            Error::ManifestLinked { .. } => StatusCode::CONFLICT,
            Error::TransactionBeginError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::CheckLinksError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::MetadataDbDeleteError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::ObjectStoreDeleteError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::TransactionCommitError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

// Note: ManifestLinked variant is not converted here because it's handled
// explicitly in the handler to include the hash in the error message
