use datasets_common::hash::Hash;

use crate::manifests::StoreError;

/// Errors specific to manifest registration operations
///
/// This error type is used by `DatasetsRegistry::register_manifest()`.
#[derive(Debug, thiserror::Error)]
pub enum RegisterManifestError {
    /// Failed to store manifest in dataset definitions store
    ///
    /// This occurs when the object store operation to save the manifest file fails.
    /// The manifest file is stored in the dataset definitions object store before being
    /// registered in the metadata database.
    ///
    /// Common causes:
    /// - Object store connection failures (S3, GCS, local filesystem, etc.)
    /// - Network issues when writing to remote storage
    /// - Permission problems (filesystem permissions, cloud IAM roles)
    /// - Disk full on local storage
    /// - Invalid storage path or bucket configuration
    ///
    /// The operation can be retried as no partial state is persisted if storage fails.
    #[error("Failed to store manifest in dataset definitions store")]
    ManifestStorage(#[source] StoreError),

    /// Failed to register manifest in metadata database
    ///
    /// This occurs when the database operation to register the manifest metadata fails.
    /// This happens after the manifest file has been successfully stored in the object store.
    ///
    /// Common causes:
    /// - Database connection issues
    /// - Database unavailability or timeouts
    /// - Permission problems (database user lacks INSERT privileges)
    /// - Database constraint violations (e.g., duplicate manifest hash)
    /// - Transaction conflicts with concurrent operations
    ///
    /// If this error occurs, the manifest file exists in the object store but is not
    /// registered in the metadata database. The operation can be retried - duplicate
    /// manifest hashes will be handled by database constraints.
    #[error("Failed to register manifest in metadata database")]
    MetadataRegistration(#[source] metadata_db::Error),
}

/// Errors that can occur when retrieving a manifest by hash
///
/// This error type is used by `DatasetsRegistry::get_manifest()`.
#[derive(Debug, thiserror::Error)]
pub enum GetManifestError {
    /// Failed to query manifest path from metadata database
    #[error("Failed to query manifest path from metadata database")]
    MetadataDbQueryPath(#[source] metadata_db::Error),

    /// Failed to retrieve manifest from object store
    #[error("Failed to retrieve manifest from object store")]
    ObjectStoreError(#[source] crate::manifests::GetError),
}

/// Errors that can occur when deleting a manifest
///
/// This error type is used by `DatasetsRegistry::delete_manifest()`.
#[derive(Debug, thiserror::Error)]
pub enum DeleteManifestError {
    /// Manifest is linked to one or more datasets and cannot be deleted
    ///
    /// Manifests must be unlinked from all datasets before deletion.
    #[error("Manifest is linked to datasets and cannot be deleted")]
    ManifestLinked,

    /// Failed to begin transaction
    ///
    /// This error occurs when the database connection fails to start a transaction,
    /// typically due to connection issues, database unavailability, or permission problems.
    #[error("Failed to begin transaction")]
    TransactionBegin(#[source] metadata_db::Error),

    /// Failed to check if manifest is linked to datasets
    #[error("Failed to check if manifest is linked to datasets")]
    MetadataDbCheckLinks(#[source] metadata_db::Error),

    /// Failed to delete manifest from metadata database
    #[error("Failed to delete manifest from metadata database")]
    MetadataDbDelete(#[source] metadata_db::Error),

    /// Failed to delete manifest from object store
    #[error("Failed to delete manifest from object store")]
    ObjectStoreError(#[source] crate::manifests::DeleteError),

    /// Failed to commit transaction after successful database operations
    ///
    /// When a commit fails, PostgreSQL guarantees that all changes are rolled back.
    /// The manifest deletion was not persisted to the database.
    ///
    /// Possible causes:
    /// - Database connection lost during commit
    /// - Transaction conflict with concurrent operations (serialization failure)
    /// - Database constraint violation detected at commit time
    /// - Database running out of disk space or resources
    ///
    /// The operation is safe to retry from the beginning as no partial state was persisted.
    #[error("Failed to commit transaction")]
    TransactionCommit(#[source] metadata_db::Error),
}

/// Errors that occur when listing datasets that use a specific manifest
///
/// This error type is used by `DatasetsRegistry::list_manifest_linked_datasets()`.
#[derive(Debug, thiserror::Error)]
pub enum ListDatasetsUsingManifestError {
    /// Failed to query manifest path from metadata database
    ///
    /// This occurs when:
    /// - Database connection is lost
    /// - SQL query to resolve manifest hash to file path fails
    /// - Database schema inconsistencies prevent path lookup
    #[error("Failed to query manifest path from metadata database")]
    MetadataDbQueryPath(#[source] metadata_db::Error),

    /// Failed to list dataset tags from metadata database
    ///
    /// This occurs when:
    /// - Database connection is lost
    /// - SQL query to retrieve dataset tags fails
    /// - Database schema inconsistencies prevent tag retrieval
    #[error("Failed to list dataset tags from metadata database")]
    MetadataDbListTags(#[source] metadata_db::Error),
}

/// Error when listing orphaned manifests
///
/// This error type is used by `DatasetsRegistry::list_orphaned_manifests()`.
///
/// This occurs when failing to query orphaned manifests from the metadata database,
/// typically due to database connection issues, unavailability, or permission problems.
#[derive(Debug, thiserror::Error)]
#[error("Failed to list orphaned manifests from metadata database")]
pub struct ListOrphanedManifestsError(#[source] pub metadata_db::Error);

/// Error when listing all registered manifests
///
/// This error type is used by `DatasetsRegistry::list_all_manifests()`.
///
/// This occurs when failing to query all manifests from the metadata database,
/// typically due to database connection issues, unavailability, or permission problems.
#[derive(Debug, thiserror::Error)]
#[error("Failed to list all manifests from metadata database")]
pub struct ListAllManifestsError(#[source] pub metadata_db::Error);

/// Errors specific to manifest linking operations
#[derive(Debug, thiserror::Error)]
pub enum LinkManifestError {
    /// Manifest does not exist in the system
    ///
    /// This occurs when attempting to link a manifest hash that hasn't been registered.
    /// The manifest must be registered first via `register_manifest` before it can be
    /// linked to a dataset.
    ///
    /// This error is detected via foreign key constraint violation (PostgreSQL error code 23503)
    /// when the database rejects the link operation due to the missing manifest.
    #[error("Manifest with hash '{0}' does not exist")]
    ManifestNotFound(Hash),

    /// Failed to begin transaction
    ///
    /// This occurs when the database connection fails to start a transaction,
    /// typically due to connection issues, database unavailability, or permission problems.
    #[error("Failed to begin transaction")]
    TransactionBegin(#[source] metadata_db::Error),

    /// Failed to link manifest to dataset in metadata database
    ///
    /// This occurs when the database operation to create the manifest-dataset link fails,
    /// typically due to:
    /// - Database connection issues during the operation
    /// - Permission problems
    /// - Other database errors
    ///
    /// Note: Foreign key constraint violations (manifest doesn't exist) are handled separately
    /// as `ManifestNotFound` errors.
    #[error("Failed to link manifest to dataset in metadata database")]
    LinkManifestToDataset(#[source] metadata_db::Error),

    /// Failed to set dev tag for dataset
    ///
    /// This occurs when the database operation to update the dev tag fails,
    /// typically due to:
    /// - Database connection issues during the operation
    /// - Permission problems
    /// - Constraint violations
    #[error("Failed to set dev tag for dataset")]
    SetDevTag(#[source] metadata_db::Error),

    /// Failed to commit transaction after successful database operations
    ///
    /// When a commit fails, PostgreSQL guarantees that all changes are rolled back.
    /// None of the operations in the transaction (linking manifest and updating dev tag)
    /// were persisted to the database.
    ///
    /// Possible causes:
    /// - Database connection lost during commit
    /// - Transaction conflict with concurrent operations (serialization failure)
    /// - Database constraint violation detected at commit time
    /// - Database running out of disk space or resources
    ///
    /// The operation is safe to retry from the beginning as no partial state was persisted.
    #[error("Failed to commit transaction")]
    TransactionCommit(#[source] metadata_db::Error),
}

/// Error when checking if a manifest is linked to a dataset
///
/// This error type is used by `DatasetsRegistry::is_manifest_linked()`.
///
/// This occurs when the database query to check manifest linkage fails,
/// typically due to database connection issues, unavailability, or permission problems.
#[derive(Debug, thiserror::Error)]
#[error("Failed to check if manifest is linked to dataset")]
pub struct IsManifestLinkedError(#[source] pub metadata_db::Error);

/// Errors specific to setting semantic version tags for dataset manifests
///
/// These errors occur during the `set_dataset_version_tag` operation, which creates
/// or updates a semantic version tag and automatically updates the "latest" tag if needed.
#[derive(Debug, thiserror::Error)]
pub enum SetVersionTagError {
    /// Failed to begin transaction or execute database operations
    ///
    /// This error occurs when:
    /// - The database connection is unavailable or times out
    /// - The manifest hash does not exist (foreign key constraint violation)
    /// - The dataset namespace/name combination doesn't exist
    /// - Database permissions prevent the upsert operation
    /// - Failed to begin the transaction
    /// - Failed to register version tag or query latest tag
    ///
    /// The operation may be retried as it's idempotent. If the manifest hash
    /// is invalid, ensure the manifest is registered first via `register_manifest`.
    #[error("Failed to execute database operations")]
    MetadataDb(#[source] metadata_db::Error),

    /// Failed to update the "latest" tag to point to the highest version
    ///
    /// This error occurs when:
    /// - The database connection fails during the latest tag update
    /// - A transaction conflict occurs if another process updates latest simultaneously
    /// - Database permissions prevent updating the latest tag
    /// - The manifest hash resolved as highest no longer exists (rare edge case)
    ///
    /// This happens after successfully registering the version tag and determining
    /// that the "latest" tag needs to be updated. The error occurs before commit,
    /// so no changes have been persisted yet.
    #[error("Failed to update latest tag in metadata database")]
    UpdateLatestTag(#[source] metadata_db::Error),

    /// Failed to commit transaction after successful database operations
    ///
    /// When a commit fails, PostgreSQL guarantees that all changes are rolled back.
    /// None of the operations in the transaction (version tag registration and latest
    /// tag update) were persisted to the database.
    ///
    /// Possible causes:
    /// - Database connection lost during commit
    /// - Transaction conflict with concurrent operations (serialization failure)
    /// - Database constraint violation detected at commit time
    /// - Database running out of disk space or resources
    ///
    /// The operation is safe to retry from the beginning as no partial state was persisted.
    #[error("Failed to commit transaction")]
    TransactionCommit(#[source] metadata_db::Error),
}

/// Error when resolving revision references to manifest hashes
///
/// This error occurs during the `resolve_revision` operation, which converts
/// revision references (version tags, hashes, or special tags) into concrete manifest hashes.
///
/// This occurs when:
/// - The database connection is unavailable or times out
/// - Database permissions prevent the query operation
/// - Database query syntax or logic error (should be rare)
///
/// The operation may be retried as it's read-only.
#[derive(Debug, thiserror::Error)]
#[error("Failed to query metadata database")]
pub struct ResolveRevisionError(#[source] pub metadata_db::Error);

/// Error when listing version tags for a dataset
///
/// This error type is used by `DatasetsRegistry::list_version_tags()`.
///
/// This occurs when failing to query version tags from the metadata database,
/// typically due to database connection issues, unavailability, or permission problems.
#[derive(Debug, thiserror::Error)]
#[error("Failed to list version tags from metadata database")]
pub struct ListVersionTagsError(#[source] pub metadata_db::Error);

/// Error when listing all datasets
///
/// This error type is used by `DatasetsRegistry::list_all_datasets()`.
///
/// This occurs when failing to query all datasets from the metadata database,
/// typically due to database connection issues, unavailability, or permission problems.
#[derive(Debug, thiserror::Error)]
#[error("Failed to list all datasets from metadata database")]
pub struct ListAllDatasetsError(#[source] pub metadata_db::Error);

/// Error when unlinking dataset manifests
///
/// This error type is used by `DatasetsRegistry::unlink_dataset_manifests()`.
///
/// This occurs when failing to delete dataset manifest links from the metadata database,
/// typically due to database connection issues, unavailability, or permission problems.
#[derive(Debug, thiserror::Error)]
#[error("Failed to delete dataset manifest links from metadata database")]
pub struct UnlinkDatasetManifestsError(#[source] pub metadata_db::Error);

/// Error when deleting a version tag
///
/// This error type is used by `DatasetsRegistry::delete_version_tag()`.
///
/// This occurs when failing to delete a version tag from the metadata database,
/// typically due to database connection issues, unavailability, or permission problems.
#[derive(Debug, thiserror::Error)]
#[error("Failed to delete version tag from metadata database")]
pub struct DeleteVersionTagError(#[source] pub metadata_db::Error);
