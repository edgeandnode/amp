use std::collections::BTreeSet;

use datasets_common::{
    hash::Hash, hash_reference::HashReference, name::Name, namespace::Namespace,
    reference::Reference, revision::Revision, version::Version,
};
use metadata_db::MetadataDb;

use crate::{
    error::{
        DeleteManifestError, DeleteVersionTagError, GetManifestError, IsManifestLinkedError,
        LinkManifestError, ListAllDatasetsError, ListAllManifestsError,
        ListDatasetsUsingManifestError, ListOrphanedManifestsError, ListVersionTagsError,
        RegisterManifestError, ResolveRevisionError, SetVersionTagError,
        UnlinkDatasetManifestsError,
    },
    manifests::{DatasetManifestsStore, ManifestContent, ManifestPath},
};
pub mod error;
pub mod manifests;

/// Datasets registry.
///
/// The authoritative local registry for managing dataset revisions and version tags.
///
/// There are a few things it helps us with:
/// - Stores and retrieves dataset manifests (content-addressed by hash).
/// - Manages version tags: semantic versions, "latest", and "dev" pointers.
/// - Tracks dataset-to-manifest relationships (linking/unlinking).
/// - Resolves revision references to concrete manifest hashes.
/// - Provides garbage collection queries for orphaned manifests.
/// - Ensures consistency during concurrent operations via transactions.
///
/// The registry is the complete local inventory: it stores manifests and tracks
/// how they're named/tagged/linked, but has no knowledge of *what's inside* those
/// manifests or *how* to use them.
#[derive(Clone)]
pub struct DatasetsRegistry {
    metadata_db: MetadataDb,
    store: DatasetManifestsStore,
}

// Manifest management APIs
impl DatasetsRegistry {
    /// Creates a new datasets registry.
    ///
    /// # Arguments
    ///
    /// * `metadata_db` - Database connection for storing manifest metadata and version tags.
    /// * `store` - Object store for manifest file storage (content-addressed by hash).
    ///
    /// The registry combines database operations (tracking which manifests exist and their
    /// version tags) with object store operations (storing/retrieving manifest content).
    pub fn new(metadata_db: MetadataDb, store: DatasetManifestsStore) -> Self {
        Self { metadata_db, store }
    }

    /// Store a manifest in both object store and metadata database without linking to datasets
    ///
    /// Does NOT create version tags or link to datasets. Idempotent (content-addressable storage
    /// + `ON CONFLICT DO NOTHING`). If DB registration fails after object store write, retry is safe.
    pub async fn register_manifest(
        &self,
        hash: &Hash,
        content: String,
    ) -> Result<(), RegisterManifestError> {
        let path = self
            .store
            .store(hash, content)
            .await
            .map_err(RegisterManifestError::ManifestStorage)?;
        metadata_db::manifests::register(&self.metadata_db, hash, path)
            .await
            .map_err(RegisterManifestError::MetadataRegistration)?;
        Ok(())
    }

    /// Retrieve a manifest by its content hash
    ///
    /// Resolves hash to file path via metadata database, then fetches content from object store.
    /// Returns `None` if manifest not found in DB or object store.
    pub async fn get_manifest(
        &self,
        hash: &Hash,
    ) -> Result<Option<ManifestContent>, GetManifestError> {
        let Some(path) = metadata_db::manifests::get_path(&self.metadata_db, hash)
            .await
            .map_err(GetManifestError::MetadataDbQueryPath)?
            .map(ManifestPath::from)
        else {
            return Ok(None);
        };

        let content = self
            .store
            .get(path)
            .await
            .map_err(GetManifestError::ObjectStoreError)?;

        Ok(content)
    }

    /// Delete a manifest from both metadata database and object store
    ///
    /// Uses transaction with `SELECT FOR UPDATE` to check links before deletion, preventing
    /// concurrent link creation. Returns `ManifestLinked` error if linked to any datasets.
    /// Idempotent (returns `Ok(())` if not found). Deletes from object store before commit.
    pub async fn delete_manifest(&self, hash: &Hash) -> Result<(), DeleteManifestError> {
        // Begin transaction for atomic check-and-delete
        let mut tx = self
            .metadata_db
            .begin_txn()
            .await
            .map_err(DeleteManifestError::TransactionBegin)?;

        // Check if manifest has remaining links (with row-level locking)
        // This prevents concurrent processes from linking the manifest until we commit
        let links = metadata_db::manifests::count_dataset_links_and_lock(&mut tx, hash)
            .await
            .map_err(DeleteManifestError::MetadataDbCheckLinks)?;

        if links > 0 {
            // No need to rollback explicitly - transaction drops on return
            return Err(DeleteManifestError::ManifestLinked);
        }

        // Delete from metadata database (CASCADE deletes links/tags)
        // Lock is still held, so no concurrent links can be created
        let Some(path) = metadata_db::manifests::delete(&mut tx, hash)
            .await
            .map_err(DeleteManifestError::MetadataDbDelete)?
            .map(Into::into)
        else {
            // Treat not found as success (idempotency)
            tracing::debug!(
                manifest_hash = %hash,
                "Manifest not found in metadata database (already deleted or never existed)"
            );
            return Ok(());
        };

        // Delete manifest file from object store BEFORE committing transaction
        // If this fails, transaction will roll back and manifest remains in DB
        self.store
            .delete(path)
            .await
            .map_err(DeleteManifestError::ObjectStoreError)?;

        tracing::debug!(
            manifest_hash = %hash,
            "Manifest deleted from object store"
        );

        // Commit transaction - releases locks
        // If this fails, object store file is gone but DB still has manifest
        // Retry is safe: DB delete will find nothing (idempotent), object store delete tolerates NotFound
        tx.commit()
            .await
            .map_err(DeleteManifestError::TransactionCommit)?;

        tracing::debug!(manifest_hash = %hash, "Manifest deletion completed successfully");
        Ok(())
    }

    /// List all datasets that use a specific manifest
    ///
    /// Returns all dataset tags (namespace, name, version) that reference the given
    /// manifest hash. This is useful for discovering which datasets and versions
    /// are using a particular manifest.
    ///
    /// System-managed tags ("latest" and "dev") are excluded from the results.
    ///
    /// Returns `None` if the manifest doesn't exist.
    /// Returns `Some(vec![])` (empty vector) if the manifest exists but no datasets use it.
    pub async fn list_manifest_linked_datasets(
        &self,
        manifest_hash: &Hash,
    ) -> Result<Option<Vec<DatasetTag>>, ListDatasetsUsingManifestError> {
        // Check if manifest exists
        let manifest_path = metadata_db::manifests::get_path(&self.metadata_db, manifest_hash)
            .await
            .map_err(ListDatasetsUsingManifestError::MetadataDbQueryPath)?;

        if manifest_path.is_none() {
            return Ok(None);
        }

        // Query all tags using this manifest
        let tags = metadata_db::datasets::list_tags_by_hash(&self.metadata_db, manifest_hash)
            .await
            .map_err(ListDatasetsUsingManifestError::MetadataDbListTags)?
            .into_iter()
            .map(DatasetTag::from)
            .collect();

        Ok(Some(tags))
    }

    /// List all orphaned manifests (manifests with no dataset links)
    ///
    /// Returns manifest hashes for all manifests that exist in storage but are not
    /// linked to any datasets. These manifests can be safely deleted.
    pub async fn list_orphaned_manifests(&self) -> Result<Vec<Hash>, ListOrphanedManifestsError> {
        metadata_db::manifests::list_orphaned(&self.metadata_db)
            .await
            .map(|hashes| hashes.into_iter().map(Into::into).collect())
            .map_err(ListOrphanedManifestsError)
    }

    /// List all registered manifests with metadata
    ///
    /// Returns all manifests in the system with:
    /// - Content-addressable hash
    /// - Number of datasets using the manifest
    ///
    /// Results are ordered by hash.
    pub async fn list_all_manifests(
        &self,
    ) -> Result<Vec<metadata_db::manifests::ManifestSummary>, ListAllManifestsError> {
        metadata_db::manifests::list_all(&self.metadata_db)
            .await
            .map_err(ListAllManifestsError)
    }
}

// Dataset versioning API
impl DatasetsRegistry {
    /// Link an existing manifest to a dataset
    ///
    /// This method assumes the manifest already exists in the system and only performs the linking
    /// operation. It does NOT store or register the manifest - use `register_manifest` first if
    /// you need to store a new manifest.
    ///
    /// ## Operations performed (in transaction)
    /// 1. Link manifest to dataset (idempotent)
    /// 2. Update "dev" tag to point to this manifest (idempotent)
    pub async fn link_manifest(
        &self,
        namespace: &Namespace,
        name: &Name,
        manifest_hash: &Hash,
    ) -> Result<(), LinkManifestError> {
        // Use transaction to ensure both operations succeed atomically
        let mut tx = self
            .metadata_db
            .begin_txn()
            .await
            .map_err(LinkManifestError::TransactionBegin)?;

        // Link manifest to dataset (idempotent)
        // Foreign key constraint will reject if manifest doesn't exist
        if let Err(err) =
            metadata_db::datasets::link_manifest(&mut tx, namespace, name, manifest_hash).await
        {
            return Err(if err.is_foreign_key_violation() {
                LinkManifestError::ManifestNotFound(manifest_hash.clone())
            } else {
                LinkManifestError::LinkManifestToDataset(err)
            });
        }

        // Automatically update dev tag to point to the linked manifest
        metadata_db::datasets::set_dev_tag(&mut tx, namespace, name, manifest_hash)
            .await
            .map_err(LinkManifestError::SetDevTag)?;

        tx.commit()
            .await
            .map_err(LinkManifestError::TransactionCommit)?;

        Ok(())
    }

    /// Check if a manifest is linked to a specific dataset
    ///
    /// Returns true if the manifest hash is currently linked to the given dataset
    /// (namespace/name combination).
    pub async fn is_manifest_linked(
        &self,
        namespace: &Namespace,
        name: &Name,
        manifest_hash: &Hash,
    ) -> Result<bool, IsManifestLinkedError> {
        metadata_db::datasets::is_manifest_linked(&self.metadata_db, namespace, name, manifest_hash)
            .await
            .map_err(IsManifestLinkedError)
    }

    /// Set a semantic version tag for a dataset manifest
    ///
    /// Creates or updates version tag, then automatically updates "latest" tag if this version is higher.
    /// Uses transaction with `SELECT FOR UPDATE` on "latest" row to prevent concurrent tag updates
    /// from causing stale writes. Idempotent.
    pub async fn set_dataset_version_tag(
        &self,
        namespace: &Namespace,
        name: &Name,
        version: &Version,
        manifest_hash: &Hash,
    ) -> Result<(), SetVersionTagError> {
        // Use transaction to ensure atomic version tag + latest tag update
        let mut tx = self
            .metadata_db
            .begin_txn()
            .await
            .map_err(SetVersionTagError::MetadataDb)?;

        metadata_db::datasets::register_version_tag(
            &mut tx,
            namespace,
            name,
            version,
            manifest_hash,
        )
        .await
        .map_err(SetVersionTagError::MetadataDb)?;

        // Lock the "latest" row to prevent concurrent modifications (SELECT FOR UPDATE)
        let current_latest =
            metadata_db::datasets::get_latest_tag_and_lock(&mut tx, namespace, name)
                .await
                .map_err(SetVersionTagError::UpdateLatestTag)?
                .map(|tag| -> Version { tag.version.into() });

        // Lock held until commit, so current_latest cannot change during this transaction.
        // Update "latest" tag only if new version is higher (or no latest exists).
        if let Some(ref current_latest) = current_latest
            && version <= current_latest
        {
            // Version is not higher than current latest, no need to update latest tag
            tx.commit()
                .await
                .map_err(SetVersionTagError::TransactionCommit)?;
            return Ok(());
        }

        metadata_db::datasets::set_latest_tag(&mut tx, namespace, name, manifest_hash)
            .await
            .map_err(SetVersionTagError::UpdateLatestTag)?;

        // Commit transaction - all operations succeed or all are rolled back
        tx.commit()
            .await
            .map_err(SetVersionTagError::TransactionCommit)?;

        Ok(())
    }

    /// Resolves the "latest" tag to its manifest hash
    ///
    /// Returns the manifest hash that the "latest" tag currently points to, or None if no
    /// "latest" tag exists for this dataset.
    pub async fn resolve_latest_version_hash(
        &self,
        namespace: &Namespace,
        name: &Name,
    ) -> Result<Option<Hash>, ResolveRevisionError> {
        let hash = metadata_db::datasets::get_latest_tag_hash(&self.metadata_db, namespace, name)
            .await
            .map_err(ResolveRevisionError)?
            .map(Into::into);
        Ok(hash)
    }

    /// Resolves the "dev" tag to its manifest hash
    ///
    /// Returns the manifest hash that the "dev" tag currently points to, or None if no
    /// "dev" tag exists for this dataset.
    pub async fn resolve_dev_version_hash(
        &self,
        namespace: &Namespace,
        name: &Name,
    ) -> Result<Option<Hash>, ResolveRevisionError> {
        let hash = metadata_db::datasets::get_dev_tag_hash(&self.metadata_db, namespace, name)
            .await
            .map_err(ResolveRevisionError)?
            .map(Into::into);
        Ok(hash)
    }

    /// Resolves a semantic version tag to its manifest hash
    ///
    /// Returns the manifest hash that the specified version tag points to, or None if the
    /// version tag doesn't exist for this dataset.
    pub async fn resolve_version_hash(
        &self,
        namespace: &Namespace,
        name: &Name,
        version: &Version,
    ) -> Result<Option<Hash>, ResolveRevisionError> {
        let hash = metadata_db::datasets::get_version_tag_hash(
            &self.metadata_db,
            namespace,
            name,
            version,
        )
        .await
        .map_err(ResolveRevisionError)?
        .map(Into::into);
        Ok(hash)
    }

    /// Resolves a reference to a hash reference by resolving its revision to a manifest hash.
    ///
    /// This is the primary resolution method used to convert revision references
    /// (version tags, hashes, or special tags) into concrete hash references.
    ///
    /// For more specific use cases, consider using the dedicated methods:
    /// - `resolve_latest_version_hash` for the "latest" tag
    /// - `resolve_dev_version_hash` for the "dev" tag
    /// - `resolve_version_hash` for semantic versions
    ///
    /// Returns `None` if the dataset or revision cannot be found.
    pub async fn resolve_revision(
        &self,
        reference: impl AsRef<Reference>,
    ) -> Result<Option<HashReference>, ResolveRevisionError> {
        let reference = reference.as_ref();

        let Some(hash) = (match reference.revision() {
            Revision::Latest => {
                self.resolve_latest_version_hash(reference.namespace(), reference.name())
                    .await?
            }
            Revision::Dev => {
                self.resolve_dev_version_hash(reference.namespace(), reference.name())
                    .await?
            }
            Revision::Version(version) => {
                self.resolve_version_hash(reference.namespace(), reference.name(), version)
                    .await?
            }
            Revision::Hash(hash) => {
                // Hash is already concrete, just verify it exists in manifest storage
                let path = metadata_db::manifests::get_path(&self.metadata_db, hash)
                    .await
                    .map_err(ResolveRevisionError)?;
                path.map(|_| hash.clone())
            }
        }) else {
            return Ok(None);
        };

        Ok(Some(HashReference::new(
            reference.namespace().clone(),
            reference.name().clone(),
            hash,
        )))
    }

    /// List all version tags for a dataset
    ///
    /// Returns all semantic version tags for the dataset with their metadata,
    /// sorted in descending order (newest version first).
    ///
    /// Returns an empty list if the dataset has no version tags.
    pub async fn list_dataset_version_tags(
        &self,
        namespace: &Namespace,
        name: &Name,
    ) -> Result<Vec<metadata_db::DatasetTag>, ListVersionTagsError> {
        metadata_db::datasets::list_version_tags(&self.metadata_db, namespace, name)
            .await
            .map_err(ListVersionTagsError)
    }

    /// List all datasets across all namespaces
    ///
    /// Returns all dataset tags from the metadata database.
    /// Each tag represents a dataset-version combination.
    ///
    /// Returns an empty list if no datasets are registered.
    pub async fn list_all_datasets(
        &self,
    ) -> Result<Vec<metadata_db::DatasetTag>, ListAllDatasetsError> {
        metadata_db::datasets::list_all(&self.metadata_db)
            .await
            .map_err(ListAllDatasetsError)
    }

    /// Unlink all manifests from a dataset
    ///
    /// Removes all dataset-manifest associations for the dataset and returns the set of
    /// unlinked manifest hashes. This will also cascade delete all version tags due to
    /// foreign key constraints.
    ///
    /// The caller is responsible for checking if any of the returned manifests became
    /// orphaned (no remaining references) and deleting them if needed.
    ///
    /// This operation is idempotent - returns an empty set if dataset doesn't exist.
    pub async fn unlink_dataset_manifests(
        &self,
        namespace: &Namespace,
        name: &Name,
    ) -> Result<BTreeSet<Hash>, UnlinkDatasetManifestsError> {
        // Delete all dataset_manifests links
        // This will cascade delete all tags due to foreign key constraint
        let unlinked_hashes =
            metadata_db::datasets::unlink_manifests(&self.metadata_db, namespace, name)
                .await
                .map_err(UnlinkDatasetManifestsError)?
                .into_iter()
                .map(Into::into)
                .collect::<BTreeSet<Hash>>();

        tracing::debug!(
            namespace=%namespace,
            name=%name,
            unlinked_count=%unlinked_hashes.len(),
            "Unlinked dataset manifests"
        );

        Ok(unlinked_hashes)
    }

    /// Delete a version tag for a dataset
    ///
    /// Removes the specified semantic version tag from the dataset.
    /// This operation is idempotent - returns `Ok(())` if the version doesn't exist.
    ///
    /// Note: This only deletes the version tag, not the manifest itself.
    /// Manifests are content-addressable and may be referenced by other versions.
    pub async fn delete_dataset_version_tag(
        &self,
        namespace: &Namespace,
        name: &Name,
        version: &Version,
    ) -> Result<(), DeleteVersionTagError> {
        metadata_db::datasets::delete_version_tag(&self.metadata_db, namespace, name, version)
            .await
            .map_err(DeleteVersionTagError)
    }
}

/// Dataset tag information
///
/// Represents a dataset version tag that points to a specific manifest.
/// This is the dataset-store's public representation of dataset tags,
/// mapped from the internal metadata-db representation.
#[derive(Debug, Clone)]
pub struct DatasetTag {
    /// Dataset namespace identifier
    pub namespace: Namespace,
    /// Dataset name
    pub name: Name,
    /// Version tag
    pub version: Version,
    /// Manifest hash this tag references
    pub hash: Hash,
}

impl From<metadata_db::DatasetTag> for DatasetTag {
    fn from(tag: metadata_db::DatasetTag) -> Self {
        Self {
            namespace: tag.namespace.into(),
            name: tag.name.into(),
            version: tag.version.into(),
            hash: tag.hash.into(),
        }
    }
}
