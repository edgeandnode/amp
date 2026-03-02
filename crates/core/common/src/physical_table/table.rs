use std::{ops::RangeInclusive, sync::Arc};

use amp_data_store::{DataStore, PhyTableRevision, physical_table::PhyTableRevisionPath};
use datafusion::arrow::datatypes::SchemaRef;
use datasets_common::{
    dataset::Table, hash_reference::HashReference, network_id::NetworkId, table_name::TableName,
};
use futures::{StreamExt as _, TryStreamExt as _};
use metadata_db::physical_table_revision::LocationId;
use url::Url;

use crate::{
    BlockNum,
    metadata::parquet::ParquetMeta,
    physical_table::{
        file::FileMetadata,
        segments::{Chain, Segment, canonical_chain, missing_ranges},
        snapshot::TableSnapshot,
    },
};

#[derive(Debug, Clone)]
pub struct PhysicalTable {
    /// Core storage information from data-store
    pub(crate) revision: PhyTableRevision,

    /// Dataset reference (namespace, name, hash).
    dataset_reference: HashReference,

    /// Dataset start block.
    dataset_start_block: Option<BlockNum>,

    /// Table name.
    table_name: TableName,

    /// Network identifier.
    network: NetworkId,

    /// Data store for accessing metadata database and object storage.
    pub(crate) store: DataStore,

    /// Table definition (schema, network, sorted_by).
    table: Table,
}

// Methods for creating and managing PhysicalTable instances
impl PhysicalTable {
    /// Constructs a [`PhysicalTable`] from a table revision ([`PhyTableRevision`]).
    ///
    /// This is the primary constructor for creating a PhysicalTable from the core
    /// storage information along with domain-specific metadata.
    pub fn from_revision(
        store: DataStore,
        dataset_reference: HashReference,
        dataset_start_block: Option<BlockNum>,
        table: Table,
        revision: PhyTableRevision,
    ) -> Self {
        Self {
            revision,
            dataset_reference,
            dataset_start_block,
            table_name: table.name().clone(),
            network: table.network().clone(),
            store,
            table,
        }
    }
}

// Methods for accessing properties of PhysicalTable
impl PhysicalTable {
    pub fn dataset_reference(&self) -> &HashReference {
        &self.dataset_reference
    }

    pub fn table_name(&self) -> &TableName {
        &self.table_name
    }

    pub fn network(&self) -> &NetworkId {
        &self.network
    }

    pub fn url(&self) -> &Url {
        self.revision.url.inner()
    }

    pub fn path(&self) -> &PhyTableRevisionPath {
        &self.revision.path
    }

    pub fn schema(&self) -> SchemaRef {
        self.table.schema().clone()
    }

    pub fn location_id(&self) -> LocationId {
        self.revision.location_id
    }

    pub fn revision(&self) -> &PhyTableRevision {
        &self.revision
    }

    pub fn table(&self) -> &Table {
        &self.table
    }

    /// Returns a compact table reference with shortened hash.
    ///
    /// Format: `namespace/name@shortHash.table_name`
    /// Uses alternate display format `{:#}` which shows first 7 characters of the hash.
    pub fn table_ref_compact(&self) -> String {
        format!("{:#}.{}", self.dataset_reference, self.table_name)
    }

    pub async fn files(&self) -> Result<Vec<FileMetadata>, GetFilesError> {
        self.store
            .stream_revision_file_metadata(&self.revision)
            .map(|result| {
                let file_meta = result.map_err(GetFilesError::StreamMetadata)?;
                file_meta.try_into().map_err(GetFilesError::ParseMetadata)
            })
            .try_collect()
            .await
    }

    /// Compute missing block ranges from the canonical chain.
    pub async fn missing_ranges(
        &self,
        desired: RangeInclusive<BlockNum>,
    ) -> Result<Vec<RangeInclusive<BlockNum>>, MissingRangesError> {
        let segments = self
            .segments()
            .await
            .map_err(MissingRangesError::GetSegments)?;

        // The blocking computation is offloaded to a dedicated thread pool via `spawn_blocking`
        // to prevent blocking the async runtime. The `missing_ranges` function performs
        // CPU-intensive operations (sorting, chain building) that can take milliseconds.
        let ranges = tokio::task::spawn_blocking(move || missing_ranges(segments, desired))
            .await
            .map_err(MissingRangesError::TaskPanicked)?;

        Ok(ranges)
    }

    /// Compute the canonical chain of segments.
    pub async fn canonical_chain(&self) -> Result<Option<Chain>, CanonicalChainError> {
        let segments = self
            .segments()
            .await
            .map_err(CanonicalChainError::GetSegments)?;

        // The blocking computation is offloaded to a dedicated thread pool via `spawn_blocking`
        // to prevent blocking the async runtime. The `canonical_chain` function performs
        // CPU-intensive operations (sorting, chain building) that can take milliseconds.
        let canonical = tokio::task::spawn_blocking(move || canonical_chain(segments))
            .await
            .map_err(CanonicalChainError::TaskPanicked)?;

        if let Some(start_block) = self.dataset_start_block
            && let Some(canonical) = &canonical
        {
            let first_ranges = canonical.first_ranges();
            if first_ranges.len() > 1 {
                return Err(CanonicalChainError::MultiNetworkWithStartBlock);
            }
            if !first_ranges.is_empty() && first_ranges[0].start() > start_block {
                return Ok(None);
            }
        }
        Ok(canonical)
    }

    pub(crate) async fn segments(&self) -> Result<Vec<Segment>, GetSegmentsError> {
        self.store
            .stream_revision_file_metadata(&self.revision)
            .map(|result| {
                // Handle stream error
                let file_meta = result.map_err(GetSegmentsError::StreamMetadata)?;

                // Parse FileMetadata
                let file: FileMetadata = file_meta
                    .try_into()
                    .map_err(GetSegmentsError::ParseMetadata)?;

                // Convert FileMetadata -> Segment
                let FileMetadata {
                    file_id: id,
                    object_meta: object,
                    parquet_meta: ParquetMeta { ranges, .. },
                    ..
                } = file;

                Ok(Segment::new(id, object, ranges))
            })
            .try_collect()
            .await
    }

    /// A snapshot binds this physical table to the currently canonical chain.
    ///
    /// `ignore_canonical_segments` will instead bind to all segments physically present. This
    /// should be used carefully as it may include duplicated or forked data.
    pub async fn snapshot(
        &self,
        ignore_canonical_segments: bool,
    ) -> Result<TableSnapshot, SnapshotError> {
        let canonical_segments = if ignore_canonical_segments {
            self.segments().await.map_err(SnapshotError::GetSegments)?
        } else {
            self.canonical_chain()
                .await
                .map_err(SnapshotError::CanonicalChain)?
                .into_iter()
                .flatten()
                .collect()
        };

        Ok(TableSnapshot::new(
            Arc::new(self.clone()),
            canonical_segments,
        ))
    }
}

/// Errors that can occur when getting files from a physical table.
#[derive(Debug, thiserror::Error)]
pub enum GetFilesError {
    /// Failed to stream file metadata from data store
    #[error("failed to stream file metadata")]
    StreamMetadata(#[source] amp_data_store::StreamFileMetadataError),

    /// Failed to parse parquet metadata JSON
    #[error("failed to parse parquet metadata")]
    ParseMetadata(#[source] serde_json::Error),
}

/// Errors that can occur when getting segments from a physical table.
#[derive(Debug, thiserror::Error)]
pub enum GetSegmentsError {
    /// Failed to stream file metadata from data store
    #[error("failed to stream file metadata")]
    StreamMetadata(#[source] amp_data_store::StreamFileMetadataError),

    /// Failed to parse parquet metadata JSON
    #[error("failed to parse parquet metadata")]
    ParseMetadata(#[source] serde_json::Error),
}

impl crate::retryable::RetryableErrorExt for GetSegmentsError {
    fn is_retryable(&self) -> bool {
        use amp_data_store::retryable::RetryableErrorExt;
        match self {
            Self::StreamMetadata(err) => err.is_retryable(),
            Self::ParseMetadata(_) => false,
        }
    }
}

/// Errors that can occur when computing missing ranges for a physical table.
#[derive(Debug, thiserror::Error)]
pub enum MissingRangesError {
    /// Failed to get segments
    #[error("failed to get segments")]
    GetSegments(#[source] GetSegmentsError),

    /// The `spawn_blocking` task for `missing_ranges` panicked or was aborted.
    ///
    /// This surfaces as a recoverable error rather than propagating a process-level panic.
    #[error("missing_ranges computation task panicked or was aborted")]
    TaskPanicked(#[source] tokio::task::JoinError),
}

impl crate::retryable::RetryableErrorExt for MissingRangesError {
    fn is_retryable(&self) -> bool {
        match self {
            Self::GetSegments(err) => err.is_retryable(),
            Self::TaskPanicked(_) => false,
        }
    }
}

/// Errors that can occur when computing the canonical chain for a physical table.
#[derive(Debug, thiserror::Error)]
pub enum CanonicalChainError {
    /// Failed to get segments
    #[error("failed to get segments")]
    GetSegments(#[source] GetSegmentsError),

    /// The `spawn_blocking` task for `canonical_chain` panicked or was aborted.
    ///
    /// This surfaces as a recoverable error rather than propagating a process-level panic.
    #[error("canonical_chain computation task panicked or was aborted")]
    TaskPanicked(#[source] tokio::task::JoinError),

    /// Multi-network canonical chain not supported with start block
    #[error("multi-network datasets with start_block are not supported")]
    MultiNetworkWithStartBlock,
}

impl crate::retryable::RetryableErrorExt for CanonicalChainError {
    fn is_retryable(&self) -> bool {
        match self {
            Self::GetSegments(err) => err.is_retryable(),
            Self::TaskPanicked(_) => true,
            Self::MultiNetworkWithStartBlock => false,
        }
    }
}

/// Errors that can occur when creating a table snapshot.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    /// Failed to get segments
    #[error("failed to get segments")]
    GetSegments(#[source] GetSegmentsError),

    /// Failed to compute canonical chain
    #[error("failed to compute canonical chain")]
    CanonicalChain(#[source] CanonicalChainError),
}

impl crate::retryable::RetryableErrorExt for SnapshotError {
    fn is_retryable(&self) -> bool {
        match self {
            Self::GetSegments(err) => err.is_retryable(),
            Self::CanonicalChain(err) => err.is_retryable(),
        }
    }
}
