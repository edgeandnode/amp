use std::{ops::RangeInclusive, sync::Arc};

use amp_data_store::{
    DataStore, PhyTableRevision,
    physical_table::{PhyTablePath, PhyTableRevisionPath},
};
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{Session, memory::DataSourceExec},
    common::{DFSchema, project_schema, stats::Precision},
    datasource::{
        TableProvider, TableType, create_ordering,
        listing::{ListingTableUrl, PartitionedFile},
        object_store::ObjectStoreUrl as DataFusionObjectStoreUrl,
        physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource},
    },
    error::Result as DataFusionResult,
    logical_expr::{ScalarUDF, SortExpr, col, utils::conjunction},
    physical_expr::LexOrdering,
    physical_plan::{ExecutionPlan, PhysicalExpr, empty::EmptyExec},
    prelude::Expr,
};
use datafusion_datasource::compute_all_files_statistics;
use datasets_common::{dataset::Table, hash_reference::HashReference, table_name::TableName};
use futures::{Stream, StreamExt as _, TryStreamExt as _, stream};
use metadata_db::LocationId;
use object_store::ObjectStore;
use url::Url;

use crate::{
    BlockNum, BoxError, LogicalCatalog,
    metadata::{
        FileMetadata,
        parquet::ParquetMeta,
        segments::{BlockRange, Chain, Segment, canonical_chain, missing_ranges},
    },
    sql::TableReference,
};

pub mod for_dump;
pub mod for_query;
pub mod reader;

#[derive(Debug, Clone)]
pub struct Catalog {
    tables: Vec<Arc<PhysicalTable>>,
    logical: LogicalCatalog,
}

impl Catalog {
    pub fn empty() -> Self {
        Catalog {
            tables: vec![],
            logical: LogicalCatalog::empty(),
        }
    }

    pub fn new(tables: Vec<Arc<PhysicalTable>>, logical: LogicalCatalog) -> Self {
        Catalog { tables, logical }
    }

    pub fn tables(&self) -> &[Arc<PhysicalTable>] {
        &self.tables
    }

    pub fn udfs(&self) -> &[ScalarUDF] {
        &self.logical.udfs
    }

    pub fn logical(&self) -> &LogicalCatalog {
        &self.logical
    }

    /// Returns the earliest block number across all tables in this catalog.
    pub async fn earliest_block(&self) -> Result<Option<BlockNum>, EarliestBlockError> {
        let mut earliest = None;
        for table in &self.tables {
            // Create a snapshot to get synced range
            let snapshot = table
                .snapshot(false, table.store.clone())
                .await
                .map_err(EarliestBlockError)?;
            let synced_range = snapshot.synced_range();
            match (earliest, &synced_range) {
                (None, Some(range)) => earliest = Some(range.start()),
                _ => earliest = earliest.min(synced_range.map(|range| range.start())),
            }
        }
        Ok(earliest)
    }
}

/// Errors that can occur when computing the earliest block across tables.
#[derive(Debug, thiserror::Error)]
#[error("failed to compute earliest block")]
pub struct EarliestBlockError(#[source] pub SnapshotError);

#[derive(Debug, Clone)]
pub struct CatalogSnapshot {
    table_snapshots: Vec<Arc<TableSnapshot>>,
    logical: LogicalCatalog,
}

impl CatalogSnapshot {
    pub async fn from_catalog(
        catalog: Catalog,
        ignore_canonical_segments: bool,
        store: DataStore,
    ) -> Result<Self, FromCatalogError> {
        let mut table_snapshots = Vec::new();
        for physical_table in &catalog.tables {
            let snapshot = physical_table
                .snapshot(ignore_canonical_segments, store.clone())
                .await
                .map_err(FromCatalogError)?;
            table_snapshots.push(Arc::new(snapshot));
        }

        Ok(CatalogSnapshot {
            table_snapshots,
            logical: catalog.logical,
        })
    }

    pub fn table_snapshots(&self) -> &[Arc<TableSnapshot>] {
        &self.table_snapshots
    }

    /// Access physical tables through the snapshots
    pub fn physical_tables(&self) -> impl Iterator<Item = &Arc<PhysicalTable>> {
        self.table_snapshots.iter().map(|s| s.physical_table())
    }

    pub fn udfs(&self) -> &[ScalarUDF] {
        &self.logical.udfs
    }

    pub fn logical(&self) -> &LogicalCatalog {
        &self.logical
    }
}

/// Errors that can occur when creating a catalog snapshot from a catalog.
#[derive(Debug, thiserror::Error)]
#[error("failed to create catalog snapshot")]
pub struct FromCatalogError(#[source] pub SnapshotError);

#[derive(Debug, Clone)]
pub struct PhysicalTable {
    /// Core storage information from data-store
    revision: PhyTableRevision,

    /// Dataset reference (namespace, name, hash).
    dataset_reference: HashReference,

    /// Dataset start block.
    dataset_start_block: Option<BlockNum>,

    /// The dataset reference portion of SQL table references.
    ///
    /// SQL table references have the format `<dataset_ref>.<table>` (e.g., `anvil_rpc.blocks`).
    /// This field stores the string form of the `<dataset_ref>` portion - the schema under
    /// which this table is registered in the catalog and referenced in SQL queries.
    ///
    /// For global namespace datasets (`_`), this can be just the dataset name (e.g., `eth_rpc`)
    /// or include the hash (e.g., `eth_rpc@abc123`), depending on how the PhysicalTable was constructed.
    /// For non-global namespaces, this is `namespace/name` (e.g., `freecandylabs/function_in_table`)
    /// or `namespace/name@hash`.
    sql_table_ref_schema: String,

    /// Table name.
    table_name: TableName,

    /// Network identifier.
    network: String,

    /// Data store for accessing metadata database and object storage.
    store: DataStore,

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
        sql_table_ref_schema: String,
    ) -> Self {
        Self {
            revision,
            dataset_reference,
            dataset_start_block,
            sql_table_ref_schema,
            table_name: table.name().clone(),
            network: table.network().to_string(),
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

    pub fn dataset_start_block(&self) -> Option<BlockNum> {
        self.dataset_start_block
    }

    pub fn table_name(&self) -> &TableName {
        &self.table_name
    }

    pub fn network(&self) -> &str {
        &self.network
    }

    pub fn url(&self) -> &Url {
        self.revision.url.inner()
    }

    pub fn path(&self) -> &PhyTableRevisionPath {
        &self.revision.path
    }

    /// Returns the dataset reference portion for SQL table references.
    ///
    /// SQL table references have the format `<dataset_ref>.<table>` (e.g., `anvil_rpc.blocks`).
    /// This returns the string form of the `<dataset_ref>` portion - the schema under which
    /// this table should be registered in the catalog and referenced in SQL queries.
    ///
    /// The format depends on how the PhysicalTable was constructed:
    /// - For global namespace datasets (`_`), this can be just the dataset name (e.g., `eth_rpc`)
    ///   or include the hash (e.g., `eth_rpc@abc123`).
    /// - For non-global namespaces, this is `namespace/name` (e.g., `freecandylabs/function_in_table`)
    ///   or `namespace/name@hash`.
    pub fn sql_table_ref_schema(&self) -> &str {
        &self.sql_table_ref_schema
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

    /// Qualified table reference in the format `dataset_name.table_name`.
    pub fn table_ref(&self) -> TableReference {
        TableReference::partial(
            self.sql_table_ref_schema().to_string(),
            self.table_name.clone(),
        )
    }

    pub fn table(&self) -> &Table {
        &self.table
    }

    /// Returns a compact table reference with shortened hash.
    ///
    /// Format: `namespace/name@shortHash.table_name`
    /// Uses `HashReference::short_display()` which shows first 7 characters of the hash.
    pub fn table_ref_compact(&self) -> String {
        format!(
            "{}.{}",
            self.dataset_reference.short_display(),
            self.table_name
        )
    }

    fn order_exprs(&self) -> Vec<Vec<SortExpr>> {
        let sorted_by = self.table.sorted_by();
        self.schema()
            .fields()
            .iter()
            .filter_map(move |field| {
                if sorted_by.contains(field.name()) {
                    Some(vec![SortExpr::new(col(field.name()), true, false)])
                } else {
                    None
                }
            })
            .collect()
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

    pub async fn missing_ranges(
        &self,
        desired: RangeInclusive<BlockNum>,
    ) -> Result<Vec<RangeInclusive<BlockNum>>, MissingRangesError> {
        let segments = self.segments().await.map_err(MissingRangesError)?;
        Ok(missing_ranges(segments, desired))
    }

    pub async fn canonical_chain(&self) -> Result<Option<Chain>, CanonicalChainError> {
        let segments = self.segments().await.map_err(CanonicalChainError)?;
        let canonical = canonical_chain(segments);
        if let Some(start_block) = self.dataset_start_block
            && let Some(canonical) = &canonical
            && canonical.start() > start_block
        {
            return Ok(None);
        }
        Ok(canonical)
    }

    async fn segments(&self) -> Result<Vec<Segment>, GetSegmentsError> {
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
                    file_id,
                    file_name,
                    object_meta,
                    parquet_meta: ParquetMeta { mut ranges, .. },
                    ..
                } = file;

                if ranges.len() != 1 {
                    return Err(GetSegmentsError::UnexpectedRangeCount {
                        file_name: file_name.to_string(),
                        count: ranges.len(),
                    });
                }

                Ok(Segment {
                    id: file_id,
                    range: ranges.remove(0),
                    object: object_meta,
                })
            })
            .try_collect()
            .await
    }

    /// A snapshot binds this physical table to the currently canonical chain.
    ///
    /// `ignore_canonical_segments` will instead bind to all segments physically present. This should
    /// be used carefully as it may include duplicated or forked data.
    pub async fn snapshot(
        &self,
        ignore_canonical_segments: bool,
        store: DataStore,
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

        // Create a reader factory with the cached store
        let reader_factory_with_cache = reader::AmpReaderFactory {
            location_id: self.revision.location_id,
            store,
            schema: self.schema(),
        };

        Ok(TableSnapshot {
            physical_table: Arc::new(self.clone()),
            reader_factory: Arc::new(reader_factory_with_cache),
            canonical_segments,
        })
    }
}

// helper methods for implementing `TableProvider` trait
impl PhysicalTable {
    fn object_store_url(&self) -> DataFusionResult<DataFusionObjectStoreUrl> {
        Ok(ListingTableUrl::try_new(self.revision.url.inner().clone(), None)?.object_store())
    }

    fn output_ordering(&self) -> DataFusionResult<Vec<LexOrdering>> {
        let schema = self.schema();
        let sort_order = self.order_exprs();
        create_ordering(&schema, &sort_order)
    }

    /// See: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/advanced_parquet_index.rs
    fn filters_to_predicate(
        &self,
        state: &dyn Session,
        filters: &[Expr],
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        let df_schema = DFSchema::try_from(self.schema())?;
        let predicate = conjunction(filters.to_vec());
        let predicate = predicate
            .map(|predicate| state.create_physical_expr(predicate, &df_schema))
            .transpose()?
            .unwrap_or_else(|| datafusion::physical_expr::expressions::lit(true));

        Ok(predicate)
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

    /// File has unexpected number of block ranges
    #[error("expected exactly 1 range for file '{file_name}', found {count}")]
    UnexpectedRangeCount { file_name: String, count: usize },
}

/// Errors that can occur when computing missing ranges for a physical table.
#[derive(Debug, thiserror::Error)]
#[error("failed to compute missing ranges")]
pub struct MissingRangesError(#[source] pub GetSegmentsError);

/// Errors that can occur when computing the canonical chain for a physical table.
#[derive(Debug, thiserror::Error)]
#[error("failed to compute canonical chain")]
pub struct CanonicalChainError(#[source] pub GetSegmentsError);

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

#[derive(Debug, Clone)]
pub struct TableSnapshot {
    physical_table: Arc<PhysicalTable>,
    reader_factory: Arc<reader::AmpReaderFactory>,
    canonical_segments: Vec<Segment>,
}

impl TableSnapshot {
    pub fn physical_table(&self) -> &Arc<PhysicalTable> {
        &self.physical_table
    }

    /// Return the block range to use for query execution over this table. None is returned if no
    /// block range has been synced.
    pub fn synced_range(&self) -> Option<BlockRange> {
        let segments = &self.canonical_segments;
        let start = segments.iter().min_by_key(|s| s.range.start())?;
        let end = segments.iter().max_by_key(|s| s.range.end())?;
        Some(BlockRange {
            network: start.range.network.clone(),
            numbers: start.range.start()..=end.range.end(),
            hash: end.range.hash,
            prev_hash: start.range.prev_hash,
        })
    }

    pub fn canonical_segments(&self) -> &[Segment] {
        &self.canonical_segments
    }

    pub fn reader_factory(&self) -> &Arc<reader::AmpReaderFactory> {
        &self.reader_factory
    }

    // Convert a Segment to a PartitionedFile and crucially associates statistics to the file
    async fn segment_to_partitioned_file(
        &self,
        segment: &Segment,
    ) -> Result<PartitionedFile, BoxError> {
        let metadata = self.reader_factory.get_cached_metadata(segment.id).await?;
        let file = PartitionedFile::from(segment.object.clone())
            .with_extensions(Arc::new(segment.id))
            .with_statistics(metadata.statistics);
        Ok(file)
    }
}

#[async_trait::async_trait]
impl TableProvider for TableSnapshot {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.physical_table.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    #[tracing::instrument(skip_all, err, fields(table = %self.physical_table.table_ref(), files = %self.canonical_segments.len()))]
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        tracing::debug!("creating scan execution plan");

        if self.synced_range().is_none() {
            // This is necessary to work around empty tables tripping the DF sanity checker
            tracing::debug!("table has no synced data, returning empty execution plan");
            let projected_schema = project_schema(&self.schema(), projection)?;
            return Ok(Arc::new(EmptyExec::new(projected_schema)));
        }

        let target_partitions = state.config_options().execution.target_partitions;
        let table_schema = self.physical_table.schema();
        let (file_groups, statistics) = {
            let file_count = self.canonical_segments.len();
            let file_stream = stream::iter(self.canonical_segments.iter())
                .then(|s| self.segment_to_partitioned_file(s));
            let partitioned = round_robin(file_stream, file_count, target_partitions).await?;
            compute_all_files_statistics(partitioned, table_schema.clone(), true, false)?
        };
        if statistics.num_rows == Precision::Absent {
            // This log likely signifies a bug in our statistics fetching.
            tracing::warn!("Table has no row count statistics. Queries may be inefficient.");
        }

        let output_ordering = self.physical_table.output_ordering()?;
        let object_store_url = self.physical_table.object_store_url()?;
        let predicate = self.physical_table.filters_to_predicate(state, filters)?;

        let parquet_file_reader_factory = Arc::clone(&self.reader_factory);
        let table_parquet_options = state.table_options().parquet.clone();
        let file_source = ParquetSource::new(table_parquet_options)
            .with_parquet_file_reader_factory(parquet_file_reader_factory)
            .with_predicate(predicate)
            .into();

        let data_source = Arc::new(
            FileScanConfigBuilder::new(object_store_url, table_schema, file_source)
                .with_file_groups(file_groups)
                .with_limit(limit)
                .with_output_ordering(output_ordering)
                .with_projection(projection.cloned())
                .with_statistics(statistics)
                .build(),
        );

        Ok(Arc::new(DataSourceExec::new(data_source)))
    }
}

/// Lists all table revision paths found in object storage.
///
/// The `store` parameter must be a prefixed object store pointing to the data directory root,
/// as this function uses relative paths like `dataset/table/revision_id`.
///
/// This function performs a `list_with_delimiter` query on the provided table path,
/// iterates through the returned subdirectories, and parses each as a `PhyTableRevisionPath`.
/// All valid revision paths are collected and returned in no particular order.
/// If no revisions exist, an empty vector is returned.
pub async fn list_table_revisions<S>(
    store: &S,
    path: &PhyTablePath,
) -> Result<Vec<PhyTableRevisionPath>, ListRevisionsError>
where
    S: ObjectStore + ?Sized,
{
    let list_result = store
        .list_with_delimiter(Some(path))
        .await
        .map_err(ListRevisionsError)?;

    let revisions = list_result
        .common_prefixes
        .into_iter()
        .filter_map(|path| path.as_ref().parse::<PhyTableRevisionPath>().ok())
        .collect();

    Ok(revisions)
}

/// Error when listing revisions from object store
///
/// This error type is used by `list_table_revisions()`.
#[derive(Debug, thiserror::Error)]
#[error("Failed to list revisions from object store")]
pub struct ListRevisionsError(#[source] pub object_store::Error);

/// Finds the latest table revision by lexicographic comparison of revision IDs.
///
/// The `store` parameter must be a prefixed object store pointing to the data directory root,
/// as this function uses relative paths like `dataset/table/revision_id`.
///
/// This function performs a `list_with_delimiter` query on the provided table path,
/// iterates through the returned subdirectories, parses each as a `PhyTableRevisionPath`,
/// and finds the maximum by lexicographic comparison.
///
/// Lexicographic comparison works correctly for UUIDv7 revisions since they are
/// time-ordered by design. The latest UUID will sort last lexicographically.
async fn round_robin(
    files: impl Stream<Item = Result<PartitionedFile, BoxError>> + Send,
    files_len: usize,
    target_partitions: usize,
) -> Result<Vec<FileGroup>, BoxError> {
    let size = files_len.min(target_partitions);
    let mut groups = vec![FileGroup::default(); size];
    let mut stream = files.enumerate().boxed();
    while let Some((idx, file)) = stream.next().await {
        groups[idx % size].push(file?);
    }
    Ok(groups)
}
