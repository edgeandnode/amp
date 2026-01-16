use std::{ops::RangeInclusive, sync::Arc};

use amp_data_store::{
    DataStore, PhysicalTableActiveRevision,
    physical_table::{PhyTablePath, PhyTableRevisionPath, PhyTableUrl},
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
use datasets_common::{hash_reference::HashReference, table_name::TableName};
use futures::{Stream, StreamExt, TryStreamExt, stream};
use metadata_db::LocationId;
use object_store::{ObjectMeta, ObjectStore, path::Path};
use url::Url;
use uuid::Uuid;

use crate::{
    BlockNum, BoxError, LogicalCatalog,
    catalog::logical::Table,
    metadata::{
        FileMetadata, amp_metadata_from_parquet_file,
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
    pub async fn earliest_block(&self) -> Result<Option<BlockNum>, BoxError> {
        let mut earliest = None;
        for table in &self.tables {
            // Create a snapshot to get synced range
            let snapshot = table.snapshot(false, table.store.clone()).await?;
            let synced_range = snapshot.synced_range();
            match (earliest, &synced_range) {
                (None, Some(range)) => earliest = Some(range.start()),
                _ => earliest = earliest.min(synced_range.map(|range| range.start())),
            }
        }
        Ok(earliest)
    }
}

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
    ) -> Result<Self, BoxError> {
        let mut table_snapshots = Vec::new();
        for physical_table in &catalog.tables {
            let snapshot = physical_table
                .snapshot(ignore_canonical_segments, store.clone())
                .await?;
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

/// Registers a new physical table revision with a unique UUIDv7-based location path.
///
/// Establishes a new storage location (revision) for a table at `<dataset_name>/<table>/<UUIDv7>/`
/// and registers it in the metadata database. Each revision is uniquely identified by a time-ordered
/// UUIDv7, allowing multiple revisions of the same table to coexist.
///
/// # Idempotency
///
/// If a location with the same URL already exists, the function returns the existing location ID
/// without creating a duplicate.
///
/// # Active Status
///
/// The new revision is atomically marked as active while deactivating all other revisions for this
/// table. Only active revisions are used for query execution.
#[tracing::instrument(skip_all, fields(dataset = %dataset_ref, table = %table.name()), err)]
pub async fn register_new_table_revision(
    store: DataStore,
    dataset_ref: HashReference,
    dataset_start_block: Option<BlockNum>,
    table: Table,
    sql_table_ref_schema: String,
) -> Result<PhysicalTable, RegisterNewTableRevisionError> {
    let revision_id = Uuid::now_v7();
    let path = PhyTableRevisionPath::new(dataset_ref.name(), table.name(), revision_id);
    let url = PhyTableUrl::new(store.url(), &path);

    let location_id = store
        .register_table_revision(&dataset_ref, table.name(), &path)
        .await
        .map_err(RegisterNewTableRevisionError)?;

    tracing::info!("Registered new revision at: {}", url);

    Ok(PhysicalTable {
        location_id,
        dataset_reference: dataset_ref,
        dataset_start_block,
        sql_table_ref_schema,
        table_name: table.name().clone(),
        network: table.network().to_string(),
        path,
        url,
        store,
        table,
    })
}

/// Failed to register and activate a new physical table revision
///
/// This error occurs when the Store fails to atomically register and activate
/// a new table revision in the metadata database.
///
/// This error type is used by `register_new_table_revision()`.
#[derive(Debug, thiserror::Error)]
#[error("Failed to register and activate new table revision")]
pub struct RegisterNewTableRevisionError(#[source] pub amp_data_store::RegisterTableRevisionError);

/// Restores the most recent physical table revision from object storage.
///
/// Discovers and restores the latest UUIDv7-based revision for a table by scanning the object
/// store at `<dataset_name>/<table>/` and selecting the revision with the highest UUID (most
/// recent timestamp). All Parquet files within the revision are registered in the metadata
/// database, enabling immediate query execution. Returns `None` if no revisions exist in object
/// storage for this table, indicating the table has never been materialized.
///
/// The function performs the following steps:
/// 1. Constructs the table prefix path and lists all revisions in object storage
/// 2. Selects the latest revision based on UUIDv7 ordering (returns `None` if no revisions found)
/// 3. Registers the revision location in the metadata database within a transaction
/// 4. Atomically marks the restored revision as active and all others as inactive
/// 5. Lists all Parquet files in the restored revision directory
/// 6. Reads metadata from all files concurrently (up to 16 at a time)
/// 7. Registers each file in the metadata database with its object metadata, Parquet footer,
///    and Amp-specific metadata
/// 8. Returns the fully initialized `PhysicalTable` ready for query execution
///
/// # Discovery Process
///
/// The function lists all subdirectories under the table prefix, parses each as a UUIDv7
/// revision identifier, and selects the latest based on UUID ordering. Since UUIDv7s are
/// time-ordered, the lexicographically highest UUID represents the most recent revision.
///
/// # Idempotency
///
/// If a location with the same URL already exists in the metadata database, the function
/// returns the existing location ID without creating a duplicate.
///
/// # Active Status
///
/// The restored revision is atomically marked as active while deactivating all other revisions
/// for this table. Only active revisions are used for query execution.
///
/// # File Registration
///
/// All Parquet files in the restored revision are read concurrently (up to 16 concurrent fetches)
/// and registered in the metadata database with their object metadata, Parquet footer statistics,
/// and Amp-specific metadata. This enables efficient query planning and execution.
///
/// **Note:** File registration occurs outside the transaction scope. If file registration fails
/// partway through, the revision will be marked as active but with incomplete file metadata.
/// Only the physical table registration (steps 3-4) is transactional.
#[tracing::instrument(skip_all, fields(dataset = %dataset_ref, table = %table.name()), err)]
pub async fn restore_table_latest_revision(
    store: DataStore,
    dataset_ref: &HashReference,
    dataset_start_block: Option<BlockNum>,
    table: &Table,
    sql_table_ref_schema: String,
) -> Result<Option<PhysicalTable>, RestoreLatestTableRevisionError> {
    let table_path = PhyTablePath::new(dataset_ref.name(), table.name());

    tracing::debug!("Restoring latest revision in prefix {}", table_path);

    let Some(path) = store
        .find_latest_table_revision_in_object_store(&table_path)
        .await
        .map_err(RestoreLatestTableRevisionError::FindLatestRevision)?
    else {
        return Ok(None);
    };

    let url = PhyTableUrl::new(store.url(), &path);

    let location_id = store
        .register_table_revision(dataset_ref, table.name(), &path)
        .await
        .map_err(RestoreLatestTableRevisionError::RegisterRevision)?;

    let files = store
        .list_revision_files_in_object_store(&path)
        .await
        .map_err(RestoreLatestTableRevisionError::ListFiles)?;

    // Process files in parallel using buffered stream
    const CONCURRENT_METADATA_FETCHES: usize = 16;

    let object_store = store.clone();
    let mut file_stream = stream::iter(files.into_iter())
        .map(|object_meta| {
            let store = object_store.clone();
            async move {
                let (file_name, amp_meta, footer) =
                    amp_metadata_from_parquet_file(&store, &object_meta)
                        .await
                        .map_err(|e: BoxError| {
                            RestoreLatestTableRevisionError::ReadParquetMetadata(e)
                        })?;

                let parquet_meta_json = serde_json::to_value(amp_meta)
                    .map_err(RestoreLatestTableRevisionError::SerializeMetadata)?;

                let ObjectMeta {
                    size: object_size,
                    e_tag: object_e_tag,
                    version: object_version,
                    ..
                } = object_meta;

                Ok((
                    file_name,
                    object_size,
                    object_e_tag,
                    object_version,
                    parquet_meta_json,
                    footer,
                ))
            }
        })
        .buffered(CONCURRENT_METADATA_FETCHES);

    // Register all files in the metadata database as they complete
    while let Some(result) = file_stream.next().await {
        let (file_name, object_size, object_e_tag, object_version, parquet_meta_json, footer) =
            result?;
        store
            .register_file(
                location_id,
                url.inner(),
                &file_name,
                object_size,
                object_e_tag,
                object_version,
                parquet_meta_json,
                &footer,
            )
            .await
            .map_err(|err| RestoreLatestTableRevisionError::RegisterFile(err.0))?;
    }

    Ok(Some(PhysicalTable {
        location_id,
        dataset_reference: dataset_ref.clone(),
        dataset_start_block,
        sql_table_ref_schema,
        table_name: table.name().clone(),
        network: table.network().to_string(),
        path,
        url,
        store,
        table: table.clone(),
    }))
}

/// Errors that occur when restoring the latest revision of a physical table
///
/// This error type is used by [`restore_table_latest_revision`].
#[derive(Debug, thiserror::Error)]
pub enum RestoreLatestTableRevisionError {
    /// Failed to find latest revision from object store
    ///
    /// This occurs when the object store cannot be queried for existing revisions,
    /// typically due to network issues, permission errors, or storage unavailability.
    #[error("Failed to find latest revision from object store")]
    FindLatestRevision(#[source] amp_data_store::FindLatestTableRevisionInObjectStoreError),

    /// Failed to register revision in metadata database
    ///
    /// This occurs when the metadata database transaction for registering the
    /// physical table and marking it as active fails.
    #[error("Failed to register revision in metadata database")]
    RegisterRevision(#[source] amp_data_store::RegisterTableRevisionError),

    /// Failed to begin transaction for marking table active
    #[error("Failed to begin transaction")]
    TransactionBegin(#[source] metadata_db::Error),
    /// Failed to register physical table in metadata database
    ///
    /// This occurs when the metadata database rejects the registration of the
    /// restored physical table location.
    #[error("Failed to register physical table in metadata database")]
    RegisterPhysicalTable(#[source] metadata_db::Error),

    /// Failed to mark existing physical tables as inactive
    #[error("Failed to mark existing physical tables as inactive")]
    MarkInactive(#[source] metadata_db::Error),

    /// Failed to mark restored physical table as active
    #[error("Failed to mark restored physical table as active")]
    MarkActive(#[source] metadata_db::Error),

    /// Failed to commit transaction after updating table status
    #[error("Failed to commit transaction")]
    TransactionCommit(#[source] metadata_db::Error),

    /// Failed to list files in the restored revision
    #[error("Failed to list files in restored revision")]
    ListFiles(#[source] amp_data_store::ListRevisionFilesInObjectStoreError),

    /// Failed to read Amp metadata from parquet file
    ///
    /// This occurs when the parquet file cannot be read or its metadata cannot be parsed.
    ///
    /// TODO: Replace BoxError with concrete error type when amp_metadata_from_parquet_file
    /// is migrated to use proper error handling patterns.
    #[error("Failed to read Amp metadata from parquet file")]
    ReadParquetMetadata(#[source] BoxError),

    /// Failed to serialize parquet metadata to JSON
    #[error("Failed to serialize parquet metadata to JSON")]
    SerializeMetadata(#[source] serde_json::Error),

    /// Failed to register file in metadata database
    #[error("Failed to register file in metadata database")]
    RegisterFile(#[source] metadata_db::Error),
}

#[derive(Debug, Clone)]
pub struct PhysicalTable {
    /// Location ID in the metadata database.
    location_id: LocationId,

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

    /// Relative path to the table revision in object storage.
    /// Format: `<dataset_name>/<table_name>/<revision_uuid>`
    path: PhyTableRevisionPath,

    /// Full URL to the table directory (for compatibility/display).
    url: PhyTableUrl,

    /// Data store for accessing metadata database and object storage.
    store: DataStore,

    /// Table definition (schema, network, sorted_by).
    table: Table,
}

// Methods for creating and managing PhysicalTable instances
impl PhysicalTable {
    /// Constructs a [`PhysicalTable`] from an active revision retrieved from the metadata database.
    ///
    /// This provides a fast path for creating a PhysicalTable when the active revision
    /// is already known, avoiding the need to query object storage.
    pub fn from_active_revision(
        store: DataStore,
        dataset_reference: HashReference,
        dataset_start_block: Option<BlockNum>,
        table: Table,
        revision: PhysicalTableActiveRevision,
        sql_table_ref_schema: String,
    ) -> Self {
        let url = PhyTableUrl::new(store.url(), &revision.path);
        Self {
            location_id: revision.location_id,
            dataset_reference,
            dataset_start_block,
            sql_table_ref_schema,
            table_name: table.name().clone(),
            network: table.network().to_string(),
            path: revision.path,
            url,
            store,
            table,
        }
    }

    /// Truncate this table by deleting all dump files making up the table
    pub async fn truncate(&self) -> Result<(), BoxError> {
        let file_locations: Vec<Path> = self
            .stream_file_metadata()
            .map_ok(|m| m.object_meta.location)
            .try_collect()
            .await?;
        self.store
            .delete_files_in_object_store(file_locations)
            .await?;
        Ok(())
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
        self.url.inner()
    }

    pub fn path(&self) -> &PhyTableRevisionPath {
        &self.path
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
        self.location_id
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

    pub fn order_exprs(&self) -> Vec<Vec<SortExpr>> {
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

    pub async fn files(&self) -> Result<Vec<FileMetadata>, BoxError> {
        self.stream_file_metadata().try_collect().await
    }

    pub async fn missing_ranges(
        &self,
        desired: RangeInclusive<BlockNum>,
    ) -> Result<Vec<RangeInclusive<BlockNum>>, BoxError> {
        let segments = self.segments().await?;
        Ok(missing_ranges(segments, desired))
    }

    pub async fn canonical_chain(&self) -> Result<Option<Chain>, BoxError> {
        let segments = self.segments().await?;
        let canonical = canonical_chain(segments);
        if let Some(start_block) = self.dataset_start_block
            && let Some(canonical) = &canonical
            && canonical.start() > start_block
        {
            return Ok(None);
        }
        Ok(canonical)
    }

    async fn segments(&self) -> Result<Vec<Segment>, BoxError> {
        self.stream_file_metadata()
            .map(|result| {
                let FileMetadata {
                    file_id,
                    file_name,
                    object_meta,
                    parquet_meta: ParquetMeta { mut ranges, .. },
                    ..
                } = result?;
                if ranges.len() != 1 {
                    return Err(BoxError::from(format!(
                        "expected exactly 1 range for {file_name}"
                    )));
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
    ) -> Result<TableSnapshot, BoxError> {
        let canonical_segments = if ignore_canonical_segments {
            self.segments().await?
        } else {
            self.canonical_chain()
                .await?
                .into_iter()
                .flatten()
                .collect()
        };

        // Create a reader factory with the cached store
        let reader_factory_with_cache = reader::AmpReaderFactory {
            location_id: self.location_id,
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

// Methods for streaming metadata and file information of PhysicalTable
impl PhysicalTable {
    fn stream_file_metadata<'a>(
        &'a self,
    ) -> impl Stream<Item = Result<FileMetadata, BoxError>> + 'a {
        let table_path = self.path.as_object_store_path().clone();
        self.store
            .stream_file_metadata(self.location_id)
            .map(move |row| FileMetadata::from_row_with_table_path(row?, &table_path))
    }
}

// helper methods for implementing `TableProvider` trait
impl PhysicalTable {
    fn object_store_url(&self) -> DataFusionResult<DataFusionObjectStoreUrl> {
        Ok(ListingTableUrl::try_new(self.url.inner().clone(), None)?.object_store())
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
