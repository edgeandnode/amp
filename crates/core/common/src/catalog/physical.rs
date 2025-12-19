use std::{ops::RangeInclusive, sync::Arc};

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
use datasets_common::{hash_reference::HashReference, name::Name, table_name::TableName};
use futures::{Stream, StreamExt, TryStreamExt, stream, stream::BoxStream};
use metadata_db::{LocationId, MetadataDb};
use object_store::{ObjectMeta, ObjectStore, path::Path};
use url::Url;
use uuid::Uuid;

use crate::{
    BlockNum, BoxError, Dataset, LogicalCatalog, ParquetFooterCache, ResolvedTable,
    catalog::reader::AmpReaderFactory,
    metadata::{
        FileMetadata, amp_metadata_from_parquet_file,
        parquet::ParquetMeta,
        segments::{BlockRange, Chain, Segment, canonical_chain, missing_ranges},
    },
    sql::TableReference,
    store::Store,
};

/// Path delimiter used in object store paths.
const PATH_DELIMITER: char = '/';

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
            let dummy_cache = foyer::CacheBuilder::new(1).build();
            let snapshot = table.snapshot(false, dummy_cache).await?;
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
        parquet_footer_cache: ParquetFooterCache,
    ) -> Result<Self, BoxError> {
        let mut table_snapshots = Vec::new();
        for physical_table in &catalog.tables {
            let snapshot = physical_table
                .snapshot(ignore_canonical_segments, parquet_footer_cache.clone())
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
#[tracing::instrument(skip_all, fields(table = %table), err)]
pub async fn register_new_table_revision(
    metadata_db: MetadataDb,
    data_store: &Store,
    dataset: HashReference,
    table: ResolvedTable,
) -> Result<PhysicalTable, RegisterNewTableRevisionError> {
    let revision_id = Uuid::now_v7();
    let path = PhyTableRevisionPath::new(dataset.name(), table.name(), revision_id);
    let url = PhyTableUrl::new(data_store.url(), &path);

    let mut tx = metadata_db
        .begin_txn()
        .await
        .map_err(RegisterNewTableRevisionError::TransactionBegin)?;

    let location_id = metadata_db::physical_table::register(
        &mut tx,
        dataset.namespace(),
        dataset.name(),
        dataset.hash(),
        table.name(),
        &path,
        false,
    )
    .await
    .map_err(RegisterNewTableRevisionError::RegisterPhysicalTable)?;

    metadata_db::physical_table::mark_inactive_by_table_id(&mut tx, dataset.hash(), table.name())
        .await
        .map_err(RegisterNewTableRevisionError::MarkInactive)?;

    metadata_db::physical_table::mark_active_by_id(
        &mut tx,
        location_id,
        dataset.hash(),
        table.name(),
    )
    .await
    .map_err(RegisterNewTableRevisionError::MarkActive)?;

    tx.commit()
        .await
        .map_err(RegisterNewTableRevisionError::TransactionCommit)?;

    tracing::info!("Registered new revision at: {}", url);

    let object_store = data_store.as_inner().clone();

    Ok(PhysicalTable {
        table,
        url,
        path,
        location_id,
        metadata_db,
        object_store,
    })
}

/// Errors that occur when registering a new revision for a physical table
///
/// This error type is used by `register_new_table_revision()`.
#[derive(Debug, thiserror::Error)]
pub enum RegisterNewTableRevisionError {
    /// Failed to begin transaction
    ///
    /// This error occurs when the database connection fails to start a transaction,
    /// typically due to connection issues, database unavailability, or permission problems.
    ///
    /// Possible causes:
    /// - Database connection lost or not established
    /// - Insufficient database permissions to create transactions
    /// - Database server overloaded or unavailable
    /// - Connection pool exhausted
    #[error("Failed to begin transaction")]
    TransactionBegin(#[source] metadata_db::Error),

    /// Failed to register physical table location in metadata database
    ///
    /// This occurs when the metadata database rejects the registration of the new
    /// physical table location. Registration is idempotent - if the URL already exists,
    /// the existing location ID is returned.
    ///
    /// Possible causes:
    /// - Database constraint violation (e.g., invalid foreign key references)
    /// - Database connection lost during operation
    /// - Insufficient permissions to insert into physical_tables table
    #[error("Failed to register physical table in metadata database")]
    RegisterPhysicalTable(#[source] metadata_db::Error),

    /// Failed to mark existing active revisions as inactive
    ///
    /// This occurs when attempting to deactivate all currently active revisions for
    /// the table before activating the new revision.
    ///
    /// Possible causes:
    /// - Database connection lost during update operation
    /// - Database constraint violation during status update
    /// - Concurrent modification conflict
    #[error("Failed to mark existing physical tables as inactive")]
    MarkInactive(#[source] metadata_db::Error),

    /// Failed to mark new physical table revision as active
    ///
    /// This occurs when attempting to activate the newly created revision after
    /// successfully deactivating existing revisions.
    ///
    /// Possible causes:
    /// - Database connection lost during update operation
    /// - Database constraint violation during status update
    /// - Concurrent modification conflict
    #[error("Failed to mark new physical table as active")]
    MarkActive(#[source] metadata_db::Error),

    /// Failed to commit transaction after successful database operations
    ///
    /// When a commit fails, PostgreSQL guarantees that all changes are rolled back.
    /// The operation is safe to retry from the beginning as no partial state was persisted.
    ///
    /// Possible causes:
    /// - Database connection lost during commit
    /// - Transaction conflict with concurrent operations (serialization failure)
    /// - Database constraint violation detected at commit time
    /// - Database running out of disk space or resources
    #[error("Failed to commit transaction")]
    TransactionCommit(#[source] metadata_db::Error),
}

#[derive(Debug, Clone)]
pub struct PhysicalTable {
    /// Logical table representation.
    table: ResolvedTable,

    /// Full URL to the table directory (for compatibility/display).
    url: PhyTableUrl,

    /// Relative path to the table revision in object storage.
    /// Format: `<dataset_name>/<table_name>/<revision_uuid>`
    path: PhyTableRevisionPath,

    /// Location ID in the metadata database.
    location_id: LocationId,
    /// Metadata database to use for this table.
    metadata_db: MetadataDb,
    /// Object store for accessing the data files.
    object_store: Arc<dyn ObjectStore>,
}

// Methods for creating and managing PhysicalTable instances
impl PhysicalTable {
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
    pub async fn restore_latest_revision(
        metadata_db: MetadataDb,
        data_store: Store,
        dataset: &HashReference,
        table: &ResolvedTable,
    ) -> Result<Option<Self>, RestoreLatestRevisionError> {
        let table_path = PhyTablePath::new(dataset.name(), table.name());

        tracing::debug!("Restoring latest revision in prefix {}", table_path);

        let Some(path) = find_table_latest_revision(data_store.as_inner(), &table_path)
            .await
            .map_err(RestoreLatestRevisionError::FindLatestRevision)?
        else {
            return Ok(None);
        };

        let url = PhyTableUrl::new(data_store.url(), &path);

        let mut tx = metadata_db
            .begin_txn()
            .await
            .map_err(RestoreLatestRevisionError::TransactionBegin)?;

        // Register the physical table location (idempotent)
        let location_id = metadata_db::physical_table::register(
            &mut tx,
            dataset.namespace(),
            dataset.name(),
            dataset.hash(),
            table.name(),
            &path,
            false,
        )
        .await
        .map_err(RestoreLatestRevisionError::RegisterPhysicalTable)?;

        // Mark all other revisions as inactive, and this one as active
        metadata_db::physical_table::mark_inactive_by_table_id(
            &mut tx,
            dataset.hash(),
            table.name(),
        )
        .await
        .map_err(RestoreLatestRevisionError::MarkInactive)?;
        metadata_db::physical_table::mark_active_by_id(
            &mut tx,
            location_id,
            dataset.hash(),
            table.name(),
        )
        .await
        .map_err(RestoreLatestRevisionError::MarkActive)?;

        tx.commit()
            .await
            .map_err(RestoreLatestRevisionError::TransactionCommit)?;

        let mut files = data_store
            .as_inner()
            .list(Some(&path))
            .try_collect::<Vec<_>>()
            .await
            .map_err(RestoreLatestRevisionError::ListFiles)?;
        files.sort_unstable_by(|a, b| a.location.cmp(&b.location));

        // Process files in parallel using buffered stream
        const CONCURRENT_METADATA_FETCHES: usize = 16;

        let mut file_stream = stream::iter(files.into_iter())
            .map(|object_meta| {
                let object_store = data_store.as_inner().clone();
                async move {
                    let (file_name, amp_meta, footer) =
                        amp_metadata_from_parquet_file(&object_meta, object_store)
                            .await
                            .map_err(RestoreLatestRevisionError::ReadParquetMetadata)?;

                    let parquet_meta_json = serde_json::to_value(amp_meta)
                        .map_err(RestoreLatestRevisionError::SerializeMetadata)?;

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
            metadata_db::files::register(
                &metadata_db,
                location_id,
                url.inner(),
                file_name,
                object_size,
                object_e_tag,
                object_version,
                parquet_meta_json,
                &footer,
            )
            .await
            .map_err(RestoreLatestRevisionError::RegisterFile)?;
        }

        Ok(Some(Self {
            table: table.clone(),
            url,
            path,
            location_id,
            metadata_db,
            object_store: data_store.as_inner().clone(),
        }))
    }

    /// Attempt to get the active revision of a table.
    pub async fn get_active(
        metadata_db: MetadataDb,
        store: &Store,
        table: &ResolvedTable,
    ) -> Result<Option<Self>, BoxError> {
        let manifest_hash = table.dataset().manifest_hash();
        let table_name = table.name();

        let Some(physical_table) = metadata_db::physical_table::get_active_physical_table(
            &metadata_db,
            manifest_hash,
            table_name,
        )
        .await?
        else {
            return Ok(None);
        };

        // Convert database path to PhyTableRevisionPath
        let path: PhyTableRevisionPath = physical_table.path.into();

        // Construct full URL from store base URL and revision path
        let url = PhyTableUrl::new(store.url(), &path);
        let object_store = store.as_inner().clone();

        Ok(Some(Self {
            table: table.clone(),
            url,
            path,
            location_id: physical_table.id,
            metadata_db,
            object_store,
        }))
    }

    /// Truncate this table by deleting all dump files making up the table
    pub async fn truncate(&self) -> Result<(), BoxError> {
        let file_locations: Vec<Path> = self
            .stream_file_metadata()
            .map_ok(|m| m.object_meta.location)
            .try_collect()
            .await?;
        let num_files = file_locations.len();
        let locations = Box::pin(stream::iter(file_locations.into_iter().map(Ok)));
        let deleted = self
            .object_store
            .delete_stream(locations)
            .try_collect::<Vec<Path>>()
            .await?;
        if deleted.len() != num_files {
            return Err(format!(
                "expected to delete {} files, but deleted {}",
                num_files,
                deleted.len()
            )
            .into());
        }
        Ok(())
    }
}

// Methods for accessing properties of PhysicalTable
impl PhysicalTable {
    pub fn dataset(&self) -> &Arc<Dataset> {
        self.table.dataset()
    }

    pub fn table_name(&self) -> &TableName {
        self.table.name()
    }

    pub fn url(&self) -> &Url {
        self.url.inner()
    }

    pub fn path(&self) -> &PhyTableRevisionPath {
        &self.path
    }

    pub fn catalog_schema(&self) -> &str {
        self.table.catalog_schema()
    }

    pub fn schema(&self) -> SchemaRef {
        self.table.schema().clone()
    }

    pub fn location_id(&self) -> LocationId {
        self.location_id
    }

    /// Qualified table reference in the format `dataset_name.table_name`.
    pub fn table_ref(&self) -> &TableReference {
        self.table.table_ref()
    }

    /// Returns a compact table reference with shortened hash
    pub fn table_ref_compact(&self) -> String {
        format!("{}.{}", self.dataset_ref_compact(), self.table.name())
    }

    /// Returns a compact dataset reference with shortened hash
    fn dataset_ref_compact(&self) -> String {
        match self.table.table_ref().schema() {
            Some(schema_str) => {
                // Parse the schema string as a PartialReference
                match schema_str.parse::<datasets_common::partial_reference::PartialReference>() {
                    Ok(partial_ref) => partial_ref.compact(),
                    // If parsing fails, return the original string
                    Err(_) => schema_str.to_string(),
                }
            }
            // If no schema, return empty string (shouldn't happen for physical tables)
            None => String::new(),
        }
    }

    pub fn order_exprs(&self) -> Vec<Vec<SortExpr>> {
        let sorted_by = self.table().table().sorted_by();
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

    pub fn network(&self) -> &str {
        self.table.network()
    }

    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.object_store)
    }

    pub fn table(&self) -> &ResolvedTable {
        &self.table
    }

    #[tracing::instrument(skip_all, err, fields(table = %self.table_ref_compact()))]
    pub async fn files(&self) -> Result<Vec<FileMetadata>, BoxError> {
        self.stream_file_metadata().try_collect().await
    }

    /// List all files in this physical table's storage location.
    ///
    /// Returns a stream of object metadata for each file in the table's directory.
    pub fn list_files(&self) -> BoxStream<'_, Result<ObjectMeta, object_store::Error>> {
        self.object_store.list(Some(&self.path))
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
        if let Some(start_block) = self.dataset().start_block
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
        parquet_footer_cache: ParquetFooterCache,
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

        // Create a reader factory with the cache
        let reader_factory_with_cache = AmpReaderFactory {
            location_id: self.location_id,
            metadata_db: self.metadata_db.clone(),
            object_store: Arc::clone(&self.object_store),
            parquet_footer_cache,
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
        metadata_db::files::stream_by_location_id_with_details(&self.metadata_db, self.location_id)
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
    reader_factory: Arc<AmpReaderFactory>,
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

    pub fn reader_factory(&self) -> &Arc<AmpReaderFactory> {
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
pub async fn find_table_latest_revision<S>(
    store: &S,
    path: &PhyTablePath,
) -> Result<Option<PhyTableRevisionPath>, FindLatestRevisionError>
where
    S: ObjectStore + ?Sized,
{
    let list_result = store
        .list_with_delimiter(Some(path))
        .await
        .map_err(FindLatestRevisionError)?;

    let latest_revision_path = list_result
        .common_prefixes
        .into_iter()
        .filter_map(|rev| rev.as_ref().parse::<PhyTableRevisionPath>().ok())
        .max_by(|a, b| a.as_str().cmp(b.as_str()));

    Ok(latest_revision_path)
}

/// Error when finding the latest revision from object store
///
/// This error type is used by `find_table_latest_revision()`.
#[derive(Debug, thiserror::Error)]
#[error("Failed to find latest revision from object store")]
pub struct FindLatestRevisionError(#[source] pub object_store::Error);

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

/// Errors that occur when restoring the latest revision of a physical table
///
/// This error type is used by `PhysicalTable::restore_latest_revision()`.
#[derive(Debug, thiserror::Error)]
pub enum RestoreLatestRevisionError {
    /// Failed to find latest revision from object store
    ///
    /// This occurs when the object store cannot be queried for existing revisions,
    /// typically due to network issues, permission errors, or storage unavailability.
    #[error("Failed to find latest revision from object store")]
    FindLatestRevision(#[source] FindLatestRevisionError),

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
    ListFiles(#[source] object_store::Error),

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

/// Physical table URL _new-type_ wrapper
///
/// Represents a base directory URL in the object store containing all parquet files for a table.
/// Individual file URLs are constructed by appending the filename to this base URL.
///
/// ## URL Format
///
/// `<store_base_url>/<dataset_name>/<table_name>/<revision_id>/`
///
/// Where:
/// - `store_base_url`: Object store base URL, may include path prefix after bucket
///   (e.g., `s3://bucket/prefix`, `file:///data/subdir`)
/// - `dataset_name`: Dataset name (without namespace)
/// - `table_name`: Table name
/// - `revision_id`: Unique identifier for this table revision (typically UUIDv7)
///
/// ## Example
///
/// ```text
/// s3://my-bucket/prefix/ethereum_mainnet/logs/01234567-89ab-cdef-0123-456789abcdef/
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PhyTableUrl(Url);

impl PhyTableUrl {
    /// Constructs a table URL from a base URL and revision path.
    ///
    /// The URL follows the structure: `<base_url>/<revision_path>/`
    ///
    /// Where:
    /// - `base_url`: Object store root URL (e.g., `s3://bucket/prefix/`, `file:///data/`)
    /// - `revision_path`: Complete path to the table revision (dataset_name/table_name/revision_uuid)
    pub fn new(base_url: &Url, revision_path: &PhyTableRevisionPath) -> Self {
        // SAFETY: Path components (Name, TableName, Uuid) contain only URL-safe characters
        let raw_url = base_url
            .join(&format!("{}/", revision_path))
            .expect("path is URL-safe");
        PhyTableUrl(raw_url)
    }

    /// Get the URL as a string slice
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Get a reference to the inner [`Url`]
    pub fn inner(&self) -> &Url {
        &self.0
    }
}

impl std::str::FromStr for PhyTableUrl {
    type Err = PhyTableUrlParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let url = s.parse().map_err(|err| PhyTableUrlParseError {
            url: s.to_string(),
            source: err,
        })?;
        Ok(PhyTableUrl(url))
    }
}

impl std::fmt::Display for PhyTableUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.as_str())
    }
}

/// Path to a table directory in object storage (without revision).
///
/// Represents the parent directory containing all revisions of a table.
/// Format: `<dataset_name>/<table_name>`
///
/// **NOTE**: The underlying [`object_store::Path`] type automatically strips leading and
/// trailing slashes, so the string representation will not contain a trailing slash.
///
/// ## Example
///
/// ```text
/// ethereum_mainnet/logs
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhyTablePath(Path);

impl PhyTablePath {
    /// Constructs the path to a table directory (without revision).
    pub fn new(dataset_name: impl AsRef<Name>, table_name: impl AsRef<TableName>) -> Self {
        Self(format!("{}/{}", dataset_name.as_ref(), table_name.as_ref()).into())
    }

    /// Create a revision path by appending the given revision ID to this table path.
    pub fn with_revision(&self, revision_id: impl AsRef<Uuid>) -> PhyTableRevisionPath {
        let path = self.0.child(revision_id.as_ref().to_string());
        PhyTableRevisionPath(path)
    }

    /// Get a reference to the underlying [`object_store::path::Path`]
    pub fn as_object_store_path(&self) -> &Path {
        &self.0
    }

    /// Get the path as a string slice
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

impl std::fmt::Display for PhyTablePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::ops::Deref for PhyTablePath {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Path to a table revision directory in object storage.
///
/// Represents a specific revision of a table, identified by a UUID.
/// Format: `<dataset_name>/<table_name>/<revision_uuid>`
///
/// **NOTE**: The underlying [`object_store::Path`] type automatically strips leading and
/// trailing slashes, so the string representation will not contain a trailing slash.
///
/// ## Example
///
/// ```text
/// ethereum_mainnet/logs/01234567-89ab-cdef-0123-456789abcdef
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhyTableRevisionPath(Path);

impl PhyTableRevisionPath {
    /// Constructs the path to a table revision directory.
    pub fn new(
        dataset_name: impl AsRef<Name>,
        table_name: impl AsRef<TableName>,
        revision_id: impl AsRef<Uuid>,
    ) -> Self {
        Self(
            format!(
                "{}/{}/{}",
                dataset_name.as_ref(),
                table_name.as_ref(),
                revision_id.as_ref()
            )
            .into(),
        )
    }

    /// Get a reference to the underlying [`Path`]
    pub fn as_object_store_path(&self) -> &Path {
        &self.0
    }

    /// Get the path as a string slice
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

impl std::str::FromStr for PhyTableRevisionPath {
    type Err = PhyTableRevisionPathError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.trim_end_matches(PATH_DELIMITER).split(PATH_DELIMITER);

        let revision_uuid: Uuid = parts
            .next_back()
            .filter(|s| !s.is_empty())
            .ok_or(PhyTableRevisionPathError::NotEnoughComponents(0))?
            .parse()
            .map_err(PhyTableRevisionPathError::InvalidRevisionUuid)?;

        let table_name: TableName = parts
            .next_back()
            .filter(|s| !s.is_empty())
            .ok_or(PhyTableRevisionPathError::NotEnoughComponents(1))?
            .parse()
            .map_err(PhyTableRevisionPathError::InvalidTableName)?;

        let dataset_name: Name = parts
            .next_back()
            .filter(|s| !s.is_empty())
            .ok_or(PhyTableRevisionPathError::NotEnoughComponents(2))?
            .parse()
            .map_err(PhyTableRevisionPathError::InvalidDatasetName)?;

        Ok(Self::new(dataset_name, table_name, revision_uuid))
    }
}

impl std::fmt::Display for PhyTableRevisionPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::ops::Deref for PhyTableRevisionPath {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Error when parsing a path into a [`PhyTableRevisionPath`]
#[derive(Debug, thiserror::Error)]
pub enum PhyTableRevisionPathError {
    #[error("path must have at least 3 components, got {0}")]
    NotEnoughComponents(usize),

    #[error("invalid dataset name")]
    InvalidDatasetName(#[source] datasets_common::name::NameError),

    #[error("invalid table name")]
    InvalidTableName(#[source] datasets_common::table_name::TableNameError),

    #[error("invalid revision UUID")]
    InvalidRevisionUuid(#[source] uuid::Error),
}

/// Error type for PhyTableUrl parsing
#[derive(Debug, thiserror::Error)]
#[error("invalid object store URL '{url}'")]
pub struct PhyTableUrlParseError {
    url: String,
    #[source]
    source: url::ParseError,
}

impl From<PhyTableRevisionPath> for metadata_db::physical_table::TablePathOwned {
    fn from(value: PhyTableRevisionPath) -> Self {
        metadata_db::physical_table::TablePath::from_owned_unchecked(value.as_str().to_owned())
    }
}

impl<'a> From<&'a PhyTableRevisionPath> for metadata_db::physical_table::TablePath<'a> {
    fn from(value: &'a PhyTableRevisionPath) -> Self {
        metadata_db::physical_table::TablePath::from_ref_unchecked(value.as_str())
    }
}

impl From<metadata_db::physical_table::TablePathOwned> for PhyTableRevisionPath {
    fn from(value: metadata_db::physical_table::TablePathOwned) -> Self {
        value
            .as_str()
            .parse()
            .expect("database path should be a valid revision path")
    }
}
