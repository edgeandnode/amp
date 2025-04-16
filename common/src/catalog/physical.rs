use std::{any::Any, borrow::Cow, path::PathBuf, sync::Arc};

use datafusion::{
    arrow::{compute::SortOptions, datatypes::SchemaRef},
    catalog::{Session, TableProvider},
    common::{
        project_schema, stats::Precision, ColumnStatistics, Constraints, Statistics, ToDFSchema,
    },
    datasource::{
        file_format::{parquet::ParquetFormat, FileFormat},
        listing::{ListingTableUrl, PartitionedFile},
        physical_plan::FileScanConfig,
        TableType,
    },
    error::{DataFusionError, Result as DataFusionResult},
    execution::{object_store::ObjectStoreUrl, SessionState},
    logical_expr::{
        utils::conjunction, Expr as LogicalExpr, LogicalPlan, TableProviderFilterPushDown,
    },
    parquet::{
        arrow::async_reader::{AsyncFileReader, ParquetObjectReader},
        file::metadata::{KeyValue, ParquetMetaData},
    },
    physical_expr::{create_physical_expr, LexOrdering, PhysicalSortExpr},
    physical_plan::{empty::EmptyExec, expressions::Column, ExecutionPlan},
    sql::TableReference,
};
use futures::{
    future::BoxFuture,
    stream::{self, BoxStream},
    FutureExt, StreamExt, TryFuture, TryFutureExt, TryStreamExt,
};
use metadata_db::{FileMetadata as NozzleFileMetadata, LocationId, MetadataDb, TableId};
use object_store::{
    path::{Error as PathError, Path},
    ObjectMeta, ObjectStore,
};
use url::Url;

use crate::{
    catalog::Table as LogicalTable,
    meta_tables::scanned_ranges::{ScannedRange, METADATA_KEY},
    BoxError, Dataset, Store, BLOCK_NUM, BLOCK_NUM_INDEX,
};

pub struct Catalog {
    datasets: Vec<PhysicalDataset>,
}

impl Catalog {
    pub fn empty() -> Self {
        Catalog { datasets: vec![] }
    }

    pub fn new(datasets: Vec<PhysicalDataset>) -> Self {
        Catalog { datasets }
    }

    pub fn add(&mut self, dataset: PhysicalDataset) {
        self.datasets.push(dataset);
    }

    /// Will include meta tables.
    pub async fn for_dataset(
        dataset: Dataset,
        data_store: Arc<Store>,
        metadata_db: Option<&MetadataDb>,
    ) -> Result<Self, BoxError> {
        let mut this = Self::empty();
        this.add(PhysicalDataset::from_dataset_at(dataset, data_store, metadata_db, true).await?);
        Ok(this)
    }

    pub fn datasets(&self) -> &[PhysicalDataset] {
        &self.datasets
    }

    pub fn all_tables(&self) -> impl Iterator<Item = &PhysicalTable> {
        self.datasets.iter().flat_map(|dataset| dataset.tables())
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalDataset {
    dataset: Dataset,
    tables: Vec<PhysicalTable>,
}

impl PhysicalDataset {
    pub fn new(dataset: Dataset, tables: Vec<PhysicalTable>) -> Self {
        Self { dataset, tables }
    }

    /// The tables are assumed to live in the subpath:
    /// `<url>/<dataset_name>/<table_name>`
    pub async fn from_dataset_at(
        dataset: Dataset,
        data_store: Arc<Store>,
        metadata_db: Option<&MetadataDb>,
        read_only: bool,
    ) -> Result<Self, BoxError> {
        let dataset_name = dataset.name.clone();
        validate_name(&dataset_name)?;

        let mut physical_tables = vec![];
        for table in dataset.tables() {
            match metadata_db {
                Some(db) => {
                    // If an active location exists for this table, this `PhysicalTable` will point to that location.
                    // Otherwise, a new location will be registered in the metadata DB. The location will be active.
                    let table = {
                        let table_id = TableId {
                            dataset: &dataset_name,
                            dataset_version: None,
                            table: &table.name,
                        };

                        let active_location = db.get_active_location(table_id).await?;
                        match active_location {
                            Some((url, location_id)) => PhysicalTable::new(
                                &dataset_name,
                                table.clone(),
                                url,
                                Some(location_id),
                                Some(db),
                            )?,
                            None => {
                                if read_only {
                                    return Err(format!(
                                        "table {}.{} has no active location",
                                        dataset_name, table.name
                                    )
                                    .into());
                                }
                                PhysicalTable::next_revision(&table, &data_store, &dataset_name, db)
                                    .await?
                            }
                        }
                    };
                    physical_tables.push(table);
                }
                None => {
                    physical_tables.push(PhysicalTable::static_location(
                        data_store.clone(),
                        &dataset_name,
                        &table,
                    )?);
                }
            }
        }

        Ok(PhysicalDataset {
            dataset,
            tables: physical_tables,
        })
    }

    /// All tables in the catalog, except meta tables.
    pub fn tables(&self) -> impl Iterator<Item = &PhysicalTable> {
        self.tables.iter().filter(|table| !table.is_meta())
    }

    pub fn meta_tables(&self) -> impl Iterator<Item = &PhysicalTable> {
        self.tables.iter().filter(|table| table.is_meta())
    }

    pub fn name(&self) -> &str {
        &self.dataset.name
    }

    pub fn kind(&self) -> &str {
        &self.dataset.kind
    }

    pub async fn location_ids(&self) -> Vec<LocationId> {
        stream::iter(&self.tables)
            .filter_map(|t| async move { t.location_id().await })
            .collect()
            .await
    }
}

/// A table in the Nozzle catalog.
///
/// ## Variants
/// - [Local](NozzleTable::Local): A table that is backed by files generated by Nozzle dump.
/// - [Optimized](NozzleTable::Optimized): A table that is backed by files generated by Nozzle dump,
///  optimized with an external Metadata provider.
/// - [Other](NozzleTable::Other): A table that is not backed by an object store but may be present
/// in a Nozzle dataset.
#[derive(Clone, Debug)]
pub enum PhysicalTable {
    /// A [`DumpTable`] that is backed by files generated by Nozzle dump.
    Local {
        /// The [`DumpTable`] that is backed by files generated by Nozzle dump.
        dump: Arc<DumpTable>,
        object_store: Arc<dyn ObjectStore>,
    },
    /// A [`DumpTable`] that is backed by files generated by Nozzle dump,
    /// This table is optimized with an external Metadata provider
    Optimized {
        /// The [`DumpTable`] that is backed by files generated by Nozzle dump.
        dump: Arc<DumpTable>,
        /// The [`MetadataDb`] that is used to optimize the table.
        metadata_provider: Arc<MetadataDb>,
    },
    /// For memory tables, streaming tables, or other tables that are
    /// not backed by an object store but may be present in a nozzle
    /// dataset.
    Other {
        /// The table provider that is not backed by an object store.
        table_provider: Arc<dyn TableProvider>,
        /// The logical table that is not backed by an object store.
        logical_table: Arc<LogicalTable>,
        /// The table reference for the table that is not backed by an object store.
        table_ref: Arc<TableReference>,
    },
}

impl PhysicalTable {
    pub fn table_ref(&self) -> Arc<TableReference> {
        use PhysicalTable::*;
        match self {
            Local { dump, .. } | Optimized { dump, .. } => dump.table_ref(),
            Other { table_ref, .. } => table_ref.clone(),
        }
    }

    pub fn table_name(&self) -> &str {
        use PhysicalTable::*;
        match self {
            Local { dump, .. } | Optimized { dump, .. } => dump.table_name(),
            Other { table_ref, .. } => table_ref.table(),
        }
    }

    pub fn is_meta(&self) -> bool {
        use PhysicalTable::*;
        match self {
            Local { .. } | Optimized { .. } => false,
            Other { logical_table, .. } => logical_table.is_meta(),
        }
    }

    pub async fn location_id(&self) -> Option<LocationId> {
        use PhysicalTable::*;
        match self {
            Optimized {
                dump,
                metadata_provider,
            } => {
                metadata_provider
                    .get_active_location(dump.table_id())
                    .map(|result| {
                        result
                            .expect("Inconsistent State in the MetadataDb. Multiple active locations for the same table.")
                            .map(|(_, location_id)| location_id)
                    })
                    .await
            }
            _ => None,
        }
    }
}

/// Streaming methods for the [`NozzleTable`].
impl PhysicalTable {
    pub fn physical_sort_exprs<'a>(&self) -> BoxStream<'a, BoxStream<'a, PhysicalSortExpr>> {
        use PhysicalTable::*;
        match self {
            Local { dump, .. } | Optimized { dump, .. } => dump.physical_sort_exprs(),
            _ => stream::empty().map(|_: ()| stream::empty().boxed()).boxed(),
        }
    }

    fn stream_files<'a>(
        &'a self,
    ) -> DataFusionResult<BoxStream<'a, DataFusionResult<(String, ObjectMeta)>>> {
        use PhysicalTable::*;
        match self {
            Local { dump, object_store } => Ok(dump.stream_files(object_store)),
            Optimized {
                dump,
                metadata_provider,
            } => {
                let tbl = dump.table_id();
                Ok(metadata_provider
                    .stream_object_meta(tbl)
                    .map_err(|e| DataFusionError::External(Box::new(e)))
                    .boxed())
            }
            Other { table_ref, .. } => Err(DataFusionError::Internal(format!(
                "NozzleTable::stream_files: {} not a Dump Table",
                table_ref
            ))),
        }
    }

    fn stream_object_readers<'a>(
        &'a self,
    ) -> DataFusionResult<BoxStream<'a, DataFusionResult<(ParquetObjectReader, ObjectMeta)>>> {
        use PhysicalTable::*;
        match self {
            Local { dump, object_store } => Ok(dump.stream_object_readers(object_store).boxed()),

            Optimized { .. } => Err(DataFusionError::Internal(
                "NozzleTable::stream_object_readers: Optimized table not supported".to_string(),
            )),

            Other { .. } => Err(DataFusionError::Internal(
                "NozzleTable::stream_object_readers: Other table not supported".to_string(),
            )),
        }
    }

    fn stream_nozzle_metadata<'a>(
        &'a self,
    ) -> DataFusionResult<BoxStream<'a, DataFusionResult<NozzleFileMetadata>>> {
        use PhysicalTable::*;
        match self {
            Local {
                dump, object_store, ..
            } => Ok(dump.stream_nozzle_metadata(object_store)),
            Optimized {
                dump,
                metadata_provider,
                ..
            } => Ok(metadata_provider
                .stream_nozzle_metadata(dump.table_id())
                .map_err(|e| DataFusionError::External(Box::new(e)))
                .boxed()),
            Other { .. } => Err(DataFusionError::Internal(
                "NozzleTable::stream_nozzle_metadata: Other table not supported".to_string(),
            )),
        }
    }

    async fn list_files_for_scan<'a>(
        &'a self,
        _filters: &'a [LogicalExpr],
        _limit: Option<usize>,
    ) -> DataFusionResult<(Vec<Vec<PartitionedFile>>, Statistics)> {
        use Precision::*;

        let mut files = vec![];
        let mut table_stats = Statistics::new_unknown(self.schema().as_ref());
        let mut stream = self.stream_nozzle_metadata()?;

        let block_num_idx = self.schema().index_of(BLOCK_NUM)?;

        while let Some(NozzleFileMetadata {
            object_meta,
            range: (range_start, range_end),
            row_count,
            data_size,
            size_hint,
        }) = stream.try_next().await?
        {
            let block_num_stats = ColumnStatistics {
                null_count: Exact(0),
                max_value: Inexact(range_end.into()),
                min_value: Inexact(range_start.into()),
                ..Default::default()
            };

            let mut file_stats = Statistics::new_unknown(&self.schema().clone());

            file_stats
                .column_statistics
                .insert(block_num_idx, block_num_stats);

            if let Some(num_rows) = row_count {
                file_stats.num_rows = Exact(num_rows as usize);
            }

            if let Some(total_size_bytes) = data_size {
                file_stats.total_byte_size = Exact(total_size_bytes as usize);
            }

            let mut partitioned_file = PartitionedFile::from(object_meta);
            partitioned_file.metadata_size_hint = size_hint.map(|s| s as usize);

            update_table_stats(&mut table_stats, &file_stats, &[block_num_idx]);

            files.push(partitioned_file);
        }

        let split_files = split_files(files, 10);

        Ok((split_files, table_stats))
    }
}

impl TableProvider for PhysicalTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        use PhysicalTable::*;
        match self {
            Local { dump, .. } | Optimized { dump, .. } => dump.schema.clone(),
            Other { table_provider, .. } => table_provider.schema(),
        }
    }

    fn table_type(&self) -> TableType {
        use PhysicalTable::*;
        match self {
            Other { table_provider, .. } => table_provider.table_type(),
            _ => TableType::Base,
        }
    }

    // Not using `async_trait` here as this is a wrapper for the
    // `scan` method of the underlying table provider.
    fn scan<'this, 'state, 'projection, 'filters, 'async_trait>(
        &'this self,
        state: &'state dyn Session,
        projection: Option<&'projection Vec<usize>>,
        filters: &'filters [LogicalExpr],
        limit: Option<usize>,
    ) -> BoxFuture<'async_trait, DataFusionResult<Arc<dyn ExecutionPlan>>>
    where
        'this: 'async_trait,
        'state: 'async_trait,
        'projection: 'async_trait,
        'filters: 'async_trait,
        Self: 'async_trait,
    {
        use PhysicalTable::*;
        match self {
            Other { table_provider, .. } => table_provider.scan(state, projection, filters, limit),
            Local { dump, .. } | Optimized { dump, .. } => dump
                .scan(
                    state,
                    projection,
                    filters,
                    limit,
                    self.list_files_for_scan(filters, limit).boxed(),
                )
                .boxed(),
        }
    }

    fn get_table_definition(&self) -> Option<&str> {
        use PhysicalTable::*;
        match self {
            Other { table_provider, .. } => table_provider.get_table_definition(),
            _ => None,
        }
    }

    fn constraints(&self) -> Option<&Constraints> {
        use PhysicalTable::*;
        match self {
            Other { table_provider, .. } => table_provider.constraints(),
            _ => None,
        }
    }

    fn get_column_default(&self, _column: &str) -> Option<&LogicalExpr> {
        use PhysicalTable::*;
        match self {
            Other { table_provider, .. } => table_provider.get_column_default(_column),
            _ => None,
        }
    }

    fn get_logical_plan(&self) -> Option<Cow<LogicalPlan>> {
        use PhysicalTable::*;
        match self {
            Other { table_provider, .. } => table_provider.get_logical_plan(),
            _ => None,
        }
    }

    fn statistics(&self) -> Option<Statistics> {
        use PhysicalTable::*;
        match self {
            Local { .. } => None,
            Optimized { .. } => None,
            Other { table_provider, .. } => table_provider.statistics(),
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&LogicalExpr],
    ) -> DataFusionResult<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        use PhysicalTable::*;
        match self {
            Local { .. } | Optimized { .. } => Ok(vec![
                TableProviderFilterPushDown::Unsupported;
                filters.len()
            ]),
            Other { table_provider, .. } => table_provider.supports_filters_pushdown(filters),
        }
    }
}

#[derive(Clone, Debug)]
pub struct DumpTable {
    name: String,
    dataset: String,
    dataset_version: Option<String>,
    network: Option<String>,

    url: Url,
    path: Path,

    format: Arc<ParquetFormat>,
    schema: SchemaRef,
    order_exprs: Vec<Vec<PhysicalSortExpr>>,
}

impl DumpTable {
    fn table_name(&self) -> &str {
        &self.name
    }

    /// Returns the table_reference for this table.
    ///
    /// Returns a TableReference::Partial where:
    /// - schema is the dataset name with version if it exists
    /// - table is the table name
    ///
    /// \<dataset>\[@<dataset_version>].\<table>
    ///
    /// TODO: The dataset_version should be a partition specifier, not a schema
    /// identifier.
    fn table_ref(&self) -> Arc<TableReference> {
        let schema = match self.dataset_version {
            Some(ref version) => format!("{}@{version}", self.dataset),
            None => self.dataset.clone(),
        };
        let table = self.name.as_str();
        Arc::new(TableReference::partial(schema, table))
    }

    fn table_id<'a>(&'a self) -> TableId<'a> {
        TableId::<'a> {
            dataset: &self.dataset,
            dataset_version: self.dataset_version.as_ref().map(String::as_str),
            table: &self.name,
        }
    }

    fn url(&self) -> &Url {
        &self.url
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn prefix(&self) -> Option<&Path> {
        Some(self.path())
    }

    fn listing_table_url(&self) -> DataFusionResult<ListingTableUrl> {
        ListingTableUrl::parse(self.url())
    }

    fn object_store_url(&self) -> DataFusionResult<ObjectStoreUrl> {
        ObjectStoreUrl::parse(self.url.as_str())
    }

    fn object_store(&self, session_state: &SessionState) -> DataFusionResult<Arc<dyn ObjectStore>> {
        let url = self.object_store_url()?;
        let object_store = session_state.runtime_env().object_store(url)?;
        Ok(object_store)
    }

    fn try_from_static_location(
        store: Arc<Store>,
        dataset: &str,
        logical_table: LogicalTable,
    ) -> Result<Self, BoxError> {
        validate_name(&logical_table.name)?;
        let name = logical_table.name.to_string();
        let dataset = dataset.to_string();
        let dataset_version = None;

        let network = logical_table.network.to_owned();

        let input = format!("{}/{}/", dataset, logical_table.name);
        let url = store.url().join(&input)?;
        let path = Path::from_url_path(url.path())?;

        let schema = logical_table.schema.clone();
        let order_exprs = vec![logical_table
            .sorted_by()
            .into_iter()
            .map(|name| {
                let index = schema.index_of(&name).expect("Column not found in schema");
                let col = Column::new(&name, index);
                let sort_options = SortOptions {
                    descending: false,
                    nulls_first: false,
                };
                PhysicalSortExpr::new(Arc::new(col), sort_options)
            })
            .collect()];

        let format = Arc::new(ParquetFormat::default());

        Ok(DumpTable {
            name,
            dataset,
            dataset_version,
            network,
            url,
            path,
            schema,
            format,
            order_exprs,
        })
    }

    fn stream_files<'a>(
        &'a self,
        object_store: &'a dyn ObjectStore,
    ) -> BoxStream<'a, DataFusionResult<(String, ObjectMeta)>> {
        object_store
            .list(self.prefix())
            .map(|entry| {
                let meta = entry?;

                let file_name = meta
                    .location
                    .filename()
                    .ok_or(PathError::InvalidPath {
                        path: PathBuf::from(meta.location.to_string()),
                    })?
                    .to_string();

                Ok((file_name, meta))
            })
            .boxed()
    }

    fn stream_object_readers<'a>(
        &'a self,
        store: &'a Arc<dyn ObjectStore>,
    ) -> BoxStream<'a, DataFusionResult<(ParquetObjectReader, ObjectMeta)>> {
        self.stream_files(store)
            .zip(stream::repeat(store))
            .map(|(res, store)| {
                let (_, meta) = res?;
                let object_reader = ParquetObjectReader::new(store.clone(), meta.clone());
                Ok((object_reader, meta))
            })
            .boxed()
    }

    fn stream_parquet_metadata<'a>(
        &'a self,
        object_store: &'a Arc<dyn ObjectStore>,
    ) -> BoxStream<'a, DataFusionResult<(Arc<ParquetMetaData>, ObjectMeta)>> {
        self.stream_object_readers(object_store)
            .try_filter_map(|(mut reader, meta)| async move {
                let metadata = reader.get_metadata().await?;
                Ok(Some((metadata, meta)))
            })
            .boxed()
    }

    fn stream_nozzle_metadata<'a>(
        &'a self,
        object_store: &'a Arc<dyn ObjectStore>,
    ) -> BoxStream<'a, DataFusionResult<NozzleFileMetadata>> {
        self.stream_parquet_metadata(object_store)
            .map(|res| {
                let (parquet_meta, object_meta) = res?;
                let file_metadata = parquet_meta.file_metadata();

                let kv_metadata =
                    file_metadata
                        .key_value_metadata()
                        .ok_or(DataFusionError::Internal(format!(
                            "Key-value metadata not found in parquet file metadata for file: {}",
                            object_meta.location
                        )))?;

                let nozzle_metadata_key_value_pair = kv_metadata
                    .into_iter()
                    .find(|KeyValue { key, .. }| key == METADATA_KEY)
                    .ok_or(crate::ArrowError::ParquetError(format!(
                        "Missing key: {} in file metadata for file {}",
                        METADATA_KEY, object_meta.location
                    )))?;

                let scanned_range_json = nozzle_metadata_key_value_pair.value.as_ref().ok_or(
                    crate::ArrowError::ParseError(format!(
                        "Missing value for key: {} in file metadata for file {}",
                        METADATA_KEY, object_meta.location
                    )),
                )?;

                let scanned_range: ScannedRange = serde_json::from_str(scanned_range_json)
                    .map_err(|e| {
                        DataFusionError::Internal(format!(
                            "Failed to parse JSON from value for key: {} in file metadata for file {}: {}",
                            METADATA_KEY,
                            object_meta.location,
                            e
                        ))
                    })?;

                let file_metadata = NozzleFileMetadata {
                    object_meta,
                    range: (
                        scanned_range.range_start as i64,
                        scanned_range.range_end as i64,
                    ),
                    row_count: None,
                    data_size: None,
                    size_hint: None,
                };

                Ok(file_metadata)
            })
            .boxed()
    }

    fn physical_sort_exprs<'a>(&self) -> BoxStream<'a, BoxStream<'a, PhysicalSortExpr>> {
        stream::iter(self.order_exprs.clone())
            .map(|exprs| stream::iter(exprs).boxed())
            .boxed()
    }

    fn sort_exprs(&self) -> Vec<Vec<PhysicalSortExpr>> {
        let col = Column::new(BLOCK_NUM, BLOCK_NUM_INDEX);
        let sort_options = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let sort_expr = PhysicalSortExpr::new(Arc::new(col), sort_options);
        let sort_exprs = vec![vec![sort_expr]];
        sort_exprs
    }

    async fn scan<'a>(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[LogicalExpr],
        limit: Option<usize>,
        files_and_stats_fut: BoxFuture<
            'a,
            DataFusionResult<(Vec<Vec<PartitionedFile>>, Statistics)>,
        >,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let (mut partitioned_file_lists, statistics) = files_and_stats_fut.await?;

        let session_state = state.as_any().downcast_ref::<SessionState>().unwrap();
        // if no files need to be read, return an `EmptyExec`
        if partitioned_file_lists.is_empty() {
            let projected_schema = project_schema(&self.schema, projection)?;
            return Ok(Arc::new(EmptyExec::new(projected_schema)));
        }

        let mut output_ordering: Vec<LexOrdering> = vec![];

        for sort_exprs in self.order_exprs.iter() {
            let mut lex_ordering = LexOrdering::default();
            for sort_expr in sort_exprs {
                lex_ordering.push(sort_expr.clone());
            }
            output_ordering.push(lex_ordering);
        }

        match state
            .config_options()
            .execution
            .split_file_groups_by_statistics
            .then(|| {
                output_ordering.first().map(|output_ordering| {
                    FileScanConfig::split_groups_by_statistics(
                        &self.schema,
                        &partitioned_file_lists,
                        output_ordering,
                    )
                })
            })
            .flatten()
        {
            Some(Err(e)) => log::debug!("failed to split file groups by statistics: {e}"),
            Some(Ok(new_groups)) => {
                if new_groups.len() <= 10 {
                    partitioned_file_lists = new_groups;
                } else {
                    log::debug!("attempted to split file groups by statistics, but there were more file groups than target_partitions; falling back to unordered")
                }
            }
            None => {} // no ordering required
        };

        let filters = match conjunction(filters.to_vec()) {
            Some(expr) => {
                let table_df_schema = self.schema.as_ref().clone().to_dfschema()?;
                let filters =
                    create_physical_expr(&expr, &table_df_schema, state.execution_props())?;
                Some(filters)
            }
            None => None,
        };

        let file_schema = self.schema.clone();

        let object_store_url = ObjectStoreUrl::parse(self.url().as_str())?;

        self.format
            .create_physical_plan(
                session_state,
                FileScanConfig::new(object_store_url, file_schema)
                    .with_file_groups(partitioned_file_lists)
                    .with_statistics(statistics)
                    .with_projection(projection.cloned())
                    .with_limit(limit)
                    .with_output_ordering(output_ordering),
                filters.as_ref(),
            )
            .await
    }
}

fn validate_name(name: &str) -> Result<(), BoxError> {
    if let Some(c) = name
        .chars()
        .find(|&c| !(c.is_ascii_lowercase() || c == '_' || c.is_numeric()))
    {
        return Err(format!(
            "names must be lowercase and contain only letters, underscores, and numbers, \
             the name: '{name}' is not allowed because it contains the character '{c}'"
        )
        .into());
    }

    Ok(())
}

fn split_files(mut files: Vec<PartitionedFile>, n: usize) -> Vec<Vec<PartitionedFile>> {
    if files.is_empty() {
        return vec![];
    }
    files.sort_by(|a, b| a.path().cmp(b.path()));

    let chunk_size = files.len().div_ceil(n);

    let mut chunks = Vec::with_capacity(n);

    let mut current_chunk = Vec::with_capacity(chunk_size);

    for file in files.drain(..) {
        current_chunk.push(file);
        if current_chunk.len() == chunk_size {
            let full_chunk = std::mem::replace(&mut current_chunk, Vec::with_capacity(chunk_size));
            chunks.push(full_chunk);
        }
    }

    if !current_chunk.is_empty() {
        chunks.push(current_chunk)
    }

    chunks
}

fn update_table_stats(
    table_stats: &mut Statistics,
    file_stats: &Statistics,
    columns_to_update: &[usize],
) -> DataFusionResult<()> {
    for index in columns_to_update {
        if let (Some(table_stats), Some(file_stats)) = (
            table_stats.column_statistics.get_mut(*index),
            file_stats.column_statistics.get(*index),
        ) {
            table_stats.max_value = table_stats.max_value.max(&file_stats.max_value);
            table_stats.min_value = table_stats.min_value.min(&file_stats.min_value);
            table_stats.null_count = table_stats.null_count.add(&file_stats.null_count);
            table_stats.distinct_count = table_stats.distinct_count.add(&file_stats.distinct_count);
            table_stats.sum_value = table_stats.sum_value.add(&file_stats.sum_value);
        } else {
            return Err(DataFusionError::Execution(
                format!("Column statistics not found for index: {index}").into(),
            ));
        }
    }
    table_stats.num_rows = table_stats.num_rows.add(&file_stats.num_rows);
    table_stats.total_byte_size = table_stats.total_byte_size.add(&file_stats.total_byte_size);

    Ok(())
}

// The path format is: `<dataset>/[<version>/]<table>/<UUIDv7>/`
fn make_location_path(table_id: TableId<'_>) -> String {
    let mut path = String::new();

    // Add dataset
    path.push_str(table_id.dataset);
    path.push('/');

    // Add version if present
    if let Some(version) = table_id.dataset_version {
        path.push_str(version);
        path.push('/');
    }

    // Add table
    path.push_str(table_id.table);
    path.push('/');

    // Add UUIDv7
    let uuid = uuid::Uuid::now_v7();
    path.push_str(&uuid.to_string());
    path.push('/');

    path
}
