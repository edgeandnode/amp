use std::{cmp::Ordering, collections::BTreeMap, path::PathBuf, sync::Arc};

use datafusion::{
    arrow::{compute::SortOptions, datatypes::SchemaRef, error::ArrowError},
    catalog::Session,
    common::{project_schema, Statistics, ToDFSchema},
    datasource::{
        file_format::{parquet::ParquetFormat, FileFormat},
        listing::{ListingTableUrl, PartitionedFile},
        physical_plan::FileScanConfig,
    },
    error::{DataFusionError, Result as DataFusionResult},
    execution::{object_store::ObjectStoreUrl, SessionState},
    logical_expr::{utils::conjunction, Expr as LogicalExpr},
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
    StreamExt, TryStreamExt,
};
use metadata_db::{FileMetadata as NozzleFileMetadata, TableId};
use object_store::{
    path::{Error as PathError, Path},
    ObjectMeta, ObjectStore,
};
use url::Url;

use crate::{
    catalog::logical::Table as LogicalTable,
    meta_tables::scanned_ranges::{ScannedRange, METADATA_KEY},
    BoxError,
};

#[derive(Clone, Debug)]
pub struct DumpListingTable {
    pub(super) name: String,
    pub(super) dataset: String,
    pub(super) dataset_version: Option<String>,
    pub(super) network: Option<String>,

    url: Url,
    path: Path,

    pub(super) format: Arc<ParquetFormat>,
    pub(super) schema: SchemaRef,
    pub(super) order_exprs: Vec<Vec<PhysicalSortExpr>>,
}

impl DumpListingTable {
    pub fn new(
        dataset_name: impl Into<String>,
        dataset_version: Option<String>,
        logical_table: &LogicalTable,
        url: Url,
    ) -> Result<Self, BoxError> {
        let dataset = dataset_name.into();
        let name = logical_table.name.clone();
        let network = logical_table.network.clone();
        let path = Path::from_url_path(url.path())?;

        let format = Arc::new(ParquetFormat::default());
        let schema = logical_table.schema.clone();
        let sort_exprs =
            logical_table
                .sorted_by()
                .into_iter()
                .try_fold(vec![], |mut acc, name| {
                    let index = schema.index_of(&name)?;
                    let col = Column::new(&name, index);
                    let sort_options = SortOptions {
                        descending: false,
                        nulls_first: false,
                    };
                    let sort_expr = PhysicalSortExpr::new(Arc::new(col), sort_options);
                    acc.push(sort_expr);
                    Ok::<_, ArrowError>(acc)
                })?;

        let order_exprs = vec![sort_exprs];

        Ok(Self {
            name,
            dataset,
            dataset_version,
            network,
            url,
            path,
            format,
            schema,
            order_exprs,
        })
    }

    pub(super) fn logical_table(&self) -> LogicalTable {
        LogicalTable {
            name: self.name.clone(),
            schema: self.schema.clone(),
            network: self.network.clone(),
        }
    }

    /// Returns the name of the table.
    pub(super) fn table_name(&self) -> &str {
        &self.name
    }

    /// Returns the table_reference for this table.
    ///
    /// Returns a TableReference::Partial where:
    /// - schema is the dataset name with version if it exists
    /// - table is the table name
    ///
    /// \<dataset>.\<table>
    ///
    /// TODO: The dataset_version should be a partition specifier, not a schema
    /// identifier.
    pub(super) fn table_ref(&self) -> TableReference {
        let schema = self.dataset.as_str();
        let table = self.name.as_str();
        TableReference::partial(schema, table)
    }

    pub(super) fn table_id<'a>(&'a self) -> TableId<'a> {
        TableId::<'a> {
            dataset: &self.dataset,
            dataset_version: self.dataset_version.as_deref(),
            table: &self.name,
        }
    }

    pub(super) fn url(&self) -> &Url {
        &self.url
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn prefix(&self) -> Option<&Path> {
        Some(self.path())
    }

    pub fn listing_table_url(&self) -> DataFusionResult<ListingTableUrl> {
        ListingTableUrl::parse(self.url())
    }

    pub fn object_store_url(&self) -> DataFusionResult<ObjectStoreUrl> {
        let object_store_url = self.url().as_str().split("/").take(1).collect::<String>() + "//";
        ObjectStoreUrl::parse(object_store_url)
    }

    pub fn object_store(
        &self,
        session_state: &SessionState,
    ) -> DataFusionResult<Arc<dyn ObjectStore>> {
        let url = self.object_store_url()?;
        let object_store = session_state.runtime_env().object_store(url)?;
        Ok(object_store)
    }
}

/// Streaming methods for the [`DumpListingTable`]
impl DumpListingTable {
    pub(super) fn stream_files<'a>(
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

    pub(super) fn object_reader_stream<'a>(
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

    fn parquet_metadata_stream<'a>(
        &'a self,
        object_store: &'a Arc<dyn ObjectStore>,
    ) -> BoxStream<'a, DataFusionResult<(Arc<ParquetMetaData>, ObjectMeta)>> {
        self.object_reader_stream(object_store)
            .try_filter_map(|(mut reader, meta)| async move {
                let metadata = reader.get_metadata().await?;
                Ok(Some((metadata, meta)))
            })
            .boxed()
    }

    pub(super) fn ranges_stream<'a>(
        &'a self,
        object_store: &'a Arc<dyn ObjectStore>,
    ) -> BoxStream<'a, DataFusionResult<(u64, u64)>> {
        self.nozzle_metadata_stream(object_store)
            .map(|res| {
                let nozzle_metadata = res?;
                let range = (
                    nozzle_metadata.range.0 as u64,
                    nozzle_metadata.range.1 as u64,
                );

                Ok(range)
            })
            .boxed()
    }

    pub(super) fn nozzle_metadata_stream<'a>(
        &'a self,
        object_store: &'a Arc<dyn ObjectStore>,
    ) -> BoxStream<'a, DataFusionResult<NozzleFileMetadata>> {
        self.parquet_metadata_stream(object_store)
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
                    .iter()
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

    pub(super) async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[LogicalExpr],
        limit: Option<usize>,
        files_and_stats_fut: BoxFuture<
            '_,
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
                let table_df_schema = self.schema.clone().to_dfschema()?;
                let filters =
                    create_physical_expr(&expr, &table_df_schema, state.execution_props())?;
                Some(filters)
            }
            None => None,
        };

        let file_schema = self.schema.clone();
        let object_store_url = self.object_store_url()?;

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

#[derive(Clone, Debug)]
pub(super) struct BlockRange(pub i64, pub i64);
impl From<(i64, i64)> for BlockRange {
    fn from((start, end): (i64, i64)) -> Self {
        BlockRange(start, end)
    }
}

impl PartialEq for BlockRange {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0 && self.1 == other.1
    }
}

impl Eq for BlockRange {}

impl PartialOrd for BlockRange {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.0 == other.0 && self.1 == other.1 {
            Some(Ordering::Equal)
        } else if self.0 == other.0 {
            self.1.partial_cmp(&other.1)
        } else if self.0 < other.0 {
            Some(Ordering::Less)
        } else {
            Some(Ordering::Greater)
        }
    }
}

impl Ord for BlockRange {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.0 == other.0 && self.1 == other.1 {
            Ordering::Equal
        } else if self.0 == other.0 {
            self.1.cmp(&other.1)
        } else if self.0 < other.0 {
            Ordering::Less
        } else {
            Ordering::Greater
        }
    }
}

pub(super) fn validate_name(name: &str) -> Result<(), BoxError> {
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

pub(super) fn split_files(
    table_stats: &mut Statistics,
    columns_to_update: &[usize],
    mut files: BTreeMap<BlockRange, PartitionedFile>,
    n: usize,
) -> DataFusionResult<Vec<Vec<PartitionedFile>>> {
    if files.is_empty() {
        return Ok(vec![]);
    }

    let chunk_size = files.len().div_ceil(n);

    let mut chunks = Vec::with_capacity(n);

    let mut current_chunk = Vec::with_capacity(chunk_size);

    while let Some((_, file)) = files.pop_first() {
        update_table_stats(table_stats, file.statistics.as_ref(), columns_to_update)?;
        current_chunk.push(file);
        if current_chunk.len() == chunk_size {
            let full_chunk = std::mem::replace(&mut current_chunk, Vec::with_capacity(chunk_size));
            chunks.push(full_chunk);
        }
    }

    if !current_chunk.is_empty() {
        chunks.push(current_chunk)
    }

    Ok(chunks)
}

fn update_table_stats(
    table_stats: &mut Statistics,
    file_stats: Option<&Statistics>,
    columns_to_update: &[usize],
) -> DataFusionResult<()> {
    for index in columns_to_update {
        if let (Some(table_stats), Some(file_stats)) = (
            table_stats.column_statistics.get_mut(*index),
            file_stats.and_then(|f| f.column_statistics.get(*index)),
        ) {
            table_stats.max_value = table_stats.max_value.max(&file_stats.max_value);
            table_stats.min_value = table_stats.min_value.min(&file_stats.min_value);
            table_stats.null_count = table_stats.null_count.add(&file_stats.null_count);
            table_stats.distinct_count = table_stats.distinct_count.add(&file_stats.distinct_count);
            table_stats.sum_value = table_stats.sum_value.add(&file_stats.sum_value);
        } else {
            return Err(DataFusionError::Execution(format!(
                "Column statistics not found for index: {index}"
            )));
        }
    }
    if let Some(file_stats) = file_stats {
        table_stats.num_rows = table_stats.num_rows.add(&file_stats.num_rows);
        table_stats.total_byte_size = table_stats.total_byte_size.add(&file_stats.total_byte_size);
    }

    Ok(())
}
