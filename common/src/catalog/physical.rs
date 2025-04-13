use std::{collections::BTreeMap, sync::Arc};

use datafusion::{
    arrow::{compute::SortOptions, datatypes::SchemaRef},
    catalog::Session,
    common::{
        plan_err, project_schema,
        stats::{ColumnStatistics, Precision, Statistics},
        ToDFSchema,
    },
    datasource::{
        file_format::{parquet::ParquetFormat, FileFormat},
        listing::PartitionedFile,
        physical_plan::FileScanConfig,
        TableProvider, TableType,
    },
    error::{DataFusionError, Result as DataFusionResult},
    execution::{object_store::ObjectStoreUrl, SessionState},
    logical_expr::{col, utils::conjunction, SortExpr},
    parquet::{
        arrow::async_reader::{AsyncFileReader, ParquetObjectReader},
        file::metadata,
    },
    physical_expr::{create_physical_expr, expressions::Column, LexOrdering, PhysicalSortExpr},
    physical_plan::{empty::EmptyExec, expressions, ExecutionPlan},
    prelude::Expr,
    sql::TableReference,
};
use futures::{
    stream::{self, BoxStream},
    StreamExt, TryStreamExt,
};
use metadata_db::{FileMetadata, LocationId, MetadataDb, TableId};
use object_store::{path::Path, ObjectMeta, ObjectStore};
use url::Url;

use super::logical::Table;
use crate::{
    meta_tables::scanned_ranges::{self, ScannedRange},
    store::{infer_object_store, Store},
    BoxError, Dataset, BLOCK_NUM, BLOCK_NUM_INDEX,
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

    pub fn location_ids(&self) -> Vec<LocationId> {
        self.tables.iter().filter_map(|t| t.location_id()).collect()
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalTable {
    table: Table,
    table_ref: TableReference,

    // Absolute URL to the data location, path section of the URL and the corresponding object store.
    url: Url,
    path: Path,

    // The underlying file format.
    format: Arc<ParquetFormat>,
    // The object store for this table.
    object_store: Arc<dyn ObjectStore>,
    // Optional MetadataDb location ID.
    location_id: Option<LocationId>,
    // Optional MetadataDb for this table.
    metadata_db: Option<Arc<MetadataDb>>,
}

impl PhysicalTable {
    pub fn new(
        dataset_name: &str,
        table: Table,
        url: Url,
        location_id: Option<LocationId>,
        metadata_db: Option<&MetadataDb>,
    ) -> Result<Self, BoxError> {
        validate_name(&table.name)?;

        let table_ref = TableReference::partial(dataset_name, table.name.as_str());
        let path = Path::from_url_path(url.path()).unwrap();
        let (object_store, _) = infer_object_store(&url)?;

        let metadata_db = metadata_db.map(|db| Arc::new(db.clone()));

        let format = Arc::new(ParquetFormat::default());

        Ok(Self {
            table,
            table_ref,
            url,
            path,
            format,
            object_store,
            location_id,
            metadata_db,
        })
    }

    /// The static location is always `<base>/<dataset_name>/<table_name>/`.
    ///
    /// This is used in a few situations where no metadata DB is available:
    /// - When using `dump` as a simple CLI tool without a Postgres for metadata.
    /// - For snapshot testing.
    fn static_location(
        data_store: Arc<Store>,
        dataset_name: &str,
        table: &Table,
    ) -> Result<Self, BoxError> {
        validate_name(&table.name)?;

        let path = format!("{}/{}/", dataset_name, table.name);
        let url = data_store.url().join(&path)?;

        let table_ref = TableReference::partial(dataset_name, table.name.as_str());
        let path = Path::from_url_path(url.path()).unwrap();

        let format = Arc::new(ParquetFormat::default());
        let object_store = data_store.object_store();

        Ok(PhysicalTable {
            table: table.clone(),
            table_ref,
            url,
            path,
            format,
            object_store,
            location_id: None,
            metadata_db: None,
        })
    }

    pub fn table_name(&self) -> &str {
        &self.table.name
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn catalog_schema(&self) -> &str {
        // Unwrap: This is always constructed with a schema.
        &self.table_ref.schema().unwrap()
    }

    pub fn is_meta(&self) -> bool {
        self.table.is_meta()
    }

    pub fn schema(&self) -> SchemaRef {
        self.table.schema.clone()
    }

    pub fn location_id(&self) -> Option<LocationId> {
        self.location_id
    }

    /// Qualified table reference in the format `dataset_name.table_name`.
    pub fn table_ref(&self) -> &TableReference {
        &self.table_ref
    }

    pub fn table_id(&self) -> TableId<'_> {
        TableId {
            dataset: self.catalog_schema(),
            dataset_version: None,
            table: self.table_name(),
        }
    }

    pub fn order_exprs(&self) -> Vec<Vec<SortExpr>> {
        self.table
            .sorted_by()
            .iter()
            .map(|col_name| vec![col(col_name).sort(true, false)])
            .collect()
    }

    pub fn sort_exprs(&self) -> Vec<Vec<PhysicalSortExpr>> {
        let col = Column::new(BLOCK_NUM, BLOCK_NUM_INDEX);
        let sort_options = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let sort_expr = PhysicalSortExpr::new(Arc::new(col), sort_options);
        let sort_exprs = vec![vec![sort_expr]];
        sort_exprs
    }

    pub fn network(&self) -> Option<&str> {
        self.table.network.as_ref().map(|n| n.as_str())
    }

    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.object_store.clone()
    }

    pub fn table(&self) -> &Table {
        &self.table
    }

    pub async fn next_revision(
        table: &Table,
        data_store: &Store,
        dataset_name: &str,
        db: &MetadataDb,
    ) -> Result<Self, BoxError> {
        let table_id = TableId {
            dataset: dataset_name,
            dataset_version: None,
            table: &table.name,
        };

        let path = make_location_path(table_id);
        let url = data_store.url().join(&path)?;
        let location_id = db
            .register_location(table_id, data_store.bucket(), &path, &url, false)
            .await?;
        db.set_active_location(table_id, &url.as_str()).await?;

        let path = Path::from_url_path(url.path()).unwrap();
        let table_ref = TableReference::partial(dataset_name, table.name.as_str());

        let metadata_db = Some(Arc::new(db.clone()));

        let physical_table = Self {
            table: table.clone(),
            table_ref,
            url,
            path,
            format: Arc::new(ParquetFormat::default()),
            object_store: data_store.object_store(),
            location_id: Some(location_id),
            metadata_db,
        };
        Ok(physical_table)
    }

    /// Return all parquet files for this table. If `dump_only` is `true`,
    /// only files of the form `<number>.parquet` will be returned. The
    /// result is a map from filename to object metadata.
    pub async fn parquet_files(
        &self,
        dump_only: bool,
    ) -> object_store::Result<BTreeMap<String, ObjectMeta>> {
        // Check that this is a file written by a dump job, with name in the format:
        // "<block_num>.parquet".
        let is_dump_file = |filename: &str| {
            filename.ends_with(".parquet")
                && (!dump_only || filename.trim_end_matches(".parquet").parse::<u64>().is_ok())
        };

        let files = self
            .object_store
            .list(Some(&self.path))
            .try_collect::<Vec<ObjectMeta>>()
            .await?
            .into_iter()
            // Unwrap: A full object path always has a filename.
            .map(|f| (f.location.filename().unwrap().to_string(), f))
            .filter(|(filename, _)| is_dump_file(filename))
            .collect();
        Ok(files)
    }

    pub async fn ranges(&self) -> Result<Vec<(u64, u64)>, BoxError> {
        let mut ranges = vec![];
        let mut file_list = self.object_store.list(Some(self.path()));

        while let Some(object_meta_result) = file_list.next().await {
            let meta = object_meta_result?;
            let location = &meta.location.to_string();

            let mut reader = ParquetObjectReader::new(self.object_store.clone(), meta);

            let parquet_metadata = reader.get_metadata().await?;

            let key_value_metadata = parquet_metadata
                .file_metadata()
                .key_value_metadata()
                .ok_or(crate::ArrowError::ParquetError(format!(
                    "Unable to fetch Key Value metadata for file {}",
                    location
                )))?;

            let scanned_range_key_value_pair = key_value_metadata
                .into_iter()
                .find(|key_value| key_value.key.as_str() == scanned_ranges::METADATA_KEY)
                .ok_or(crate::ArrowError::ParquetError(format!(
                    "Missing key: {} in file metadata for file {}",
                    scanned_ranges::METADATA_KEY,
                    self.path
                )))?;

            let range = scanned_range_key_value_pair
                .value
                .as_ref()
                .ok_or(crate::ArrowError::ParseError(format!(
                    "Unable to parse ScannedRange from key value metadata for file {}",
                    self.path
                )))
                .map(|scanned_range_json| serde_json::from_str::<ScannedRange>(scanned_range_json))?
                .map(|scanned_range| (scanned_range.range_start, scanned_range.range_end))?;

            ranges.push(range);
        }

        Ok(ranges)
    }

    /// Truncate this table by deleting all dump files making up the table
    pub async fn truncate(&self) -> Result<(), BoxError> {
        let files = self.parquet_files(false).await?;
        let num_files = files.len();
        let locations = Box::pin(stream::iter(files.into_values().map(|m| Ok(m.location))));
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

#[async_trait::async_trait]
impl TableProvider for PhysicalTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.table.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let session_state = state.as_any().downcast_ref::<SessionState>().unwrap();

        let statistic_file_limit = if filters.is_empty() { limit } else { None };

        let (mut partitioned_file_lists, statistics) = self
            .list_files_for_scan(session_state, filters, statistic_file_limit)
            .await?;

        // if no files need to be read, return an `EmptyExec`
        if partitioned_file_lists.is_empty() {
            let projected_schema = project_schema(&self.schema(), projection)?;
            return Ok(Arc::new(EmptyExec::new(projected_schema)));
        }

        let mut output_ordering = vec![];

        for exprs in self.order_exprs() {
            let mut sort_exprs = LexOrdering::default();
            for sort in exprs {
                match &sort.expr {
                    Expr::Column(column) => match expressions::col(&column.name, &self.schema()) {
                        Ok(expr) => {
                            sort_exprs.push(PhysicalSortExpr {
                                expr,
                                options: SortOptions {
                                    descending: !sort.asc,
                                    nulls_first: sort.nulls_first,
                                },
                            });
                        }
                        Err(_) => break,
                    },
                    expr => {
                        return plan_err!(
                            "Expected single column references in output_ordering, got {expr}"
                        )
                    }
                }
            }
            if !sort_exprs.is_empty() {
                output_ordering.push(sort_exprs);
            }
        }
        match state
            .config_options()
            .execution
            .split_file_groups_by_statistics
            .then(|| {
                output_ordering.first().map(|output_ordering| {
                    FileScanConfig::split_groups_by_statistics(
                        &self.schema(),
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
                let table_df_schema = self.schema().as_ref().clone().to_dfschema()?;
                let filters =
                    create_physical_expr(&expr, &table_df_schema, state.execution_props())?;
                Some(filters)
            }
            None => None,
        };

        let file_schema = self.schema();

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

impl PhysicalTable {
    async fn list_files_for_scan<'a>(
        &'a self,
        _ctx: &'a SessionState,
        _filters: &'a [Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<(Vec<Vec<PartitionedFile>>, Statistics)> {
        let mut files = vec![];
        let mut stats = Statistics::new_unknown(&self.schema());
        if let Some(metadata_db) = &self.metadata_db {
            let tbl = self.table_id();
            metadata_db
                .stream_nozzle_metadata(tbl)
                .map_err(|e| DataFusionError::External(Box::new(e)))
                .try_fold(
                    (&mut files, &mut stats),
                    |(files, table_stats),
                     FileMetadata {
                         object_meta,
                         range: (range_start, range_end),
                         row_count: num_rows,
                         data_size,
                         size_hint: metadata_size_hint,
                     }| async move {
                        let block_num_stats = ColumnStatistics {
                            null_count: Precision::Exact(0),
                            max_value: Precision::Exact(range_end.into()),
                            min_value: Precision::Exact(range_start.into()),
                            ..Default::default()
                        };

                        let mut file_stats = Statistics::new_unknown(&self.schema().clone());

                        file_stats.column_statistics.insert(0, block_num_stats);
                        file_stats.num_rows = Precision::Exact(num_rows as usize);
                        file_stats.total_byte_size = Precision::Exact(data_size as usize);

                        let mut partitioned_file = PartitionedFile::from(object_meta)
                            .with_metadata_size_hint(metadata_size_hint as usize);

                        update_table_stats(table_stats, &file_stats, &[0])?;

                        partitioned_file.statistics = Some(file_stats);

                        files.push(partitioned_file);

                        Ok((files, table_stats))
                    },
                )
                .await?;
        } else {
            let mut parquet_metadata_stream = self.stream_parquet_metadata();

            while let Some((parquet_metadata, meta)) = parquet_metadata_stream.try_next().await? {
                let num_rows = parquet_metadata.file_metadata().num_rows();
                let scanned_ranges = ScannedRange::try_from((parquet_metadata, &meta))?;
                let (range_start, range_end) =
                    (scanned_ranges.range_start, scanned_ranges.range_end);

                let mut file = PartitionedFile::from(meta);

                let mut file_stats = Statistics::new_unknown(&self.schema());

                let block_num_stats = ColumnStatistics {
                    null_count: Precision::Exact(0),
                    max_value: Precision::Exact(range_end.into()),
                    min_value: Precision::Exact(range_start.into()),
                    ..Default::default()
                };

                file_stats.column_statistics.insert(0, block_num_stats);
                file_stats.num_rows = Precision::Exact(num_rows as usize);

                update_table_stats(&mut stats, &file_stats, &[0])?;

                file.statistics = Some(file_stats);
                files.push(file);
            }
        };
        let split_files = split_files(files, 10);
        Ok((split_files, stats))
    }

    fn stream_object_meta<'a>(&'a self) -> BoxStream<'a, DataFusionResult<ObjectMeta>> {
        self.object_store
            .list(Some(&self.path))
            .map_err(|e| DataFusionError::ObjectStore(e))
            .boxed()
    }

    fn stream_object_readers<'a>(
        &'a self,
    ) -> BoxStream<'a, DataFusionResult<(ParquetObjectReader, ObjectMeta)>> {
        let meta_stream = self.stream_object_meta();
        let store = self.object_store.clone();

        meta_stream
            .map(move |meta| {
                let meta = meta?;
                let reader = ParquetObjectReader::new(store.clone(), meta.clone());
                Ok((reader, meta))
            })
            .boxed()
    }

    fn stream_parquet_metadata<'a>(
        &'a self,
    ) -> BoxStream<'a, DataFusionResult<(Arc<metadata::ParquetMetaData>, ObjectMeta)>> {
        self.stream_object_readers()
            .try_filter_map(|(mut reader, meta)| async move {
                let metadata = reader.get_metadata().await?;
                Ok(Some((metadata, meta)))
            })
            .boxed()
    }
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
