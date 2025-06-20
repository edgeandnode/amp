use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{memory::DataSourceExec, Session},
    common::{stats::Precision, ColumnStatistics, DFSchema, Statistics},
    datasource::{
        create_ordering,
        listing::{FileRange, ListingTableUrl, PartitionedFile},
        physical_plan::{
            parquet::ParquetAccessPlan, FileGroup, FileScanConfigBuilder, ParquetSource,
        },
        TableProvider, TableType,
    },
    error::{DataFusionError, Result as DataFusionResult},
    execution::{cache::CacheAccessor, object_store::ObjectStoreUrl},
    logical_expr::{col, utils::conjunction, ScalarUDF, SortExpr, TableProviderFilterPushDown},
    parquet::{
        data_type::{ByteArray, FixedLenByteArray, Int96},
        file::{metadata::ColumnChunkMetaData, statistics::Statistics as FileStatistics},
    },
    physical_expr::{
        utils::{Guarantee, LiteralGuarantee},
        LexOrdering,
    },
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::{ExecutionPlan, PhysicalExpr},
    prelude::Expr,
    scalar::ScalarValue,
    sql::TableReference,
};
use futures::{stream, FutureExt, Stream, StreamExt, TryStreamExt};
use metadata_db::{LocationId, MetadataDb, TableId};
use object_store::{path::Path, ObjectMeta, ObjectStore};
use tracing::info;
use url::Url;
use uuid::Uuid;

use super::statistics::{get_min_block_num, RowGroupId};
use crate::{
    catalog::statistics::{
        determine_pruning, update_statistics, PruningGuarantees, RowGroupStatisticsCache,
    },
    metadata::{
        parquet::ParquetMeta, range::BlockRange, read_metadata_bytes_from_parquet, FileMetadata,
        MetadataHash,
    },
    multirange::MultiRange,
    store::{infer_object_store, Store},
    BoxError, BoxResult, Dataset, ResolvedTable,
};

#[derive(Debug, Clone)]
pub struct Catalog {
    tables: Vec<Arc<PhysicalTable>>,
    /// User-defined functions (UDFs) specific to this catalog.
    udfs: Vec<ScalarUDF>,
}

impl Catalog {
    pub fn empty() -> Self {
        Catalog {
            tables: vec![],
            udfs: vec![],
        }
    }

    pub fn new(tables: Vec<Arc<PhysicalTable>>, udfs: Vec<ScalarUDF>) -> Self {
        Catalog { tables, udfs }
    }

    pub fn add_table(&mut self, table: Arc<PhysicalTable>) {
        self.tables.push(table);
    }

    pub fn add_udf(&mut self, udf: ScalarUDF) {
        self.udfs.push(udf);
    }

    pub fn tables(&self) -> &[Arc<PhysicalTable>] {
        &self.tables
    }

    pub fn udfs(&self) -> &[ScalarUDF] {
        &self.udfs
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalTable {
    /// Logical table representation.
    table: ResolvedTable,

    /// Absolute URL to the data location, path section of the URL and the corresponding object store.
    url: Url,
    /// Path to the data location in the object store.
    path: Path,
    /// Object store to use for this table.
    object_store: Arc<dyn ObjectStore>,

    /// Location ID in the metadata database.
    location_id: LocationId,
    /// Metadata database to use for this table.
    pub metadata_db: Arc<MetadataDb>,

    /// Statistics Cache
    statistics_cache: Arc<RowGroupStatisticsCache>,
}

// Methods for creating and managing PhysicalTable instances
impl PhysicalTable {
    /// Create a new physical table with the given dataset name, table, URL, and object store.
    pub fn new(
        table: ResolvedTable,
        url: Url,
        location_id: LocationId,
        metadata_db: Arc<MetadataDb>,
    ) -> Result<Self, BoxError> {
        let path = Path::from_url_path(url.path()).unwrap();
        let (object_store, _) = infer_object_store(&url)?;
        let statistics_cache = Arc::new(RowGroupStatisticsCache::default());

        Ok(Self {
            table,
            url,
            path,
            object_store,
            location_id,
            metadata_db,
            statistics_cache,
        })
    }

    /// Create a new physical table with the given dataset name, table, URL, and object store.
    /// This is used for creating a new location (revision) for a new or  existing table in
    /// the metadata database.
    #[tracing::instrument(skip_all, fields(table = %table, active = %set_active), err)]
    pub async fn next_revision(
        table: &ResolvedTable,
        data_store: &Store,
        metadata_db: Arc<MetadataDb>,
        set_active: bool,
    ) -> Result<Self, BoxError> {
        let dataset_name = &table.dataset().name;
        let table_id = TableId {
            dataset: dataset_name,
            dataset_version: None,
            table: &table.name(),
        };

        let path = make_location_path(dataset_name, &table.name());
        let url = data_store.url().join(&path)?;
        let location_id = metadata_db
            .register_location(table_id, data_store.bucket(), &path, &url, false)
            .await?;

        if set_active {
            metadata_db
                .set_active_location(table_id, url.as_str())
                .await?;
        }

        let path = Path::from_url_path(url.path()).unwrap();
        let statistics_cache = Arc::new(RowGroupStatisticsCache::default());
        let physical_table = Self {
            table: table.clone(),
            url,
            path,
            object_store: data_store.object_store(),
            location_id,
            metadata_db,
            statistics_cache,
        };

        info!("Created new revision at {}", physical_table.path);

        Ok(physical_table)
    }

    /// Attempts to restore the latest revision of a table from the data store.
    /// If the table is not found, it returns `None`.
    pub async fn restore_latest_revision(
        table: &ResolvedTable,
        data_store: Arc<Store>,
        metadata_db: Arc<MetadataDb>,
    ) -> Result<Option<Self>, BoxError> {
        let dataset_name = &table.dataset().name;
        let table_id = TableId {
            dataset: &table.dataset().name,
            dataset_version: None,
            table: &table.name(),
        };

        let prefix = format!("{}/{}/", &dataset_name, table.name());
        let url = data_store.url().join(&prefix)?;
        let path = Path::from_url_path(url.path()).unwrap();
        let revisions = list_revisions(&data_store, &prefix, &path).await?;
        Self::restore_latest(
            revisions,
            table,
            &table_id,
            data_store.clone(),
            metadata_db.clone(),
        )
        .await
    }

    /// Attempt to get the active revision of a table.
    pub async fn get_active(
        table: &ResolvedTable,
        metadata_db: Arc<MetadataDb>,
    ) -> Result<Option<Self>, BoxError> {
        let dataset_name = &table.dataset().name;
        let table_id = TableId {
            dataset: dataset_name,
            dataset_version: None,
            table: &table.name(),
        };

        let Some((url, location_id)) = metadata_db.get_active_location(table_id).await? else {
            return Ok(None);
        };

        let path = Path::from_url_path(url.path()).unwrap();
        let (object_store, _) = infer_object_store(&url)?;
        let statistics_cache = Arc::new(RowGroupStatisticsCache::default());

        Ok(Some(Self {
            table: table.clone(),
            url,
            path,
            object_store,
            location_id,
            metadata_db: metadata_db.clone(),
            statistics_cache,
        }))
    }

    /// Attempt to restore the latest revision of a table from a provided map of revisions
    /// and register it in the metadata database.
    /// If no revisions are found, it returns `None`.
    ///
    /// Revisions are expected to be sorted in ascending order by their revision uuid.
    async fn restore_latest(
        revisions: BTreeMap<String, (Path, Url, String)>,
        table: &ResolvedTable,
        table_id: &TableId<'_>,
        data_store: Arc<Store>,
        metadata_db: Arc<MetadataDb>,
    ) -> Result<Option<Self>, BoxError> {
        if let Some((path, url, prefix)) = revisions.values().last() {
            Self::restore(table, table_id, prefix, path, url, data_store, metadata_db)
                .await
                .map(Some)
        } else {
            Ok(None)
        }
    }

    /// Restore a location from the data store and register it in the metadata database.
    async fn restore(
        table: &ResolvedTable,
        table_id: &TableId<'_>,
        prefix: &str,
        path: &Path,
        url: &Url,
        data_store: Arc<Store>,
        metadata_db: Arc<MetadataDb>,
    ) -> Result<Self, BoxError> {
        let location_id = metadata_db
            .register_location(*table_id, data_store.bucket(), prefix, url, false)
            .await?;

        metadata_db
            .set_active_location(*table_id, url.as_str())
            .await?;

        let object_store = data_store.object_store();
        let mut file_stream = object_store.list(Some(&path));

        while let Some(object_meta) = file_stream.try_next().await? {
            let (file_name, metadata) =
                read_metadata_bytes_from_parquet(&object_meta, object_store.clone()).await?;
            let object_size = object_meta.size;
            let object_e_tag = object_meta.e_tag;
            let object_version = object_meta.version;

            metadata_db
                .insert_metadata(
                    location_id,
                    file_name,
                    object_size,
                    object_e_tag,
                    object_version,
                    metadata,
                )
                .await?;
        }
        let statistics_cache = Arc::new(RowGroupStatisticsCache::default());

        let physical_table = Self {
            table: table.clone(),
            url: url.clone(),
            path: path.clone(),
            object_store,
            location_id,
            metadata_db,
            statistics_cache,
        };

        Ok(physical_table)
    }

    /// Truncate this table by deleting all dump files making up the table
    pub async fn truncate(&self) -> Result<(), BoxError> {
        let files = self.parquet_files().await?;
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

// Methods for accessing properties of PhysicalTable
impl PhysicalTable {
    pub fn dataset(&self) -> &Dataset {
        self.table.dataset()
    }

    pub fn table_name(&self) -> &str {
        self.table.name()
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    pub fn path(&self) -> &Path {
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

    pub fn table_id(&self) -> TableId<'_> {
        TableId {
            dataset: self.catalog_schema(),
            dataset_version: None,
            table: self.table_name(),
        }
    }

    pub fn order_exprs(&self) -> Vec<Vec<SortExpr>> {
        let sorted_by = self.table().table().sorted_by();
        self.schema()
            .fields()
            .iter()
            .filter_map(move |field| {
                sorted_by
                    .iter()
                    .find(|name| *name == field.name())
                    .map(|name| vec![SortExpr::new(col(*name), true, false)])
            })
            .collect()
    }

    pub fn network(&self) -> &str {
        self.table.network()
    }

    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.object_store.clone()
    }

    pub fn table(&self) -> &ResolvedTable {
        &self.table
    }

    pub async fn file_names(&self) -> Result<Vec<String>, BoxError> {
        let file_names = self.stream_file_names().try_collect().await?;
        Ok(file_names)
    }

    /// Return all parquet files for this table. If `dump_only` is `true`,
    /// only files of the form `<number>.parquet` will be returned. The
    /// result is a map from filename to object metadata.
    pub async fn parquet_files(&self) -> Result<BTreeMap<String, ObjectMeta>, BoxError> {
        let parquet_files = self
            .stream_parquet_files()
            .try_collect::<BTreeMap<String, ObjectMeta>>()
            .await?;
        Ok(parquet_files)
    }

    pub async fn ranges(&self) -> Result<Vec<BlockRange>, BoxError> {
        let ranges = self.stream_ranges().try_collect().await?;
        Ok(ranges)
    }

    pub async fn multi_range(&self) -> Result<MultiRange, BoxError> {
        let ranges = self
            .stream_ranges()
            .map(|r| r.map(|r| r.numbers.clone().into_inner()))
            .try_collect()
            .await?;
        MultiRange::from_ranges(ranges).map_err(Into::into)
    }
}

// Methods for streaming metadata and file information of PhysicalTable
impl PhysicalTable {
    pub fn stream_file_metadata<'a>(
        &'a self,
    ) -> impl Stream<Item = Result<FileMetadata, BoxError>> + 'a {
        self.metadata_db
            .stream_file_metadata(self.location_id)
            .map(|row| row?.try_into())
    }

    pub fn stream_ranges<'a>(&'a self) -> impl Stream<Item = Result<BlockRange, BoxError>> + 'a {
        self.stream_file_metadata().map(|r| {
            let FileMetadata {
                file_name,
                parquet_meta: ParquetMeta { mut ranges, .. },
                ..
            } = r?;
            if ranges.len() != 1 {
                return Err(BoxError::from(format!(
                    "expected exactly 1 range for {file_name}"
                )));
            }
            Ok(ranges.remove(0))
        })
    }

    pub fn stream_file_names<'a>(&'a self) -> impl Stream<Item = Result<String, BoxError>> + 'a {
        self.stream_file_metadata()
            .map_ok(|FileMetadata { file_name, .. }| file_name)
    }

    pub fn stream_parquet_files<'a>(
        &'a self,
    ) -> impl Stream<Item = Result<(String, ObjectMeta), BoxError>> + 'a {
        self.stream_file_metadata().map_ok(
            |FileMetadata {
                 object_meta,
                 file_name,
                 ..
             }| (file_name, object_meta),
        )
    }
}

// helper methods for managing statistics and pruning
impl PhysicalTable {
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

    fn process_column_chunks(
        &self,
        column_chunks: Vec<ColumnChunkMetaData>,
    ) -> DataFusionResult<Arc<Statistics>> {
        let mut column_statistics = vec![ColumnStatistics::default(); column_chunks.len()];
        let schema = self.schema();
        let mut num_rows = Precision::Absent;
        for chunk in column_chunks {
            if let Some(chunk_statistics) = chunk.statistics() {
                let name = chunk.column_descr().name();
                let idx = schema.index_of(name)?;
                let column = &mut column_statistics[idx];
                match chunk_statistics {
                    FileStatistics::Boolean(inner) => {
                        let distinct_count_opt = inner.distinct_count();
                        let null_count_opt = inner.null_count_opt();
                        let max_opt = inner.max_opt();
                        let min_opt = inner.min_opt();
                        let max_is_exact = inner.max_is_exact();
                        let min_is_exact = inner.min_is_exact();
                        let f = |v: &bool| ScalarValue::Boolean(Some(*v));
                        update_statistics(
                            column,
                            distinct_count_opt,
                            null_count_opt,
                            max_opt,
                            min_opt,
                            max_is_exact,
                            min_is_exact,
                            f,
                        );
                    }
                    FileStatistics::ByteArray(inner) => {
                        let distinct_count_opt = inner.distinct_count();
                        let null_count_opt = inner.null_count_opt();
                        let max_opt = inner.max_opt();
                        let min_opt = inner.min_opt();
                        let max_is_exact = inner.max_is_exact();
                        let min_is_exact = inner.min_is_exact();
                        let f = |v: &ByteArray| ScalarValue::Binary(Some(v.data().to_vec()));
                        update_statistics(
                            column,
                            distinct_count_opt,
                            null_count_opt,
                            max_opt,
                            min_opt,
                            max_is_exact,
                            min_is_exact,
                            f,
                        );
                    }
                    FileStatistics::Double(inner) => {
                        let distinct_count_opt = inner.distinct_count();
                        let null_count_opt = inner.null_count_opt();
                        let max_opt = inner.max_opt();
                        let min_opt = inner.min_opt();
                        let max_is_exact = inner.max_is_exact();
                        let min_is_exact = inner.min_is_exact();
                        let f = |v: &f64| ScalarValue::Float64(Some(*v));
                        update_statistics(
                            column,
                            distinct_count_opt,
                            null_count_opt,
                            max_opt,
                            min_opt,
                            max_is_exact,
                            min_is_exact,
                            f,
                        );
                    }
                    FileStatistics::FixedLenByteArray(inner) => {
                        let distinct_count_opt = inner.distinct_count();
                        let null_count_opt = inner.null_count_opt();
                        let max_opt = inner.max_opt();
                        let min_opt = inner.min_opt();
                        let max_is_exact = inner.max_is_exact();
                        let min_is_exact = inner.min_is_exact();
                        let f = |v: &FixedLenByteArray| {
                            ScalarValue::FixedSizeBinary(
                                chunk.column_descr().type_length(),
                                Some(v.data().to_vec()),
                            )
                        };
                        update_statistics(
                            column,
                            distinct_count_opt,
                            null_count_opt,
                            max_opt,
                            min_opt,
                            max_is_exact,
                            min_is_exact,
                            f,
                        );
                    }
                    FileStatistics::Float(inner) => {
                        let distinct_count_opt = inner.distinct_count();
                        let null_count_opt = inner.null_count_opt();
                        let max_opt = inner.max_opt();
                        let min_opt = inner.min_opt();
                        let max_is_exact = inner.max_is_exact();
                        let min_is_exact = inner.min_is_exact();
                        let f = |v: &f32| ScalarValue::Float32(Some(*v));
                        update_statistics(
                            column,
                            distinct_count_opt,
                            null_count_opt,
                            max_opt,
                            min_opt,
                            max_is_exact,
                            min_is_exact,
                            f,
                        );
                    }
                    FileStatistics::Int32(inner) => {
                        let distinct_count_opt = inner.distinct_count();
                        let null_count_opt = inner.null_count_opt();
                        let max_opt = inner.max_opt();
                        let min_opt = inner.min_opt();
                        let max_is_exact = inner.max_is_exact();
                        let min_is_exact = inner.min_is_exact();
                        let f = |v: &i32| ScalarValue::Int32(Some(*v));
                        update_statistics(
                            column,
                            distinct_count_opt,
                            null_count_opt,
                            max_opt,
                            min_opt,
                            max_is_exact,
                            min_is_exact,
                            f,
                        );
                    }
                    FileStatistics::Int64(inner) => {
                        let distinct_count_opt = inner.distinct_count();
                        let null_count_opt = inner.null_count_opt();
                        let max_opt = inner.max_opt();
                        let min_opt = inner.min_opt();
                        let max_is_exact = inner.max_is_exact();
                        let min_is_exact = inner.min_is_exact();
                        let f = |v: &i64| ScalarValue::Int64(Some(*v));
                        update_statistics(
                            column,
                            distinct_count_opt,
                            null_count_opt,
                            max_opt,
                            min_opt,
                            max_is_exact,
                            min_is_exact,
                            f,
                        );
                    }
                    FileStatistics::Int96(inner) => {
                        let distinct_count_opt = inner.distinct_count();
                        let null_count_opt = inner.null_count_opt();
                        let max_opt = inner.max_opt();
                        let min_opt = inner.min_opt();
                        let max_is_exact = inner.max_is_exact();
                        let min_is_exact = inner.min_is_exact();
                        let f = |v: &Int96| {
                            ScalarValue::TimestampNanosecond(
                                Some(v.to_nanos()),
                                Some(Arc::from("UTC")),
                            )
                        };
                        update_statistics(
                            column,
                            distinct_count_opt,
                            null_count_opt,
                            max_opt,
                            min_opt,
                            max_is_exact,
                            min_is_exact,
                            f,
                        );
                    }
                }
                num_rows = Precision::Exact(chunk.num_values() as usize).max(&num_rows);
            }
        }
        let statsistics = Statistics {
            column_statistics,
            num_rows,
            ..Default::default()
        };
        Ok(statsistics.into())
    }

    fn prune(
        &self,
        guarantees: Arc<PruningGuarantees>,
        statistics: &Statistics,
    ) -> ParquetAccessPlan {
        let prune = false;
        let schema = self.schema();

        determine_pruning(
            Guarantee::In,
            guarantees.clone(),
            statistics,
            &schema,
            prune,
        );
        determine_pruning(Guarantee::NotIn, guarantees, statistics, &schema, prune);

        match prune {
            true => ParquetAccessPlan::new_none(1),
            false => ParquetAccessPlan::new_all(1),
        }
    }

    fn write_literal_values(
        &self,
        guarantees: &[LiteralGuarantee],
        literal_values: &mut PruningGuarantees,
    ) {
        let sorted_non_null_columns = self.table().table().sorted_by();

        for (guarantee, literals, column_name) in guarantees.into_iter().filter_map(|g| {
            sorted_non_null_columns
                .contains(&g.column.name())
                .then_some::<(_, _, Arc<str>)>((
                    g.guarantee,
                    g.literals
                        .iter()
                        .cloned()
                        .map(Arc::new)
                        .collect::<HashSet<_>>(),
                    g.column.name().into(),
                ))
        }) {
            literal_values
                .entry(guarantee)
                .and_modify(|map: &mut HashMap<Arc<str>, HashSet<Arc<ScalarValue>>>| {
                    map.entry(column_name.clone())
                        .and_modify(|column_literals: &mut HashSet<Arc<ScalarValue>>| {
                            column_literals.extend(literals.clone())
                        })
                        .or_insert(literals.clone());
                })
                .or_insert_with(|| {
                    let mut map = HashMap::new();
                    map.insert(column_name.clone(), literals.clone());
                    map
                });
        }
    }
}

// helper methods for implementing `TableProvider` trait
impl PhysicalTable {
    async fn apply_statistics(
        &self,
        part_file: PartitionedFile,
        column_chunks: Vec<ColumnChunkMetaData>,
        predicate: Arc<PruningGuarantees>,
        row_group_id: RowGroupId,
        metadata_hash: MetadataHash,
    ) -> BoxResult<(u64, PartitionedFile)> {
        let statistics = match self
            .statistics_cache
            .get_with_extra(&row_group_id, &metadata_hash)
        {
            Some(statistics) => statistics,
            None => {
                let statistics = self.process_column_chunks(column_chunks)?;
                self.statistics_cache.put_with_extra(
                    &row_group_id,
                    statistics.clone(),
                    &metadata_hash,
                );
                statistics
            }
        };

        let min_block_num = get_min_block_num(&statistics, &self.schema(), self.table_name())?;
        let access_plan = self.prune(predicate, &statistics);

        Ok((
            min_block_num,
            part_file
                .with_statistics(statistics)
                .with_extensions(Arc::new(access_plan)),
        ))
    }

    fn object_store_url(&self) -> DataFusionResult<ObjectStoreUrl> {
        Ok(ListingTableUrl::try_new(self.url.clone(), None)?.object_store())
    }

    fn output_ordering(&self) -> DataFusionResult<Vec<LexOrdering>> {
        let schema = self.schema();
        let sort_order = self.order_exprs();
        create_ordering(&schema, &sort_order)
    }

    fn stream_partitioned_files<'a>(
        &'a self,
        ctx: &'a dyn Session,
        predicate: Arc<PruningGuarantees>,
    ) -> impl Stream<Item = BoxResult<(u64, PartitionedFile)>> + 'a {
        self.stream_file_metadata()
            .map_ok(
                move |FileMetadata {
                          object_meta,
                          statistics,
                          metadata_hash,
                          ..
                      }| {
                    let predicate = predicate.clone();
                    stream::iter(statistics.into_iter())
                        .map(move |(row_group_id, row_group)| {
                            let column_chunks = row_group.columns().to_vec();
                            let start = row_group.file_offset().ok_or(
                                DataFusionError::Internal(format!(
                                    "File offset not found for row group in file {}",
                                    object_meta.location
                                )),
                            )?;
                            let end = start + row_group.total_byte_size();
                            let range = Some(FileRange { start, end });
                            let part_file = PartitionedFile {
                                object_meta: object_meta.clone(),
                                range,
                                partition_values: Default::default(),
                                statistics: Default::default(),
                                extensions: Default::default(),
                                metadata_size_hint: None,
                            };

                            Ok::<_, BoxError>(
                                self.apply_statistics(
                                    part_file,
                                    column_chunks,
                                    predicate.clone(),
                                    row_group_id,
                                    metadata_hash.clone(),
                                )
                                .into_stream()
                                .boxed(),
                            )
                        })
                        .try_flatten()
                },
            )
            .try_flatten_unordered(ctx.config_options().execution.target_partitions)
    }
}

#[async_trait::async_trait]
impl TableProvider for PhysicalTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn schema(&self) -> SchemaRef {
        self.schema()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let predicate = filters
            .is_empty()
            .then_some(datafusion::physical_expr::expressions::lit(true))
            .unwrap_or(self.filters_to_predicate(state, filters)?);

        let pruning_predicate =
            Arc::new(PruningPredicate::try_new(predicate.clone(), self.schema())?);

        let mut literal_values = Arc::new(HashMap::new());
        self.write_literal_values(
            pruning_predicate.literal_guarantees(),
            Arc::make_mut(&mut literal_values),
        );

        let files = self
            .stream_partitioned_files(state, literal_values)
            .try_collect::<BTreeMap<_, _>>()
            .await?;

        if files.is_empty() {
            return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                self.schema(),
            )));
        }

        let target_partitions = state.config_options().execution.target_partitions;
        let file_groups = round_robin(files, target_partitions);

        let output_ordering = self.output_ordering()?;

        let file_schema = self.schema();
        let object_store_url = self.object_store_url()?;
        let file_source = ParquetSource::default()
            .with_predicate(predicate)
            .with_pushdown_filters(true)
            .into();

        let file_scan_config =
            FileScanConfigBuilder::new(object_store_url, file_schema, file_source)
                .with_file_groups(file_groups)
                .with_limit(limit)
                .with_output_ordering(output_ordering)
                .with_projection(projection.cloned())
                .build();

        Ok(DataSourceExec::from_data_source(file_scan_config))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // Inexact because the pruning can't handle all expressions and pruning
        // is not done at the row level -- there may be rows in returned files
        // that do not pass the filter
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

// The path format is: `<dataset>/<table>/<UUIDv7>/`
pub fn make_location_path(dataset: &str, table: &str) -> String {
    let mut path = String::new();

    // Add dataset
    path.push_str(dataset);
    path.push('/');

    // Add table
    path.push_str(table);
    path.push('/');

    // Add UUIDv7
    let uuid = uuid::Uuid::now_v7();
    path.push_str(&uuid.to_string());
    path.push('/');

    path
}

pub async fn list_revisions(
    store: &Store,
    prefix: &str,
    path: &Path,
) -> Result<BTreeMap<String, (Path, Url, String)>, BoxError> {
    let object_store = store.object_store();
    Ok(object_store
        .list_with_delimiter(Some(path))
        .await?
        .common_prefixes
        .into_iter()
        .filter_map(|path| {
            let revision = Uuid::parse_str(path.parts().last()?.as_ref())
                .as_ref()
                .map(Uuid::to_string)
                .ok()?;
            let full_prefix = format!("{prefix}{revision}/");
            let full_url = store.url().join(&full_prefix).ok()?;
            let full_path = Path::from_url_path(full_url.path()).ok()?;
            Some((revision, (full_path, full_url, full_prefix)))
        })
        .collect())
}

fn round_robin(files: BTreeMap<u64, PartitionedFile>, target_partitions: usize) -> Vec<FileGroup> {
    let size = files.len().min(target_partitions);
    if size <= 0 {
        return vec![];
    }
    let mut groups = vec![FileGroup::default(); size];
    for (idx, (_, file)) in files.into_iter().enumerate() {
        groups[idx % size].push(file);
    }
    groups
}
