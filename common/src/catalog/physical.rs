use std::{collections::BTreeMap, ops::Deref, sync::Arc, u64};

use datafusion::{
    arrow::{
        array::{Scalar, UInt64Array},
        datatypes::{Field, SchemaRef},
    },
    catalog::{Session, TableProvider},
    common::{project_schema, stats::Precision, Statistics},
    datasource::{
        file_format::parquet::ParquetFormat, listing::helpers::expr_applicable_for_cols, TableType,
    },
    error::{DataFusionError, Result as DataFusionResult},
    execution::{
        cache::{cache_manager::FileStatisticsCache, cache_unit::DefaultFileStatisticsCache},
        object_store::ObjectStoreUrl,
    },
    logical_expr::{col, Between, BinaryExpr, Operator, ScalarUDF, SortExpr},
    parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader},
    physical_expr::{create_ordering, LexOrdering},
    physical_plan::{empty::EmptyExec, ExecutionPlan},
    prelude::Expr,
    scalar::ScalarValue,
    sql::TableReference,
};
use datafusion_datasource::{
    compute_all_files_statistics,
    file_format::FileFormat,
    file_groups::FileGroup,
    file_scan_config::{FileScanConfig, FileScanConfigBuilder},
    schema_adapter::DefaultSchemaAdapterFactory,
    PartitionedFile,
};
use futures::{future, stream, Stream, StreamExt, TryStreamExt};
use metadata_db::{LocationId, MetadataDb, TableId};
use object_store::{path::Path, ObjectMeta, ObjectStore};
use tracing::{debug, info};
use url::Url;
use uuid::Uuid;

use crate::{
    metadata::{
        parquet::{ParquetMeta, PARQUET_METADATA_KEY},
        FileMetadata,
    },
    store::{infer_object_store, Store},
    BoxError, Dataset, ResolvedTable, BLOCK_NUM,
};

type BlockNumScalar = Scalar<UInt64Array>;
type BlockRangeScalar = (BlockNumScalar, BlockNumScalar);

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

    /// Cached statistics for the table.
    collected_statistics: FileStatisticsCache,
}

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

        let collected_statistics: FileStatisticsCache =
            Arc::new(DefaultFileStatisticsCache::default());

        Ok(Self {
            table,
            url,
            path,
            object_store,
            location_id,
            metadata_db,
            collected_statistics,
        })
    }

    /// Create a new physical table with the given dataset name, table, URL, and object store.
    /// This is used for creating a new location (revision) for a new or  existing table in
    /// the metadata database.
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

        let collected_statistics: FileStatisticsCache =
            Arc::new(DefaultFileStatisticsCache::default());

        let physical_table = Self {
            table: table.clone(),
            url,
            path,
            object_store: data_store.object_store(),
            location_id,
            metadata_db,
            collected_statistics,
        };
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

        info!(
            "Restored table `{}` from {} with id {}",
            table.table_ref(),
            url,
            location_id
        );

        Ok(Some(Self {
            table: table.clone(),
            url,
            path,
            object_store,
            location_id,
            metadata_db: metadata_db.clone(),
            collected_statistics: Arc::new(DefaultFileStatisticsCache::default()),
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
        let metadata_db: Arc<MetadataDb> = metadata_db.clone().into();
        let location_id = metadata_db
            .register_location(*table_id, data_store.bucket(), prefix, url, false)
            .await?;

        metadata_db
            .set_active_location(*table_id, url.as_str())
            .await?;

        let object_store = data_store.object_store();
        let mut file_stream = object_store.list(Some(&path));

        while let Some(object_meta) = file_stream.try_next().await? {
            let (file_name, nozzle_meta) =
                nozzle_meta_from_object_meta(&object_meta, object_store.clone()).await?;
            let parquet_meta_json = serde_json::to_value(nozzle_meta)?;
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
                    parquet_meta_json,
                )
                .await?;
        }

        let collected_statistics: FileStatisticsCache =
            Arc::new(DefaultFileStatisticsCache::default());

        let physical_table = Self {
            table: table.clone(),
            url: url.clone(),
            path: path.clone(),
            object_store,
            location_id,
            metadata_db,
            collected_statistics,
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

/// Methods for `PhysicalTable` to access its properties and metadata.
impl PhysicalTable {
    pub fn dataset(&self) -> &Dataset {
        self.table.dataset()
    }

    pub fn table_name(&self) -> &str {
        &self.table.name()
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn catalog_schema(&self) -> &str {
        // Unwrap: This is always constructed with a schema.
        &self.table.catalog_schema()
    }

    pub fn schema(&self) -> SchemaRef {
        self.table.schema().clone()
    }

    pub fn location_id(&self) -> LocationId {
        self.location_id
    }

    /// Qualified table reference in the format `dataset_name.table_name`.
    pub fn table_ref(&self) -> &TableReference {
        &self.table.table_ref()
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
            .table()
            .sorted_by()
            .iter()
            .map(|col_name| vec![col(*col_name).sort(true, false)])
            .collect()
    }

    pub fn network(&self) -> &str {
        &self.table.network()
    }

    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.object_store.clone()
    }

    pub fn table(&self) -> &ResolvedTable {
        &self.table
    }

    pub async fn file_names(&self) -> Result<Vec<String>, BoxError> {
        self.stream_file_names().try_collect().await
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

    pub async fn ranges(&self) -> Result<Vec<(u64, u64)>, BoxError> {
        self.stream_ranges().try_collect().await
    }
}

/// Streaming methods for `PhysicalTable` to fetch file metadata, ranges, and file names.
impl PhysicalTable {
    fn stream_file_metadata(&self) -> impl Stream<Item = Result<FileMetadata, BoxError>> + '_ {
        self.metadata_db
            .stream_file_metadata(self.location_id)
            .map(|row| row?.try_into())
    }

    fn stream_ranges(&self) -> impl Stream<Item = Result<(u64, u64), BoxError>> + '_ {
        self.stream_file_metadata().map_ok(
            |FileMetadata {
                 parquet_meta:
                     ParquetMeta {
                         range_start,
                         range_end,
                         ..
                     },
                 ..
             }| (range_start, range_end),
        )
    }

    fn stream_file_names(&self) -> impl Stream<Item = Result<String, BoxError>> + '_ {
        self.stream_file_metadata()
            .map_ok(|FileMetadata { file_name, .. }| file_name)
    }

    fn stream_parquet_files(
        &self,
    ) -> impl Stream<Item = Result<(String, ObjectMeta), BoxError>> + '_ {
        self.stream_file_metadata()
            .map_ok(
                move |FileMetadata {
                          object_meta,
                          file_name,
                          ..
                      }| (file_name, object_meta),
            )
            .boxed()
    }

    fn stream_partitioned_files<'a>(
        &'a self,
        ctx: &'a dyn Session,
    ) -> impl Stream<Item = Result<(BlockRangeScalar, PartitionedFile), BoxError>> + 'a {
        self.stream_file_metadata().try_filter_map(
            move |FileMetadata {
                      object_meta,
                      parquet_meta:
                          ParquetMeta {
                              range_start,
                              range_end,
                              ..
                          },
                      ..
                  }| async move {
                let part_file = PartitionedFile::from(object_meta);
                let statistics = self.do_collect_statistics(ctx, &part_file).await?;

                Ok(Some((
                    (
                        UInt64Array::new_scalar(range_start),
                        UInt64Array::new_scalar(range_end),
                    ),
                    part_file.with_statistics(statistics),
                )))
            },
        )
    }
}

#[async_trait::async_trait]
impl TableProvider for PhysicalTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(self.table.schema())
    }

    async fn scan<'table, 'state, 'projection, 'filters>(
        &'table self,
        state: &'state dyn Session,
        projection: Option<&'projection Vec<usize>>,
        filters: &'filters [Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let table_partition_cols = self.partition_cols()?;
        let table_partition_col_names = table_partition_cols
            .iter()
            .map(Field::name)
            .map(Deref::deref)
            .collect::<Vec<_>>();

        let (partition_filters, filters): &(Vec<&Expr>, Vec<&Expr>) =
            &filters.iter().partition(|filter| {
                can_be_evaluted_for_partition_pruning(&table_partition_col_names, filter)
            });

        let statistic_file_limit = if filters.is_empty() { limit } else { None };

        let (mut file_groups, statistics) = self
            .list_files_for_scan(state, partition_filters, statistic_file_limit)
            .await?;

        if file_groups.is_empty() {
            let projected_schema = project_schema(&self.schema(), projection)?;
            return Ok(Arc::new(EmptyExec::new(projected_schema)));
        }

        let output_ordering = self.create_output_ordering()?;

        match state
            .config_options()
            .execution
            .split_file_groups_by_statistics
            .then(|| {
                output_ordering.first().map(|output_ordering| {
                    FileScanConfig::split_groups_by_statistics_with_target_partitions(
                        &self.schema(),
                        &file_groups,
                        output_ordering,
                        1,
                    )
                })
            })
            .flatten()
        {
            Some(Err(e)) => debug!("failed to split file groups by statistics: {e}"),
            Some(Ok(new_groups)) => {
                if new_groups.len() <= 1 {
                    file_groups = new_groups;
                } else {
                    debug!("attempted to split file groups by statistics, but there were more file groups than target_partitions; falling back to unordered")
                }
            }
            None => {} // no ordering required
        };

        ParquetFormat::default()
            .with_enable_pruning(true)
            .with_force_view_types(true)
            .create_physical_plan(
                state,
                FileScanConfigBuilder::new(
                    self.object_store_url()?,
                    self.schema(),
                    ParquetFormat::default().file_source(),
                )
                .with_file_groups(file_groups)
                .with_statistics(statistics)
                .with_projection(projection.cloned())
                .with_limit(limit)
                .with_output_ordering(output_ordering)
                .build(),
            )
            .await
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }
}

/// Helper methods for `PhysicalTable` to implement the `TableProvider` trait.
impl PhysicalTable {
    fn object_store_url(&self) -> DataFusionResult<ObjectStoreUrl> {
        let url = self.url().as_str();
        let url_start = url.find("://").ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Invalid URL: {}. Expected format: <scheme>://<path>",
                self.url()
            ))
        })?;
        let scheme = url[..url_start].to_string() + "://";
        ObjectStoreUrl::parse(scheme)
    }

    fn create_output_ordering(&self) -> DataFusionResult<Vec<LexOrdering>> {
        let schema = self.schema();
        let sort_order = self.order_exprs();
        create_ordering(&schema, &sort_order)
    }

    async fn do_collect_statistics(
        &self,
        ctx: &dyn Session,
        part_file: &PartitionedFile,
    ) -> DataFusionResult<Arc<Statistics>> {
        match self
            .collected_statistics
            .get_with_extra(&part_file.object_meta.location, &part_file.object_meta)
        {
            Some(statistics) => Ok(statistics),
            None => {
                let statistics = ParquetFormat::default()
                    .infer_stats(
                        ctx,
                        &self.object_store,
                        Arc::clone(&self.schema()),
                        &part_file.object_meta,
                    )
                    .await?;
                let statistics = Arc::new(statistics);
                self.collected_statistics.put_with_extra(
                    &part_file.object_meta.location,
                    Arc::clone(&statistics),
                    &part_file.object_meta,
                );
                Ok(statistics)
            }
        }
    }

    fn pruned_partition_stream<'a>(
        &'a self,
        ctx: &'a dyn Session,
        filters: &'a [&'a Expr],
    ) -> impl Stream<Item = DataFusionResult<PartitionedFile>> + 'a {
        let (filter_min, filter_max) = get_min_max_from_filters(filters);

        self.stream_partitioned_files(ctx)
            .try_filter_map(move |((range_start, range_end), partitioned_file)| {
                future::ok(
                    (filter_min
                        .clone()
                        .into_inner()
                        .value(0)
                        .le(&range_start.into_inner().value(0))
                        && filter_max
                            .clone()
                            .into_inner()
                            .value(0)
                            .ge(&range_end.into_inner().value(0)))
                    .then_some(partitioned_file),
                )
            })
            .map_err(DataFusionError::from)
    }

    fn partition_cols(&self) -> DataFusionResult<Vec<Field>> {
        let schema = self.schema();
        self.table
            .table()
            .sorted_by()
            .iter()
            .filter(|name| **name == BLOCK_NUM)
            .map(|name| Ok(schema.field_with_name(name)?.clone()))
            .collect()
    }

    async fn list_files_for_scan<'a>(
        &'a self,
        ctx: &'a dyn Session,
        filters: &'a [&'a Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<(Vec<FileGroup>, Statistics)> {
        let files = self.pruned_partition_stream(ctx, filters);
        let (file_group, inexact_stats) = get_files_with_limit(files, limit).await?;
        let file_groups = vec![file_group];
        let (mut file_groups, mut stats) =
            compute_all_files_statistics(file_groups, self.schema(), true, inexact_stats)?;
        let (schema_mapper, _) =
            DefaultSchemaAdapterFactory::from_schema(self.schema()).map_schema(&self.schema())?;
        stats.column_statistics = schema_mapper.map_column_statistics(&stats.column_statistics)?;
        file_groups.iter_mut().try_for_each(|file_group| {
            if let Some(stat) = file_group.statistics_mut() {
                stat.column_statistics =
                    schema_mapper.map_column_statistics(&stat.column_statistics)?;
            }
            Ok::<_, DataFusionError>(())
        })?;
        Ok((file_groups, stats))
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

async fn nozzle_meta_from_object_meta(
    object_meta: &ObjectMeta,
    object_store: Arc<dyn ObjectStore>,
) -> Result<(String, ParquetMeta), BoxError> {
    let mut reader = ParquetObjectReader::new(object_store.clone(), object_meta.location.clone())
        .with_file_size(object_meta.size);
    let parquet_metadata = reader.get_metadata(None).await?;
    let file_metadata = parquet_metadata.file_metadata();
    let key_value_metadata =
        file_metadata
            .key_value_metadata()
            .ok_or(crate::ArrowError::ParquetError(format!(
                "Unable to fetch Key Value metadata for file {}",
                &object_meta.location
            )))?;
    let parquet_meta_key_value_pair = key_value_metadata
        .into_iter()
        .find(|key_value| key_value.key.as_str() == PARQUET_METADATA_KEY)
        .ok_or(crate::ArrowError::ParquetError(format!(
            "Missing key: {} in file metadata for file {}",
            PARQUET_METADATA_KEY, &object_meta.location
        )))?;
    let parquet_meta_json =
        parquet_meta_key_value_pair
            .value
            .as_ref()
            .ok_or(crate::ArrowError::ParquetError(format!(
                "Unable to parse ParquetMeta from empty value in metadata for file {}",
                &object_meta.location
            )))?;
    let parquet_meta: ParquetMeta = serde_json::from_str(parquet_meta_json).map_err(|e| {
        crate::ArrowError::ParseError(format!(
            "Unable to parse ParquetMeta from key value metadata for file {}: {}",
            &object_meta.location, e
        ))
    })?;
    // Unwrap: We know this is a path with valid file name because we just opened it
    let file_name = object_meta.location.filename().unwrap().to_string();
    Ok((file_name, parquet_meta))
}

fn can_be_evaluted_for_partition_pruning(partition_column_names: &[&str], expr: &Expr) -> bool {
    !partition_column_names.is_empty() && expr_applicable_for_cols(partition_column_names, expr)
}

/// Note: Converts gt/lt values to gte/lte equivalents for block number filters.
///
/// # DO NOT PUSH THIS TO MAIN
/// ## THIS IS NOT A COMPLETE IMPLEMENTATION. WE WANT SOMETHING THAT CAN BE USED FOR ALL FILTERS, NOT JUST BLOCK NUMBERS COMPARING WITH LITERALS.
fn get_min_max_from_filters(filters: &[&Expr]) -> BlockRangeScalar {
    let mut min = None;
    let mut max = None;

    for filter in filters {
        if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = filter {
            if let (Expr::Column(col), Expr::Literal(ScalarValue::UInt64(lit))) =
                (left.as_ref(), right.as_ref())
            {
                if col.name() == BLOCK_NUM {
                    if let Some(value) = lit {
                        match op {
                            Operator::Eq => {
                                min = Some(*value);
                                max = Some(*value);
                            }
                            Operator::GtEq => {
                                min = Some(*value);
                            }
                            Operator::LtEq => {
                                max = Some(*value);
                            }
                            Operator::Gt => {
                                min = Some(value + 1);
                            }
                            Operator::Lt => {
                                max = Some(value - 1);
                            }
                            Operator::NotEq => {
                                min = Some(value + 1);
                                max = Some(value - 1);
                            }
                            _ => {}
                        }
                    }
                }
            }
        } else if let Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) = filter
        {
            if let Expr::Column(col) = expr.as_ref() {
                if col.name() == BLOCK_NUM {
                    if let (
                        Expr::Literal(ScalarValue::UInt64(low_lit)),
                        Expr::Literal(ScalarValue::UInt64(high_lit)),
                    ) = (low.as_ref(), high.as_ref())
                    {
                        if let (Some(low_value), Some(high_value)) = (low_lit, high_lit) {
                            if *negated {
                                min = Some(*high_value);
                                max = Some(*low_value);
                            } else {
                                min = Some(*low_value);
                                max = Some(*high_value);
                            }
                        }
                    }
                }
            }
        }
    }

    (
        UInt64Array::new_scalar(min.unwrap_or(u64::MIN)),
        UInt64Array::new_scalar(max.unwrap_or(u64::MAX)),
    )
}

async fn get_files_with_limit(
    files: impl Stream<Item = DataFusionResult<PartitionedFile>>,
    limit: Option<usize>,
) -> DataFusionResult<(FileGroup, bool)> {
    let mut file_group = FileGroup::default();
    // Fusing the stream allows us to call next safely even once it is finished.
    let mut all_files = Box::pin(files.fuse());
    enum ProcessingState {
        ReadingFiles,
        ReachedLimit,
    }

    let mut state = ProcessingState::ReadingFiles;
    let mut num_rows = Precision::Absent;

    while let Some(file_result) = all_files.next().await {
        // Early exit if we've already reached our limit
        if matches!(state, ProcessingState::ReachedLimit) {
            break;
        }

        let file = file_result?;

        if let Some(file_stats) = &file.statistics {
            num_rows = if file_group.is_empty() {
                // For the first file, just take its row count
                file_stats.num_rows
            } else {
                // For subsequent files, accumulate the counts
                num_rows.add(&file_stats.num_rows)
            };
        }

        // Always add the file to our group
        file_group.push(file);

        // Check if we've hit the limit (if one was specified)
        if let Some(limit) = limit {
            if let Precision::Exact(row_count) = num_rows {
                if row_count > limit {
                    state = ProcessingState::ReachedLimit;
                }
            }
        }
    }
    // If we still have files in the stream, it means that the limit kicked
    // in, and the statistic could have been different had we processed the
    // files in a different order.
    let inexact_stats = all_files.next().await.is_some();
    Ok((file_group, inexact_stats))
}
