use std::{collections::BTreeMap, ops::RangeInclusive, sync::Arc};

use dashmap::DashMap;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::Session,
    common::DFSchema,
    datasource::{
        create_ordering,
        file_format::{parquet::ParquetFormat, FileFormat},
        listing::{ListingTableUrl, PartitionedFile},
        physical_plan::{FileGroup, FileScanConfigBuilder, FileSource, ParquetSource},
        TableProvider, TableType,
    },
    error::{DataFusionError, Result as DataFusionResult},
    execution::object_store::ObjectStoreUrl,
    logical_expr::{col, utils::conjunction, ScalarUDF, SortExpr, TableProviderFilterPushDown},
    physical_expr::LexOrdering,
    physical_plan::{ExecutionPlan, PhysicalExpr},
    prelude::Expr,
    sql::TableReference,
};
use futures::{stream, Stream, StreamExt, TryStreamExt};
use metadata_db::{FileId, FileMetadataRow, LocationId, MetadataDb, TableId};
use object_store::{path::Path, ObjectStore};
use tokio::sync::mpsc;
use tracing::info;
use url::Url;
use uuid::Uuid;

use super::reader::NozzleParquetFileReaderFactory;
use crate::{
    metadata::{parquet::ParquetMeta, read_metadata_bytes_from_parquet, FileMetadata},
    multirange::MultiRange,
    store::{infer_object_store, Store},
    BlockNum, BoxError, Dataset, ResolvedTable,
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

    /// Location ID in the metadata database.
    location_id: LocationId,
    /// Metadata database to use for this table.
    pub metadata_db: Arc<MetadataDb>,
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

        Ok(Self {
            table,
            url,
            path,
            location_id,
            metadata_db,
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

        let physical_table = Self {
            table: table.clone(),
            url,
            path,
            location_id,
            metadata_db,
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

        Ok(Some(Self {
            table: table.clone(),
            url,
            path,
            location_id,
            metadata_db: metadata_db.clone(),
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

        let physical_table = Self {
            table: table.clone(),
            url: url.clone(),
            path: path.clone(),
            location_id,
            metadata_db,
        };

        Ok(physical_table)
    }

    /// Truncate this table by deleting all dump files making up the table
    pub async fn truncate(&self) -> Result<(), BoxError> {
        let file_locations: Vec<Path> = self
            .stream_file_metadata()
            .map_ok(|m| m.object_meta.location.clone())
            .try_collect()
            .await?;
        let num_files = file_locations.len();
        let locations = Box::pin(stream::iter(file_locations.into_iter().map(Ok)));
        let deleted = self
            .object_store()
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
        infer_object_store(self.url())
            .map(|(object_store, _)| object_store)
            .expect("Failed to infer object store")
    }

    pub fn table(&self) -> &ResolvedTable {
        &self.table
    }

    pub async fn files(&self) -> Result<Vec<Arc<FileMetadata>>, BoxError> {
        self.stream_file_metadata().try_collect().await
    }

    /// Return the block range to use for query execution over this table. This is defined as the
    /// contiguous range of block numbers starting from the lowest start block. Ok(None) is
    /// returned if no block range has been synced.
    pub async fn synced_range(&self) -> Result<Option<RangeInclusive<BlockNum>>, BoxError> {
        let ranges = self.multi_range().await?;
        Ok(ranges.first().map(|(start, end)| start..=end))
    }

    pub async fn multi_range(&self) -> Result<MultiRange, BoxError> {
        let ranges = self
            .stream_file_metadata()
            .map(|r| {
                let file_metadata = r?;
                let file_name = file_metadata.file_name().to_string();
                let ParquetMeta { ranges, .. } =
                    ParquetMeta::try_from_file_metadata(file_metadata)?;
                if ranges.len() != 1 {
                    return Err(BoxError::from(format!(
                        "expected exactly 1 range for {file_name}"
                    )));
                }
                Ok(ranges[0].numbers.clone().into_inner())
            })
            .try_collect()
            .await?;
        MultiRange::from_ranges(ranges).map_err(Into::into)
    }
}

// Methods for streaming metadata and file information of PhysicalTable
impl PhysicalTable {
    fn stream_file_metadata<'a>(
        &'a self,
    ) -> impl Stream<Item = Result<Arc<FileMetadata>, BoxError>> + 'a {
        self.metadata_db
            .stream_file_metadata(self.location_id)
            .map(|row| {
                let row = row?;
                let file_metadata: Arc<FileMetadata> = FileMetadata::try_from(row)?.into();

                Ok(file_metadata)
            })
    }
}

// helper methods for implementing `TableProvider` trait

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

    fn object_store_url(&self) -> DataFusionResult<ObjectStoreUrl> {
        Ok(ListingTableUrl::try_new(self.url.clone(), None)?.object_store())
    }

    fn output_ordering(&self) -> DataFusionResult<Vec<LexOrdering>> {
        let schema = self.schema();
        let sort_order = self.order_exprs();
        create_ordering(&schema, &sort_order)
    }

    fn create_parquet_file_reader_factory(
        &self,
        file_id_groups: Vec<Vec<FileId>>,
    ) -> Arc<NozzleParquetFileReaderFactory> {
        let metadata_generator = DashMap::new();

        for (group_id, file_ids) in file_id_groups.into_iter().enumerate() {
            let (sender, receiver) = mpsc::channel(1);
            metadata_generator.insert(group_id, receiver);

            let sender_clone = sender.clone();
            let metadata_db_clone = self.metadata_db.clone();
            let file_ids = Arc::new(file_ids);
            let location_id = self.location_id;

            tokio::spawn(async move {
                let mut stream = metadata_db_clone.stream_file_metadata(location_id);

                while let Some(Ok(FileMetadataRow { id, metadata, .. })) = stream.next().await {
                    if file_ids.contains(&id) {
                        if let Err(e) = sender_clone.send(metadata).await {
                            if e.to_string().as_str() == "channel closed" {
                                // The receiver has been dropped, we can stop sending
                                break;
                            } else {
                                tracing::error!("Failed to send metadata: {}", e);
                            }
                        }
                    }
                }
            });
        }

        let factory = NozzleParquetFileReaderFactory {
            metadata_generator: metadata_generator.into(),
            object_store: self.object_store(),
        };

        Arc::new(factory)
    }

    fn stream_partitioned_files<'a>(
        &'a self,
        ctx: &'a dyn Session,
    ) -> impl Stream<Item = DataFusionResult<(u64, (FileId, PartitionedFile))>> + 'a {
        self.stream_file_metadata()
            .map_err(DataFusionError::from)
            .map(async move |res| {
                let file_meta = res?;
                let FileMetadata {
                    file_id,
                    ref object_meta,
                    ref url,
                    ref metadata,
                    location_id,
                    ..
                } = file_meta.as_ref();

                // let table_schema = self.schema();
                // let statistics = statistics_from_parquet_meta_calc(metadata, table_schema)?.into();
                let partitioned_file = PartitionedFile::from(object_meta.clone())
                    .with_range(0, object_meta.size as i64);

                let ParquetMeta { ranges, .. } =
                    ParquetMeta::try_from_parquet_metadata(metadata.clone(), url, *location_id)?;

                let range_start = ranges
                    .first()
                    .ok_or(DataFusionError::Execution(format!(
                        "No ranges found for file `{}` for table `{}`",
                        object_meta.location,
                        self.table_ref()
                    )))?
                    .numbers
                    .start();

                Ok((*range_start, (*file_id, partitioned_file)))
            })
            .buffer_unordered(ctx.config_options().execution.meta_fetch_concurrency)
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

    #[tracing::instrument(skip_all, err)]
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let predicate = self.filters_to_predicate(state, filters)?;
        let files = self
            .stream_partitioned_files(state)
            .try_collect::<BTreeMap<_, _>>()
            .await?;
        if files.is_empty() {
            return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                self.schema(),
            )));
        }
        let target_partitions = state.config_options().execution.target_partitions;
        let (file_groups, file_id_groups) = round_robin(files, target_partitions);

        let parquet_file_reader_factory = self.create_parquet_file_reader_factory(file_id_groups);

        let output_ordering = self.output_ordering()?;

        let file_schema = self.schema();
        let object_store_url = self.object_store_url()?;
        let file_source: Arc<dyn FileSource> = ParquetSource::default()
            .with_parquet_file_reader_factory(parquet_file_reader_factory)
            .with_predicate(predicate)
            .into();

        ParquetFormat::default()
            .create_physical_plan(
                state,
                FileScanConfigBuilder::new(
                    object_store_url,
                    file_schema,
                    ParquetSource::default().into(),
                )
                .with_file_groups(file_groups)
                .with_output_ordering(output_ordering)
                .with_projection(projection.cloned())
                .with_limit(limit)
                .with_source(file_source)
                .build(),
            )
            .await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
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

fn round_robin(
    files: BTreeMap<u64, (FileId, PartitionedFile)>,
    target_partitions: usize,
) -> (Vec<FileGroup>, Vec<Vec<FileId>>) {
    let size = files.len().min(target_partitions);
    if size <= 0 {
        return (vec![], vec![]);
    }
    let mut file_groups = vec![FileGroup::default(); size];
    let mut file_id_groups: Vec<Vec<FileId>> = vec![vec![]; size];
    for (idx, (_, (id, file))) in files.into_iter().enumerate() {
        file_groups[idx % size].push(file.with_extensions(Arc::new(idx % size)));
        file_id_groups[idx % size].push(id);
    }
    (file_groups, file_id_groups)
}
