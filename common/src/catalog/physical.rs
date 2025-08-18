use std::{collections::BTreeMap, ops::RangeInclusive, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{Session, memory::DataSourceExec},
    common::DFSchema,
    datasource::{
        TableProvider, TableType, create_ordering,
        listing::{ListingTableUrl, PartitionedFile},
        physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource},
    },
    error::Result as DataFusionResult,
    execution::object_store::ObjectStoreUrl,
    logical_expr::{ScalarUDF, SortExpr, col, utils::conjunction},
    physical_expr::LexOrdering,
    physical_plan::{ExecutionPlan, PhysicalExpr},
    prelude::Expr,
    sql::TableReference,
};
use futures::{Stream, StreamExt, TryStreamExt, stream};
use metadata_db::{LocationId, MetadataDb, TableId};
use object_store::{ObjectMeta, ObjectStore, path::Path};
use tracing::info;
use url::Url;
use uuid::Uuid;

use crate::{
    BlockNum, BoxError, Dataset, LogicalCatalog, ResolvedTable,
    catalog::reader::NozzleReaderFactory,
    metadata::{
        FileMetadata, nozzle_metadata_from_parquet_file,
        parquet::ParquetMeta,
        segments::{Chain, Segment, canonical_chain, missing_ranges},
    },
    query_context::NozzleSessionConfig,
    store::{Store, infer_object_store},
};

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

    /// Returns the earliest block number across all tables in this catalog.
    pub async fn earliest_block(&self) -> Result<Option<BlockNum>, BoxError> {
        let mut earliest = None;
        for table in &self.tables {
            let synced_range = table.synced_range().await?;
            match (earliest, &synced_range) {
                (None, Some(range)) => earliest = Some(*range.start()),
                _ => earliest = earliest.min(synced_range.map(|s| *s.start())),
            }
        }
        Ok(earliest)
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
    metadata_db: Arc<MetadataDb>,

    /// ParquetFileReaderFactory
    pub reader_factory: Arc<NozzleReaderFactory>,
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
        let reader_factory = NozzleReaderFactory {
            location_id,
            object_store: object_store.clone(),
            metadata_db: metadata_db.clone(),
        }
        .into();

        Ok(Self {
            table,
            url,
            path,
            location_id,
            metadata_db,
            reader_factory,
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
        let dataset_version = match table.dataset().kind.as_str() {
            "manifest" => table.dataset_version(),
            _ => None,
        };
        let table_id = TableId {
            dataset: dataset_name,
            dataset_version: dataset_version.as_deref(),
            table: &table.name(),
        };

        let path = make_location_path(dataset_name, &table.name());
        let url = data_store.url().join(&path)?;
        let location_id = metadata_db
            .register_location(table_id, data_store.bucket(), &path, &url, false)
            .await?;

        if set_active {
            metadata_db.set_active_location(table_id, &url).await?;
        }

        let path = Path::from_url_path(url.path()).unwrap();

        let object_store = data_store.object_store();
        let reader_factory = NozzleReaderFactory {
            location_id,
            object_store,
            metadata_db: Arc::clone(&metadata_db),
        }
        .into();

        let physical_table = Self {
            table: table.clone(),
            url,
            path,
            location_id,
            metadata_db,
            reader_factory,
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
        let dataset_version = match table.dataset().kind.as_str() {
            "manifest" => table.dataset_version(),
            _ => None,
        };
        let table_id = TableId {
            dataset: &table.dataset().name,
            dataset_version: dataset_version.as_deref(),
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
            Arc::clone(&data_store),
            Arc::clone(&metadata_db),
        )
        .await
    }

    /// Attempt to get the active revision of a table.
    pub async fn get_active(
        table: &ResolvedTable,
        metadata_db: Arc<MetadataDb>,
    ) -> Result<Option<Self>, BoxError> {
        let dataset_name = &table.dataset().name;
        let dataset_version = match table.dataset().kind.as_str() {
            "manifest" => table.dataset_version(),
            _ => None,
        };
        let table_id = TableId {
            dataset: dataset_name,
            dataset_version: dataset_version.as_deref(),
            table: &table.name(),
        };

        let Some((url, location_id)) = metadata_db.get_active_location(table_id).await? else {
            return Ok(None);
        };

        let path = Path::from_url_path(url.path()).unwrap();
        let (object_store, _) = infer_object_store(&url)?;
        let reader_factory = NozzleReaderFactory {
            location_id,
            object_store,
            metadata_db: Arc::clone(&metadata_db),
        }
        .into();

        Ok(Some(Self {
            table: table.clone(),
            url,
            path,
            location_id,
            metadata_db,
            reader_factory,
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

        metadata_db.set_active_location(*table_id, &url).await?;

        let object_store = data_store.object_store();
        let mut file_stream = object_store.list(Some(&path));

        while let Some(object_meta) = file_stream.try_next().await? {
            let (file_name, nozzle_meta, footer) =
                nozzle_metadata_from_parquet_file(&object_meta, object_store.clone()).await?;

            let parquet_meta_json = serde_json::to_value(nozzle_meta)?;

            let ObjectMeta {
                size: object_size,
                e_tag: object_e_tag,
                version: object_version,
                ..
            } = object_meta;

            metadata_db
                .insert_metadata(
                    location_id,
                    file_name,
                    object_size,
                    object_e_tag,
                    object_version,
                    parquet_meta_json,
                    footer,
                )
                .await?;
        }

        let reader_factory = NozzleReaderFactory {
            location_id,
            object_store: Arc::clone(&object_store),
            metadata_db: Arc::clone(&metadata_db),
        }
        .into();

        let physical_table = Self {
            table: table.clone(),
            url: url.clone(),
            path: path.clone(),
            location_id,
            metadata_db,
            reader_factory,
        };

        Ok(physical_table)
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
            .reader_factory
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
        Arc::clone(&self.reader_factory.object_store)
    }

    pub fn table(&self) -> &ResolvedTable {
        &self.table
    }

    pub async fn files(&self) -> Result<Vec<FileMetadata>, BoxError> {
        self.stream_file_metadata().try_collect().await
    }

    /// Return the block range to use for query execution over this table. This is defined as the
    /// contiguous range of block numbers starting from the lowest start block. Ok(None) is
    /// returned if no block range has been synced.
    pub async fn synced_range(&self) -> Result<Option<RangeInclusive<BlockNum>>, BoxError> {
        let chain = self.canonical_chain().await?;
        Ok(chain.map(|c| c.start()..=c.end()))
    }

    pub async fn missing_ranges(
        &self,
        desired: RangeInclusive<BlockNum>,
    ) -> Result<Vec<RangeInclusive<BlockNum>>, BoxError> {
        let segments = self.segments().await?;
        Ok(missing_ranges(segments, desired))
    }

    pub async fn canonical_chain(&self) -> Result<Option<Chain>, BoxError> {
        self.segments().await.map(canonical_chain)
    }

    async fn segments(&self) -> Result<Vec<Segment>, BoxError> {
        self.stream_file_metadata()
            .map(|result| {
                let FileMetadata {
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
                    range: ranges.remove(0),
                    object: object_meta,
                })
            })
            .try_collect()
            .await
    }
}

// Methods for streaming metadata and file information of PhysicalTable
impl PhysicalTable {
    fn stream_file_metadata<'a>(
        &'a self,
    ) -> impl Stream<Item = Result<FileMetadata, BoxError>> + 'a {
        self.metadata_db
            .stream_file_metadata(self.location_id)
            .map(|row| row?.try_into())
    }
}

// helper methods for implementing `TableProvider` trait

impl PhysicalTable {
    fn object_store_url(&self) -> DataFusionResult<ObjectStoreUrl> {
        Ok(ListingTableUrl::try_new(self.url.clone(), None)?.object_store())
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
        let NozzleSessionConfig {
            ignore_canonical_segments,
        } = state
            .config_options()
            .extensions
            .get()
            .cloned()
            .unwrap_or_default();

        let segments = if ignore_canonical_segments {
            self.segments().await?
        } else {
            self.canonical_chain()
                .await?
                .into_iter()
                .flatten()
                .collect()
        };
        let target_partitions = state.config_options().execution.target_partitions;
        let file_groups = round_robin(segments, target_partitions);

        let output_ordering = self.output_ordering()?;

        let file_schema = self.schema();
        let object_store_url = self.object_store_url()?;
        let predicate = self.filters_to_predicate(state, filters)?;

        let parquet_file_reader_factory = Arc::clone(&self.reader_factory);
        let table_parquet_options = state.table_options().parquet.clone();
        let file_source = ParquetSource::new(table_parquet_options)
            .with_parquet_file_reader_factory(parquet_file_reader_factory)
            .with_predicate(predicate)
            .into();

        let data_source = Arc::new(
            FileScanConfigBuilder::new(object_store_url, file_schema, file_source)
                .with_file_groups(file_groups)
                .with_limit(limit)
                .with_output_ordering(output_ordering)
                .with_projection(projection.cloned())
                .build(),
        );

        Ok(Arc::new(DataSourceExec::new(data_source)))
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

fn round_robin(segments: Vec<Segment>, target_partitions: usize) -> Vec<FileGroup> {
    let size = segments.len().min(target_partitions);
    if size == 0 {
        return vec![];
    }
    let mut groups = vec![FileGroup::default(); size];
    for (idx, segment) in segments.into_iter().enumerate() {
        let file = PartitionedFile::from(segment.object);
        groups[idx % size].push(file);
    }
    groups
}
