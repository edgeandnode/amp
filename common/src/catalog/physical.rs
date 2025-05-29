use std::{collections::BTreeMap, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    logical_expr::{col, ScalarUDF, SortExpr},
    parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader},
    sql::TableReference,
};
use futures::{
    stream::{self, BoxStream},
    StreamExt, TryStreamExt,
};
use metadata_db::{LocationId, MetadataDb, TableId};
use object_store::{path::Path, ObjectMeta, ObjectStore};
use url::Url;
use uuid::Uuid;

use super::logical::Table;
use crate::{
    metadata::{
        parquet::{ParquetMeta, PARQUET_METADATA_KEY},
        FileMetadata,
    },
    store::{infer_object_store, Store},
    BoxError, Dataset,
};

#[derive(Debug, Clone)]
pub struct Catalog {
    datasets: Vec<PhysicalDataset>,
    /// User-defined functions (UDFs) specific to this catalog.
    udfs: Vec<ScalarUDF>,
}

impl Catalog {
    pub fn empty() -> Self {
        Catalog {
            datasets: vec![],
            udfs: vec![],
        }
    }

    pub fn new(datasets: Vec<PhysicalDataset>) -> Self {
        Catalog {
            datasets,
            udfs: vec![],
        }
    }

    pub fn add_dataset(&mut self, dataset: PhysicalDataset) {
        self.datasets.push(dataset);
    }

    pub fn add_udf(&mut self, udf: ScalarUDF) {
        self.udfs.push(udf);
    }

    pub fn datasets(&self) -> &[PhysicalDataset] {
        &self.datasets
    }

    pub fn udfs(&self) -> &[ScalarUDF] {
        &self.udfs
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

    /// All tables in the catalog
    pub fn tables(&self) -> impl Iterator<Item = &PhysicalTable> {
        self.tables.iter()
    }

    pub fn name(&self) -> &str {
        &self.dataset.name
    }

    pub fn kind(&self) -> &str {
        &self.dataset.kind
    }

    pub fn location_ids(&self) -> Vec<LocationId> {
        self.tables.iter().map(|t| t.location_id()).collect()
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalTable {
    /// Logical table representation.
    table: Table,
    /// Qualified table reference in the format `dataset_name.table_name`.
    table_ref: TableReference,

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
}

impl PhysicalTable {
    /// Create a new physical table with the given dataset name, table, URL, and object store.
    pub fn new(
        dataset_name: &str,
        table: Table,
        url: Url,
        location_id: LocationId,
        metadata_db: Arc<MetadataDb>,
    ) -> Result<Self, BoxError> {
        validate_name(&table.name)?;

        let table_ref = TableReference::partial(dataset_name, table.name.as_str());
        let path = Path::from_url_path(url.path()).unwrap();
        let (object_store, _) = infer_object_store(&url)?;

        Ok(Self {
            table,
            table_ref,
            url,
            path,
            object_store,
            location_id,
            metadata_db,
        })
    }

    /// Create a new physical table with the given dataset name, table, URL, and object store.
    /// This is used for creating a new location (revision) for a new or  existing table in
    /// the metadata database.
    pub async fn next_revision(
        table: &Table,
        data_store: &Store,
        dataset_name: &str,
        metadata_db: Arc<MetadataDb>,
    ) -> Result<Self, BoxError> {
        let table_id = TableId {
            dataset: dataset_name,
            dataset_version: None,
            table: &table.name,
        };

        let path = make_location_path(table_id);
        let url = data_store.url().join(&path)?;
        let location_id = metadata_db
            .register_location(table_id, data_store.bucket(), &path, &url, false)
            .await?;

        let path = Path::from_url_path(url.path()).unwrap();
        let table_ref = TableReference::partial(dataset_name, table.name.as_str());
        let physical_table = Self {
            table: table.clone(),
            table_ref,
            url,
            path,
            object_store: data_store.object_store(),
            location_id,
            metadata_db,
        };
        Ok(physical_table)
    }

    /// Attempts to restore the latest revision of a table from the data store.
    /// If the table is not found, it returns `None`.
    pub async fn restore_latset_revision(
        table: &Table,
        data_store: Arc<Store>,
        dataset_name: &str,
        metadata_db: Arc<MetadataDb>,
    ) -> Result<Option<Self>, BoxError> {
        let table_id = TableId {
            dataset: dataset_name,
            dataset_version: None,
            table: &table.name,
        };

        let prefix = format!("{}/{}/", &dataset_name, table.name);
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

    /// Attempt to get the active revision of a table. If it doesn't exist, restore the latest revision
    /// and register it in the metadata database.
    pub async fn get_or_restore_active_revision(
        table: &Table,
        dataset_name: &str,
        data_store: Arc<Store>,
        metadata_db: Arc<MetadataDb>,
    ) -> Result<Option<Self>, BoxError> {
        let table_id = TableId {
            dataset: dataset_name,
            dataset_version: None,
            table: &table.name,
        };

        let physical_table =
            if let Some((url, location_id)) = metadata_db.get_active_location(table_id).await? {
                let table_ref = TableReference::partial(dataset_name, table.name.as_str());
                let path = Path::from_url_path(url.path()).unwrap();
                let (object_store, _) = infer_object_store(&url)?;
                Some(Self {
                    table: table.clone(),
                    table_ref,
                    url,
                    path,
                    object_store,
                    location_id,
                    metadata_db: metadata_db.clone(),
                })
            } else {
                PhysicalTable::restore_latset_revision(
                    table,
                    data_store.clone(),
                    dataset_name,
                    metadata_db.clone(),
                )
                .await?
            };

        Ok(physical_table)
    }

    /// Attempt to restore the latest revision of a table from a provided map of revisions
    /// and register it in the metadata database.
    /// If no revisions are found, it returns `None`.
    ///
    /// Revisions are expected to be sorted in ascending order by their revision uuid.
    async fn restore_latest(
        revisions: BTreeMap<String, (Path, Url, String)>,
        table: &Table,
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
        table: &Table,
        table_id: &TableId<'_>,
        prefix: &str,
        path: &Path,
        url: &Url,
        data_store: Arc<Store>,
        metadata_db: Arc<MetadataDb>,
    ) -> Result<Self, BoxError> {
        let table_ref = TableReference::partial(table_id.dataset, table.name.as_str());
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

        let physical_table = Self {
            table: table.clone(),
            table_ref,
            url: url.clone(),
            path: path.clone(),
            object_store,
            location_id,
            metadata_db,
        };

        Ok(physical_table)
    }
}

impl PhysicalTable {
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

    pub fn schema(&self) -> SchemaRef {
        self.table.schema.clone()
    }

    pub fn location_id(&self) -> LocationId {
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

    pub fn network(&self) -> Option<&str> {
        self.table.network.as_ref().map(|n| n.as_str())
    }

    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.object_store.clone()
    }

    pub fn table(&self) -> &Table {
        &self.table
    }

    pub fn stream_file_metadata(&self) -> BoxStream<'_, Result<FileMetadata, BoxError>> {
        self.metadata_db
            .stream_file_metadata(self.location_id)
            .map(|row| row?.try_into())
            .boxed()
    }

    pub fn stream_ranges(&self) -> BoxStream<'_, Result<(u64, u64), BoxError>> {
        self.stream_file_metadata()
            .map_ok(
                |FileMetadata {
                     parquet_meta:
                         ParquetMeta {
                             range_start,
                             range_end,
                             ..
                         },
                     ..
                 }| { (range_start, range_end) },
            )
            .boxed()
    }

    pub fn stream_file_names(&self) -> BoxStream<'_, Result<String, BoxError>> {
        self.stream_file_metadata()
            .map_ok(|FileMetadata { file_name, .. }| file_name)
            .boxed()
    }

    pub fn stream_parquet_files(&self) -> BoxStream<'_, Result<(String, ObjectMeta), BoxError>> {
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

    pub async fn ranges(&self) -> Result<Vec<(u64, u64)>, BoxError> {
        let ranges = self.stream_ranges().try_collect().await?;
        Ok(ranges)
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
pub fn make_location_path(table_id: TableId<'_>) -> String {
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
