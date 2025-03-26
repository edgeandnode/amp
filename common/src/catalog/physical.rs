use std::{collections::BTreeMap, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    logical_expr::{col, SortExpr},
    parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader},
    sql::TableReference,
};
use futures::{stream, TryStreamExt};
use metadata_db::{LocationId, MetadataDb, TableId};
use object_store::{path::Path, ObjectMeta, ObjectStore};
use url::Url;

use super::logical::Table;
use crate::{
    meta_tables::scanned_ranges::{self, ScannedRange},
    store::{infer_object_store, Store},
    BoxError, Dataset,
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

    pub async fn register(
        &mut self,
        dataset: &Dataset,
        data_store: Arc<Store>,
        metadata_db: Option<&MetadataDb>,
    ) -> Result<(), BoxError> {
        let physical_dataset =
            PhysicalDataset::from_dataset_at(dataset.clone(), data_store, metadata_db).await?;
        self.datasets.push(physical_dataset);
        Ok(())
    }

    /// Will include meta tables.
    pub async fn for_dataset(
        dataset: &Dataset,
        data_store: Arc<Store>,
        metadata_db: Option<&MetadataDb>,
    ) -> Result<Self, BoxError> {
        let mut this = Self::empty();
        this.register(dataset, data_store, metadata_db).await?;
        Ok(this)
    }

    pub fn datasets(&self) -> &[PhysicalDataset] {
        &self.datasets
    }

    pub fn all_tables(&self) -> impl Iterator<Item = &PhysicalTable> {
        self.datasets.iter().flat_map(|dataset| dataset.tables())
    }

    pub fn all_meta_tables(&self) -> impl Iterator<Item = &PhysicalTable> {
        self.datasets
            .iter()
            .flat_map(|dataset| dataset.meta_tables())
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
    ) -> Result<Self, BoxError> {
        let dataset_name = dataset.name.clone();
        validate_name(&dataset_name)?;

        let mut physical_tables = vec![];
        for table in dataset.tables_with_meta() {
            match metadata_db {
                Some(db) => {
                    physical_tables.push(
                        PhysicalTable::resolve_or_register_location(
                            data_store.clone(),
                            db,
                            &dataset_name,
                            &table,
                        )
                        .await?,
                    );
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
    object_store: Arc<dyn ObjectStore>,
    location_id: Option<LocationId>,
}

impl PhysicalTable {
    pub fn new(
        dataset_name: &str,
        table: Table,
        url: Url,
        location_id: Option<LocationId>,
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
        })
    }

    /// If an active location exists for this table, this `PhysicalTable` will point to that location.
    /// Otherwise, a new location will be registered in the metadata DB. The location will be active.
    async fn resolve_or_register_location(
        data_store: Arc<Store>,
        metadata_db: &MetadataDb,
        dataset_name: &str,
        table: &Table,
    ) -> Result<Self, BoxError> {
        let (url, location_id) = {
            let table_id = TableId {
                dataset: dataset_name,
                dataset_version: None,
                table: &table.name,
            };

            let active_location = metadata_db.get_active_location(table_id).await?;
            match active_location {
                Some((url, location_id)) => (url, location_id),
                None => {
                    let path = make_location_path(table_id);
                    let url = data_store.url().join(&path)?;
                    let location_id = metadata_db
                        .register_location(table_id, data_store.bucket(), &path, &url, true)
                        .await?;
                    (url, location_id)
                }
            }
        };

        Self::new(dataset_name, table.clone(), url, Some(location_id))
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

        Ok(PhysicalTable {
            table: table.clone(),
            table_ref,
            url,
            path,
            object_store: data_store.object_store(),
            location_id: None,
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
        let physical_table = Self {
            table: table.clone(),
            table_ref,
            url,
            path,
            object_store: data_store.object_store(),
            location_id: Some(location_id),
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

    // TODO: Break this into smaller functions
    // TODO: Buffer the stream and sort the ranges after
    pub async fn scanned_ranges(&self) -> Result<Vec<(u64, u64)>, BoxError> {
        let ranges = self
            .object_store
            .list(Some(self.path()))
            .try_filter_map(|f| async move {
                let mut reader = ParquetObjectReader::new(self.object_store.clone(), f);
                let scanned_range: Option<(u64, u64)> =
                    reader.get_metadata().await.map_or(None, |parquet_meta| {
                        parquet_meta
                            .file_metadata()
                            .key_value_metadata()
                            .map(|kv_meta| {
                                kv_meta
                                    .into_iter()
                                    .find(|kv| kv.key.as_str() == scanned_ranges::TABLE_NAME)
                            })
                            .flatten()
                            .map(|kv| {
                                let scanned_range: ScannedRange = serde_json::from_str(kv.value.clone().unwrap().as_str()).ok()?;
                                Some((scanned_range.range_start, scanned_range.range_end))
                            })
                            .flatten()
                    });
                Ok(scanned_range)
            })
            .try_collect::<Vec<_>>()
            .await?;
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
