use std::sync::Arc;

use datafusion::{
    arrow::datatypes::SchemaRef,
    logical_expr::{col, Expr},
    sql::TableReference,
};
use metadata_db::{make_location_path, MetadataDb, ViewId};
use url::Url;

use super::logical::Table;
use crate::{store::Store, BoxError, Dataset};

pub struct Catalog {
    datasets: Vec<PhysicalDataset>,
}

impl Catalog {
    pub fn empty() -> Self {
        Catalog { datasets: vec![] }
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
    pub(crate) dataset: Dataset,
    pub(crate) data_store: Arc<Store>,
    pub(crate) tables: Vec<PhysicalTable>,
}

impl PhysicalDataset {
    /// The tables are assumed to live in the subpath:
    /// `<url>/<dataset_name>/<table_name>`
    pub async fn from_dataset_at(
        dataset: Dataset,
        data_store: Arc<Store>,
        metadata_db: Option<&MetadataDb>,
    ) -> Result<Self, BoxError> {
        let dataset_name = dataset.name.clone();
        validate_name(&dataset_name)?;

        let tables = {
            let mut tables = dataset.tables.clone();
            tables.append(&mut dataset.meta_tables());
            tables
        };

        let mut physical_tables = vec![];
        for table in tables {
            let base = &data_store.url();
            match metadata_db {
                Some(db) => {
                    physical_tables.push(
                        PhysicalTable::resolve_or_register_location(
                            base,
                            db,
                            &dataset_name,
                            &table,
                        )
                        .await?,
                    );
                }
                None => {
                    physical_tables.push(PhysicalTable::static_location(
                        base,
                        &dataset_name,
                        &table,
                    )?);
                }
            }
        }

        Ok(PhysicalDataset {
            dataset,
            data_store,
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

    pub fn data_store(&self) -> Arc<Store> {
        self.data_store.clone()
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalTable {
    table: Table,
    table_ref: TableReference,

    // Absolute URL.
    url: Url,

    // Location path relative the store URL.
    path: String,
}

impl PhysicalTable {
    /// If an active location exists for this view, this `PhysicalTable` will point to that location.
    /// Otherwise, a new location will be registered in the metadata DB. The location will be active.
    async fn resolve_or_register_location(
        base: &Url,
        metadata_db: &MetadataDb,
        dataset_name: &str,
        table: &Table,
    ) -> Result<Self, BoxError> {
        validate_name(&table.name)?;

        let (path, url) = {
            let view_id = ViewId {
                dataset: dataset_name,
                dataset_version: None,
                view: &table.name,
            };

            let active_location = metadata_db.get_active_location(view_id).await?;
            let location = match active_location {
                Some(location) => location,
                None => {
                    let path = make_location_path(view_id);
                    metadata_db.register_location(view_id, &path, true).await?;
                    path
                }
            };
            (location.clone(), base.join(&location)?)
        };

        let table_ref = TableReference::partial(dataset_name, table.name.as_str());

        Ok(PhysicalTable {
            table: table.clone(),
            table_ref,
            url,
            path,
        })
    }

    /// The static location is always `<base>/<dataset_name>/<table_name>/`.
    ///
    /// This is used in a few situations where no metadata DB is available:
    /// - When using `dump` as a simple CLI tool without a Postgres for metadata.
    /// - For snapshot testing.
    fn static_location(base: &Url, dataset_name: &str, table: &Table) -> Result<Self, BoxError> {
        validate_name(&table.name)?;

        let path = format!("{}/{}/", dataset_name, table.name);
        let url = base.join(&path)?;
        let table_ref = TableReference::partial(dataset_name, table.name.as_str());

        Ok(PhysicalTable {
            table: table.clone(),
            table_ref,
            url,
            path,
        })
    }

    pub fn table_name(&self) -> &str {
        &self.table.name
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    pub fn path(&self) -> &str {
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

    /// Qualified table reference in the format `dataset_name.table_name`.
    pub fn table_ref(&self) -> &TableReference {
        &self.table_ref
    }

    pub fn order_exprs(&self) -> Vec<Vec<Expr>> {
        self.table
            .sorted_by()
            .iter()
            .map(|col_name| vec![col(col_name).sort(true, false)])
            .collect()
    }

    pub fn network(&self) -> Option<&str> {
        self.table.network.as_ref().map(|n| n.as_str())
    }
}

fn validate_name(name: &str) -> Result<(), BoxError> {
    if let Some(c) = name
        .chars()
        .find(|&c| !(c.is_ascii_lowercase() || c == '_'))
    {
        return Err(format!(
            "names must be lowercase and contain only letters and underscores, \
             the name: '{name}' is not allowed because it contains the character '{c}'"
        )
        .into());
    }

    Ok(())
}
