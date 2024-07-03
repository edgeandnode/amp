use std::sync::Arc;

use datafusion::{
    arrow::datatypes::SchemaRef,
    logical_expr::{col, Expr},
    sql::TableReference,
};
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

    /// The tables are assumed to live in the path:
    /// `<url>/<dataset_name>/<table_name>`
    /// Where `url` is the base URL of this catalog.
    pub fn register(&mut self, dataset: &Dataset, data_store: Arc<Store>) -> Result<(), BoxError> {
        let physical_dataset = PhysicalDataset::from_dataset_at(dataset.clone(), data_store)?;
        self.datasets.push(physical_dataset);
        Ok(())
    }

    /// Will include meta tables.
    pub fn for_dataset(dataset: &Dataset, data_store: Arc<Store>) -> Result<Self, BoxError> {
        let mut this = Self::empty();
        this.register(dataset, data_store)?;
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
    pub fn from_dataset_at(dataset: Dataset, data_store: Arc<Store>) -> Result<Self, BoxError> {
        let dataset_name = dataset.name.clone();
        validate_name(&dataset_name)?;

        let tables = {
            let mut tables = dataset.tables.clone();
            tables.append(&mut dataset.meta_tables());
            tables
        };

        let physical_tables = tables
            .iter()
            .map(|table| PhysicalTable::resolve(data_store.url(), &dataset_name, table))
            .collect::<Result<Vec<_>, _>>()?;

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

    // Path relative the store URL.
    path: String,
}

impl PhysicalTable {
    fn resolve(base: &Url, dataset_name: &str, table: &Table) -> Result<Self, BoxError> {
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
