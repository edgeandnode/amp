use datafusion::{
    logical_expr::{col, Expr},
    sql::TableReference,
};
use url::Url;

use super::logical::Table;
use crate::{store::Store, BoxError, Dataset};

pub struct Catalog {
    store: Store,
    datasets: Vec<PhysicalDataset>,
}

impl Catalog {
    /// To obtain `url` and `object_store`, call `infer_object_store` on a path.
    pub fn empty(store: Store) -> Result<Self, BoxError> {
        Ok(Catalog {
            store,
            datasets: vec![],
        })
    }

    /// The tables are assumed to live in the path:
    /// `<url>/<dataset_name>/<table_name>`
    /// Where `url` is the base URL of this catalog.
    pub fn register(&mut self, dataset: &Dataset) -> Result<(), BoxError> {
        let physical_dataset =
            PhysicalDataset::from_dataset_at(dataset.clone(), self.store.url().clone())?;
        self.datasets.push(physical_dataset);
        Ok(())
    }

    /// Will include meta tables.
    pub fn for_dataset(dataset: &Dataset, data_store: Store) -> Result<Self, BoxError> {
        let mut this = Self::empty(data_store)?;
        this.register(dataset)?;
        Ok(this)
    }

    pub fn store(&self) -> &Store {
        &self.store
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
    #[allow(unused)]
    pub(crate) url: Url,
    pub(crate) tables: Vec<PhysicalTable>,
}

impl PhysicalDataset {
    /// The tables are assumed to live in the subpath:
    /// `<url>/<dataset_name>/<table_name>`
    pub fn from_dataset_at(dataset: Dataset, url: Url) -> Result<Self, BoxError> {
        let dataset_name = dataset.name.clone();
        validate_name(&dataset_name)?;

        let tables = {
            let mut tables = dataset.tables.clone();
            tables.append(&mut dataset.meta_tables());
            tables
        };

        let physical_tables = tables
            .iter()
            .map(|table| PhysicalTable::resolve(&url, &dataset_name, table))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(PhysicalDataset {
            dataset,
            url,
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
}

#[derive(Debug, Clone)]
pub struct PhysicalTable {
    pub table: Table,
    pub table_ref: TableReference,

    // URL in a format understood by the object store.
    pub url: Url,
}

impl PhysicalTable {
    pub fn table_name(&self) -> &str {
        &self.table.name
    }

    pub fn catalog_schema(&self) -> &str {
        // Unwrap: This is always constructed with a schema.
        &self.table_ref.schema().unwrap()
    }

    fn resolve(base: &Url, dataset_name: &str, table: &Table) -> Result<Self, BoxError> {
        validate_name(&table.name)?;

        let url = if base.scheme() == "file" {
            Url::from_directory_path(&format!("/{}/", &table.name))
                .map_err(|()| "error parsing table name as URL")?
        } else {
            base.join(&format!("{}/", &table.name))?
        };

        let table_ref = TableReference::partial(dataset_name, table.name.as_str());

        Ok(PhysicalTable {
            table: table.clone(),
            table_ref,
            url,
        })
    }

    pub fn is_meta(&self) -> bool {
        self.table.is_meta()
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
