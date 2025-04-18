mod dataset;
mod dump;
mod table;

use datafusion::{
    catalog::{CatalogProvider, SchemaProvider},
    common::{exec_err, HashMap},
    error::{DataFusionError, Result as DataFusionResult},
};
use dump::validate_name;
use std::{
    any::Any,
    sync::{Arc, RwLock},
};

pub use dataset::PhysicalDataset;
use metadata_db::MetadataDb;
pub use table::PhysicalTable;

use crate::Store;

use super::logical::Dataset as LogicalDataset;

#[derive(Clone, Debug)]
pub struct Catalog {
    datasets: Arc<RwLock<HashMap<String, Arc<dyn SchemaProvider>>>>,
    store: Option<Arc<Store>>,
    metadata_provider: Option<Arc<MetadataDb>>,
}

impl Default for Catalog {
    fn default() -> Self {
        Self::empty()
    }
}

// Methods to create a new Catalog instance
impl Catalog {
    pub fn empty() -> Self {
        let datasets = Arc::new(RwLock::new(HashMap::new()));
        Catalog {
            datasets,
            store: None,
            metadata_provider: None,
        }
    }

    pub async fn for_physical_dataset<T: AsRef<PhysicalDataset>, U: AsRef<MetadataDb>>(
        dataset: T,
        store: Arc<Store>,
        metadata_provider: Option<U>,
    ) -> DataFusionResult<Self> {
        let store = Some(store);
        let metadata_provider = metadata_provider.map(|mp| Arc::new((*mp.as_ref()).clone()));
        let schema = Arc::new((*dataset.as_ref()).clone());
        let catalog = Catalog {
            datasets: Arc::new(RwLock::new(HashMap::new())),
            store,
            metadata_provider,
        };

        let name = schema.name();

        validate_name(name)?;

        catalog.register_schema(name, schema.clone())?;

        Ok(catalog)
    }

    pub async fn for_logical_dataset<
        Dataset: AsRef<LogicalDataset>,
        Metadata: AsRef<MetadataDb>,
    >(
        dataset: Dataset,
        store: Arc<Store>,
        metadata_provider: Option<Metadata>,
        read_only: bool,
    ) -> DataFusionResult<Self> {
        let store = Some(store);
        let metadata_provider = metadata_provider.map(|mp| Arc::new(mp.as_ref().clone()));
        let datasets = Arc::new(RwLock::new(HashMap::new()));

        let catalog = Catalog {
            datasets,
            store,
            metadata_provider: metadata_provider.clone(),
        };

        let (name, dataset) = catalog.build_dataset(dataset, read_only).await?;

        catalog.register_schema(&name, dataset.clone())?;

        Ok(catalog)
    }

    pub fn from_store(store: Arc<Store>) -> Self {
        let datasets = Arc::new(RwLock::new(HashMap::new()));

        Catalog {
            datasets,
            store: Some(store),
            metadata_provider: None,
        }
    }

    pub fn from_metadata_provider(metadata_provider: Arc<MetadataDb>) -> Self {
        let datasets = Arc::new(RwLock::new(HashMap::new()));

        Catalog {
            datasets,
            store: None,
            metadata_provider: Some(metadata_provider),
        }
    }

    pub fn with_store(self, store: Arc<Store>) -> Self {
        Catalog {
            store: Some(store),
            ..self
        }
    }

    pub fn with_metadata_provider(self, metadata_provider: Option<Arc<MetadataDb>>) -> Self {
        Catalog {
            metadata_provider,
            ..self
        }
    }
}

// Methods to register and build datasets
impl Catalog {
    pub async fn build_dataset<Dataset: AsRef<LogicalDataset>>(
        &self,
        dataset: Dataset,
        read_only: bool,
    ) -> DataFusionResult<(String, Arc<PhysicalDataset>)> {
        let dataset_name = dataset.as_ref().name.clone();
        validate_name(&dataset_name)?;
        let data_store = self.store.clone().ok_or(DataFusionError::Internal(
            "Store not set in Catalog".to_string(),
        ))?;

        let metadata_db = self.metadata_provider.clone();

        let physical_dataset =
            PhysicalDataset::from_dataset_at(dataset, data_store, metadata_db, read_only).await?;

        Ok((dataset_name.clone(), Arc::new(physical_dataset)))
    }

    pub async fn all_datasets(&self) -> DataFusionResult<Vec<Arc<PhysicalDataset>>> {
        let datasets = self.datasets.read().map_err(|e| {
            DataFusionError::Execution(format!("Failed to acquire read lock on datasets: {}", e))
        })?;

        let mut all_datasets = Vec::new();

        for dataset in datasets.values() {
            let dataset = PhysicalDataset::try_from(dataset.clone())?;
            all_datasets.push(Arc::new(dataset));
        }

        Ok(all_datasets)
    }

    pub async fn all_schemas(&self) -> DataFusionResult<Vec<Arc<dyn SchemaProvider>>> {
        let datasets = self.datasets.read().map_err(|e| {
            DataFusionError::Execution(format!("Failed to acquire read lock on datasets: {}", e))
        })?;

        Ok(datasets
            .iter()
            .map(|(_, dataset)| dataset.clone() as Arc<dyn SchemaProvider>)
            .collect())
    }

    pub async fn all_tables(&self) -> DataFusionResult<Vec<Arc<PhysicalTable>>> {
        let datasets = self.datasets.read().map_err(|e| {
            DataFusionError::Execution(format!("Failed to acquire read lock on datasets: {}", e))
        })?;
        let mut all_tables = Vec::new();

        for dataset in datasets.values() {
            let dataset = PhysicalDataset::try_from(dataset.clone())?;
            let tables = dataset.tables();
            all_tables.extend(tables);
        }

        Ok(all_tables)
    }

    pub fn all_tables_blocking(&self) -> DataFusionResult<Vec<Arc<PhysicalTable>>> {
        let datasets = self.datasets.read().map_err(|e| {
            DataFusionError::Execution(format!("Failed to acquire read lock on datasets: {}", e))
        })?;
        let mut all_tables = Vec::new();

        for dataset in datasets.values() {
            let dataset = PhysicalDataset::try_from(dataset.clone())?;
            let tables = dataset.tables();
            all_tables.extend(tables);
        }

        Ok(all_tables)
    }
}

#[async_trait::async_trait]
impl CatalogProvider for Catalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Retrieves the list of available schema names in this catalog.
    fn schema_names(&self) -> Vec<String> {
        let datasets = self
            .datasets
            .read()
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to acquire read lock on datasets: {}",
                    e
                ))
            })
            .expect("Failed to acquire read lock on datasets");

        datasets
            .iter()
            .map(|(dataset_name, ..)| dataset_name.into())
            .collect()
    }

    /// Retrieves a specific schema from the catalog by name, provided it exists.
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let datasets = self
            .datasets
            .read()
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to acquire read lock on datasets: {}",
                    e
                ))
            })
            .expect("Failed to acquire read lock on datasets");

        datasets
            .iter()
            .filter_map(|(dataset_name, dataset)| {
                dataset_name
                    .eq(name)
                    .then_some(dataset.clone() as Arc<dyn SchemaProvider>)
            })
            .next()
    }

    /// Adds a new schema to this catalog.
    ///
    /// If a schema of the same name existed before, it is replaced in
    /// the catalog and returned.
    ///
    /// By default returns a "Not Implemented" error
    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> DataFusionResult<Option<Arc<dyn SchemaProvider>>> {
        let mut datasets = self.datasets.write().map_err(|e| {
            DataFusionError::Execution(format!("Failed to acquire read lock on datasets: {}", e))
        })?;

        if let Some(dataset) = schema.as_ref().as_any().downcast_ref::<PhysicalDataset>() {
            let dataset = Arc::new(dataset.clone());
            Ok(datasets
                .insert(name.to_string(), dataset.clone())
                .map(|dataset| dataset as Arc<dyn SchemaProvider>))
        } else {
            Err(DataFusionError::Plan(format!(
                "Schema {} is not a PhysicalDataset",
                name
            )))
        }
    }

    /// Removes a schema from this catalog. Implementations of this method should return
    /// errors if the schema exists but cannot be dropped. For example, in DataFusion's
    /// default in-memory catalog, `MemoryCatalogProvider`, a non-empty schema
    /// will only be successfully dropped when `cascade` is true.
    /// This is equivalent to how DROP SCHEMA works in PostgreSQL.
    ///
    /// Implementations of this method should return None if schema with `name`
    /// does not exist.
    ///
    /// By default returns a "Not Implemented" error
    fn deregister_schema(
        &self,
        name: &str,
        cascade: bool,
    ) -> DataFusionResult<Option<Arc<dyn SchemaProvider>>> {
        if let Some(dataset) = self.schema(name) {
            let dependent_tables = dataset.table_names();
            match (dependent_tables.is_empty(), cascade) {
                (true, _) | (false, true) => {
                    let mut datasets = self.datasets.write().map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to acquire write lock on datasets: {}",
                            e
                        ))
                    })?;

                    Ok(datasets
                        .remove(name)
                        .map(|dataset| dataset as Arc<dyn SchemaProvider>))
                }
                (false, false) => exec_err!(
                    "Cannot drop dataset {} because other tables depend on it: {}",
                    name,
                    itertools::join(dependent_tables.iter(), ", ")
                ),
            }
        } else {
            Ok(None)
        }
    }
}
