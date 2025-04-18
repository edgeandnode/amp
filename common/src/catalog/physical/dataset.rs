use std::{
    any::Any,
    ops::Not,
    sync::{Arc, RwLock},
};

use datafusion::{
    catalog::{SchemaProvider, TableProvider},
    common::{DataFusionError, HashMap, Result as DataFusionResult},
};
use metadata_db::{LocationId, MetadataDb, TableId};

use crate::{BoxError, Store};

use super::{dump::validate_name, LogicalDataset, PhysicalTable};

#[derive(Clone, Debug)]
pub struct PhysicalDataset {
    dataset: LogicalDataset,
    tables: Arc<RwLock<HashMap<String, Arc<PhysicalTable>>>>,
}

// Methods to create a new PhysicalDataset instance
impl PhysicalDataset {
    pub fn new(dataset: LogicalDataset, tables: Vec<PhysicalTable>) -> Self {
        // Convert the tables to Arc<Table>
        let tables = tables
            .into_iter()
            .map(|table| (table.table_name().to_string(), Arc::new(table)))
            .collect::<HashMap<_, _>>();
        let dataset = PhysicalDataset {
            dataset,
            tables: Arc::new(RwLock::new(HashMap::new())),
        };
        for (name, table) in tables {
            dataset.register_table(name, table).unwrap();
        }
        dataset
    }

    pub async fn from_dataset_at<Dataset: AsRef<LogicalDataset>, MetaData: AsRef<MetadataDb>>(
        dataset: Dataset,
        data_store: Arc<Store>,
        metadata_db: Option<MetaData>,
        read_only: bool,
    ) -> Result<Self, BoxError> {
        let dataset_name = dataset.as_ref().name.clone();

        let physical_dataset = PhysicalDataset {
            dataset: dataset.as_ref().clone(),
            tables: Arc::new(RwLock::new(HashMap::new())),
        };

        for logical_table in dataset.as_ref().tables() {
            match metadata_db {
                Some(ref metadata_provider) => {
                    let table_id = TableId {
                        dataset: &dataset_name,
                        dataset_version: None,
                        table: &logical_table.name,
                    };
                    // If an active location exists for this table, this `PhysicalTable` will point to that location.
                    // Otherwise, a new location will be registered in the metadata DB. The location will be active.
                    let table = if let Some((url, location_id)) = metadata_provider
                        .as_ref()
                        .get_active_location(table_id)
                        .await?
                    {
                        PhysicalTable::try_at_active_location(
                            &dataset_name,
                            logical_table,
                            url,
                            location_id,
                            data_store.object_store(),
                            metadata_provider,
                        )?
                    } else if !read_only {
                        PhysicalTable::try_next_revision(
                            logical_table,
                            &data_store,
                            &dataset_name,
                            metadata_provider,
                        )
                        .await?
                    } else {
                        return Err(format!(
                            "table {}.{} has no active location",
                            dataset_name, logical_table.name
                        )
                        .into());
                    };
                    physical_dataset
                        .register_table(table.table_name().to_string(), Arc::new(table))?;
                }
                None => {
                    let table = PhysicalTable::try_at_static_location(
                        data_store.clone(),
                        &dataset_name,
                        logical_table.clone(),
                    )?;
                    physical_dataset
                        .register_table(table.table_name().to_string(), Arc::new(table))?;
                }
            }
        }
        Ok(physical_dataset)
    }
}

// Methods to access the dataset
impl PhysicalDataset {
    /// All tables in the catalog, except meta tables.
    pub fn tables(&self) -> Vec<Arc<PhysicalTable>> {
        let tables = self.tables.read().unwrap();
        tables
            .clone()
            .into_iter()
            .filter_map(|(_, table)| table.is_meta().not().then_some(table))
            .collect::<Vec<_>>()
    }

    pub fn meta_tables(&self) -> Vec<Arc<PhysicalTable>> {
        let tables = self.tables.read().unwrap();
        tables
            .clone()
            .into_iter()
            .filter_map(|(_, table)| table.is_meta().then_some(table))
            .collect::<Vec<_>>()
    }

    pub fn name(&self) -> &str {
        &self.dataset.name
    }

    pub fn kind(&self) -> &str {
        &self.dataset.kind
    }

    pub fn location_ids(&self) -> Vec<LocationId> {
        let tables = self.tables.read().unwrap();
        tables
            .values()
            .filter_map(|table| table.location_id())
            .collect()
    }
}

impl AsRef<PhysicalDataset> for PhysicalDataset {
    fn as_ref(&self) -> &PhysicalDataset {
        self
    }
}

impl TryFrom<Arc<dyn SchemaProvider>> for PhysicalDataset {
    type Error = DataFusionError;

    fn try_from(value: Arc<dyn SchemaProvider>) -> Result<Self, Self::Error> {
        value
            .as_ref()
            .as_any()
            .downcast_ref::<PhysicalDataset>()
            .cloned()
            .ok_or(DataFusionError::Internal(format!(
                "Cannot convert to PhysicalDataset, type: {:?}",
                value.as_any().type_id()
            )))
    }
}

#[async_trait::async_trait]
impl SchemaProvider for PhysicalDataset {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables()
            .into_iter()
            .map(|t| t.table_name().to_string())
            .collect()
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        let tables = self
            .tables
            .read()
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let table = tables
            .iter()
            .find(|(table_name, _)| *table_name == name)
            .map(|(_, t)| t.clone() as Arc<dyn TableProvider>);
        Ok(table)
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        validate_name(&name)?;

        // Do we block overwriting existing tables?
        // if self.tables.blocking_read().iter().any(|(t, _)| t == &name) {
        //     return Err(DataFusionError::Plan(format!(
        //         "Table {} already exists",
        //         name
        //     )));
        // }

        if let Some(table) = table.as_ref().as_any().downcast_ref::<PhysicalTable>() {
            let table = Arc::new(table.clone());
            let mut tables = self
                .tables
                .write()
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
            tables.insert(name.clone(), table.clone());
            Ok(Some(table))
        } else {
            Err(DataFusionError::Plan(format!(
                "Table {} is not a valid table. Expected a Table, found {:?}",
                name,
                table.as_ref().type_id()
            )))
        }
    }

    fn deregister_table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        let mut tables = self
            .tables
            .write()
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        Ok(tables.remove(name).map(|t| t as Arc<dyn TableProvider>))
    }

    fn table_exist(&self, name: &str) -> bool {
        if let Ok(tables) = self.tables.read() {
            tables.iter().any(|(table_name, _)| table_name == name)
        } else {
            false
        }
    }
}
