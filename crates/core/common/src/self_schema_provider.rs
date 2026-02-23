//! Schema provider for virtual schemas like `"self"`.
//!
//! Handles schema names that don't correspond to any dataset in the store but
//! still need to resolve tables and functions.

use std::{any::Any, collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::{
        AsyncSchemaProvider as TableAsyncSchemaProvider, SchemaProvider as TableSchemaProvider,
        TableProvider,
    },
    error::DataFusionError,
    logical_expr::{ScalarUDF, async_udf::AsyncScalarUDF},
};
use datasets_common::table_name::TableName;
use datasets_derived::{deps::SELF_REF_KEYWORD, func_name::FuncName, manifest::Function};
use js_runtime::{isolate_pool::IsolatePool, js_udf::JsUdf};
use parking_lot::RwLock;

use crate::{
    catalog::logical::LogicalTable,
    func_catalog::{
        function_provider::{FunctionProvider, ScalarFunctionProvider},
        schema_provider::{
            AsyncSchemaProvider as FuncAsyncSchemaProvider, SchemaProvider as FuncSchemaProvider,
        },
    },
    plan_table::PlanTable,
};

/// Schema provider for virtual schemas (e.g., `"self"`) that resolve tables
/// and functions without requiring a backing dataset in the store.
pub struct SelfSchemaProvider {
    schema_name: String,
    logical_tables: Vec<LogicalTable>,
    udfs: Vec<ScalarUDF>,
    table_cache: RwLock<BTreeMap<String, Arc<dyn TableProvider>>>,
    function_cache: RwLock<BTreeMap<String, Arc<ScalarUDF>>>,
}

impl SelfSchemaProvider {
    /// Creates a provider from pre-built tables and UDFs.
    pub fn new(schema_name: String, tables: Vec<LogicalTable>, udfs: Vec<ScalarUDF>) -> Self {
        Self {
            schema_name,
            logical_tables: tables,
            udfs,
            table_cache: RwLock::new(Default::default()),
            function_cache: RwLock::new(Default::default()),
        }
    }

    /// Returns the UDFs held by this provider.
    pub fn udfs(&self) -> &[ScalarUDF] {
        &self.udfs
    }

    /// Creates a provider from manifest function definitions (no tables).
    ///
    /// Builds UDFs from all manifest functions.
    pub fn from_manifest_udfs(
        schema_name: String,
        isolate_pool: IsolatePool,
        manifest_udfs: &BTreeMap<FuncName, Function>,
    ) -> Self {
        let udfs: Vec<ScalarUDF> = manifest_udfs
            .iter()
            .map(|(func_name, func_def)| {
                AsyncScalarUDF::new(Arc::new(JsUdf::new(
                    isolate_pool.clone(),
                    Some(SELF_REF_KEYWORD.to_string()),
                    func_def.source.source.clone(),
                    func_def.source.filename.clone().into(),
                    Arc::from(func_name.as_str()),
                    func_def
                        .input_types
                        .iter()
                        .map(|dt| dt.clone().into_arrow())
                        .collect(),
                    func_def.output_type.clone().into_arrow(),
                )))
                .into_scalar_udf()
            })
            .collect();

        Self::new(schema_name, vec![], udfs)
    }
}

#[async_trait]
impl TableSchemaProvider for SelfSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.logical_tables
            .iter()
            .filter(|t| t.sql_schema_name() == self.schema_name)
            .map(|t| t.name().to_string())
            .collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        // Check cache first
        {
            let tables = self.table_cache.read();
            if let Some(table) = tables.get(name) {
                return Ok(Some(table.clone()));
            }
        }

        let table_name: TableName = name.parse().map_err(|err| {
            DataFusionError::Plan(format!("Invalid table name '{}': {}", name, err))
        })?;

        let Some(lt) = self
            .logical_tables
            .iter()
            .find(|t| t.sql_schema_name() == self.schema_name && t.name() == &table_name)
        else {
            return Ok(None);
        };

        let table_provider: Arc<dyn TableProvider> = Arc::new(PlanTable::new(lt.schema().clone()));
        self.table_cache
            .write()
            .insert(name.to_string(), table_provider.clone());
        Ok(Some(table_provider))
    }

    fn table_exist(&self, name: &str) -> bool {
        if self.table_cache.read().contains_key(name) {
            return true;
        }

        let Ok(table_name) = name.parse::<TableName>() else {
            return false;
        };

        self.logical_tables
            .iter()
            .any(|t| t.sql_schema_name() == self.schema_name && t.name() == &table_name)
    }
}

#[async_trait]
impl TableAsyncSchemaProvider for SelfSchemaProvider {
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        <Self as TableSchemaProvider>::table(self, name).await
    }
}

#[async_trait]
impl FuncSchemaProvider for SelfSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn function_names(&self) -> Vec<String> {
        let prefix = format!("{}.", self.schema_name);
        self.udfs
            .iter()
            .filter_map(|u| u.name().strip_prefix(&prefix).map(String::from))
            .collect()
    }

    async fn function(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn FunctionProvider>>, DataFusionError> {
        // Check cache first
        {
            let functions = self.function_cache.read();
            if let Some(func) = functions.get(name) {
                return Ok(Some(Arc::new(ScalarFunctionProvider::from(func.clone()))));
            }
        }

        // UDF names are stored as "schema.function" (e.g., "self.addSuffix").
        // Match by checking the schema-qualified prefix.
        let qualified_name = format!("{}.{}", self.schema_name, name);
        let Some(udf) = self.udfs.iter().find(|u| u.name() == qualified_name) else {
            return Ok(None);
        };

        let udf = Arc::new(udf.clone());
        self.function_cache
            .write()
            .insert(name.to_string(), udf.clone());
        Ok(Some(Arc::new(ScalarFunctionProvider::from(udf))))
    }

    fn function_exist(&self, name: &str) -> bool {
        if self.function_cache.read().contains_key(name) {
            return true;
        }
        let qualified_name = format!("{}.{}", self.schema_name, name);
        self.udfs.iter().any(|u| u.name() == qualified_name)
    }
}

#[async_trait]
impl FuncAsyncSchemaProvider for SelfSchemaProvider {
    async fn function(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn FunctionProvider>>, DataFusionError> {
        <Self as FuncSchemaProvider>::function(self, name).await
    }
}

impl std::fmt::Debug for SelfSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SelfSchemaProvider")
            .field("schema_name", &self.schema_name)
            .finish()
    }
}
