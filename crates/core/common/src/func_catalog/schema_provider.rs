use std::{any::Any, collections::HashMap, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    common::{Result, TableReference, exec_err},
    error::DataFusionError,
    execution::config::SessionConfig,
};

use super::function_provider::FunctionProvider;

/// Type alias for function references, mirroring DataFusion's [`TableReference`].
pub type FuncReference = TableReference;

/// Represents a schema, comprising a number of named functions.
///
/// Parallel to DataFusion's [`SchemaProvider`] but for functions instead of tables.
/// This is the cached/resolved trait used during query planning.
///
/// Note that [`SchemaProvider::function`] is `async` in order to simplify
/// implementing providers where resolving a function requires I/O (e.g., creating
/// an RPC client for `eth_call`).
///
/// [`SchemaProvider`]: datafusion::catalog::SchemaProvider
#[async_trait]
pub trait SchemaProvider: Debug + Sync + Send {
    /// Returns the owner of the schema, default is None.
    fn owner_name(&self) -> Option<&str> {
        None
    }

    /// Returns this `SchemaProvider` as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Retrieves the list of available function names in this schema.
    fn function_names(&self) -> Vec<String>;

    /// Retrieves a specific function from the schema by name, if it exists,
    /// otherwise returns `None`.
    async fn function(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn FunctionProvider>>, DataFusionError>;

    /// If supported by the implementation, adds a new function named `name` to
    /// this schema.
    ///
    /// If a function of the same name was already registered, returns "Function
    /// already exists" error.
    fn register_function(
        &self,
        _name: String,
        _function: Arc<dyn FunctionProvider>,
    ) -> Result<Option<Arc<dyn FunctionProvider>>> {
        exec_err!("schema provider does not support registering functions")
    }

    /// If supported by the implementation, removes the `name` function from this
    /// schema and returns the previously registered [`FunctionProvider`], if any.
    ///
    /// If no `name` function exists, returns Ok(None).
    fn deregister_function(&self, _name: &str) -> Result<Option<Arc<dyn FunctionProvider>>> {
        exec_err!("schema provider does not support deregistering functions")
    }

    /// Returns true if a function exists in this schema provider, false otherwise.
    fn function_exist(&self, name: &str) -> bool;
}

/// Resolves functions lazily before query planning.
///
/// Parallel to DataFusion's `AsyncSchemaProvider` but for functions instead of tables.
/// Implementors provide lazy resolution that can perform I/O; the `resolve` method
/// caches matching functions into a [`SchemaProvider`] for use during planning.
#[async_trait]
pub trait AsyncSchemaProvider: Send + Sync {
    /// Retrieves a specific function from the schema by name, if it exists.
    async fn function(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn FunctionProvider>>, DataFusionError>;

    /// Resolves the referenced functions and returns a cached [`SchemaProvider`].
    ///
    /// The default implementation resolves each matching function reference by
    /// calling [`function`](Self::function) and caches the results.
    async fn resolve(
        &self,
        references: &[FuncReference],
        config: &SessionConfig,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<Arc<dyn SchemaProvider>, DataFusionError> {
        let mut cached_functions = HashMap::<String, Option<Arc<dyn FunctionProvider>>>::new();

        for reference in references {
            let ref_catalog_name = reference
                .catalog()
                .unwrap_or(&config.options().catalog.default_catalog);

            if ref_catalog_name != catalog_name {
                continue;
            }

            let ref_schema_name = reference
                .schema()
                .unwrap_or(&config.options().catalog.default_schema);

            if ref_schema_name != schema_name {
                continue;
            }

            if !cached_functions.contains_key(reference.table()) {
                let resolved = self.function(reference.table()).await?;
                cached_functions.insert(reference.table().to_string(), resolved);
            }
        }

        let functions = cached_functions
            .into_iter()
            .filter_map(|(key, maybe_value)| maybe_value.map(|value| (key, value)))
            .collect();

        Ok(Arc::new(ResolvedSchemaProvider { functions }))
    }
}

/// A [`SchemaProvider`] backed by a pre-resolved set of functions.
///
/// Created by [`AsyncSchemaProvider::resolve`] to cache functions for use
/// during query planning.
#[derive(Debug)]
pub struct ResolvedSchemaProvider {
    functions: HashMap<String, Arc<dyn FunctionProvider>>,
}

#[async_trait]
impl SchemaProvider for ResolvedSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn function_names(&self) -> Vec<String> {
        self.functions.keys().cloned().collect()
    }

    async fn function(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn FunctionProvider>>, DataFusionError> {
        Ok(self.functions.get(name).cloned())
    }

    fn function_exist(&self, name: &str) -> bool {
        self.functions.contains_key(name)
    }
}

/// Builder for [`ResolvedSchemaProvider`].
///
/// Used internally by [`AsyncCatalogProvider::resolve`] to resolve and cache
/// functions for a single schema before building the provider.
pub(crate) struct ResolvedSchemaProviderBuilder {
    async_provider: Arc<dyn AsyncSchemaProvider>,
    cached_functions: HashMap<String, Option<Arc<dyn FunctionProvider>>>,
}

impl ResolvedSchemaProviderBuilder {
    /// Creates a new builder wrapping the given async schema provider.
    pub(crate) fn new(async_provider: Arc<dyn AsyncSchemaProvider>) -> Self {
        Self {
            async_provider,
            cached_functions: HashMap::new(),
        }
    }

    /// Resolves and caches the named function from the async provider, skipping if already cached.
    pub(crate) async fn resolve_function(&mut self, name: &str) -> Result<(), DataFusionError> {
        if !self.cached_functions.contains_key(name) {
            let resolved = self.async_provider.function(name).await?;
            self.cached_functions.insert(name.to_string(), resolved);
        }
        Ok(())
    }

    /// Consumes the builder and returns a [`ResolvedSchemaProvider`] containing all resolved functions.
    pub(crate) fn finish(self) -> Arc<dyn SchemaProvider> {
        let functions = self
            .cached_functions
            .into_iter()
            .filter_map(|(key, maybe_value)| maybe_value.map(|value| (key, value)))
            .collect();
        Arc::new(ResolvedSchemaProvider { functions })
    }
}
