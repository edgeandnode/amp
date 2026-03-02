use std::{any::Any, collections::HashMap, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    common::{Result, not_impl_err},
    error::DataFusionError,
    execution::config::SessionConfig,
};

use super::schema_provider::{
    AsyncSchemaProvider as FuncAsyncSchemaProvider, FuncReference, ResolvedSchemaProviderBuilder,
    SchemaProvider as FuncSchemaProvider,
};

/// Represents a catalog, comprising a number of named function schemas.
///
/// Parallel to DataFusion's [`CatalogProvider`] but for functions instead of tables.
/// This is the cached/resolved trait used during query planning.
///
/// [`CatalogProvider`]: datafusion::catalog::CatalogProvider
pub trait CatalogProvider: Debug + Sync + Send {
    /// Returns this `CatalogProvider` as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Returns the list of available schema names in this catalog.
    fn schema_names(&self) -> Vec<String>;

    /// Retrieves a specific schema from the catalog by name, if it exists.
    fn schema(&self, name: &str) -> Option<Arc<dyn FuncSchemaProvider>>;

    /// Adds a new schema to this catalog.
    ///
    /// If a schema of the same name existed before, it is replaced in
    /// the catalog and returned.
    ///
    /// By default returns a "Not Implemented" error.
    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn FuncSchemaProvider>,
    ) -> Result<Option<Arc<dyn FuncSchemaProvider>>> {
        let _ = name;
        let _ = schema;
        not_impl_err!("Registering new schemas is not supported")
    }

    /// Removes a schema from this catalog.
    ///
    /// Implementations of this method should return errors if the schema exists
    /// but cannot be dropped. When `cascade` is false, non-empty schemas should
    /// be rejected.
    ///
    /// Implementations of this method should return None if schema with `name`
    /// does not exist.
    ///
    /// By default returns a "Not Implemented" error.
    fn deregister_schema(
        &self,
        _name: &str,
        _cascade: bool,
    ) -> Result<Option<Arc<dyn FuncSchemaProvider>>> {
        not_impl_err!("Deregistering schemas is not supported")
    }
}

/// Resolves function schemas lazily before query planning.
///
/// Parallel to DataFusion's `AsyncCatalogProvider` but for functions instead of tables.
/// Implementors provide lazy resolution that can perform I/O; the `resolve` method
/// caches matching schemas into a [`CatalogProvider`] for use during planning.
#[async_trait]
pub trait AsyncCatalogProvider: Debug + Send + Sync {
    /// Retrieves a specific async schema from the catalog by name, if it exists.
    async fn schema(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn FuncAsyncSchemaProvider>>, DataFusionError>;

    /// Resolves the referenced functions and returns a cached [`CatalogProvider`].
    ///
    /// The default implementation resolves each schema referenced and resolves
    /// the individual function references within each schema.
    async fn resolve(
        &self,
        references: &[FuncReference],
        config: &SessionConfig,
        catalog_name: &str,
    ) -> Result<Arc<dyn CatalogProvider>, DataFusionError> {
        let mut cached_schemas = HashMap::<String, Option<ResolvedSchemaProviderBuilder>>::new();

        for reference in references {
            let ref_catalog_name = reference
                .catalog()
                .unwrap_or(&config.options().catalog.default_catalog);

            if ref_catalog_name != catalog_name {
                continue;
            }

            let schema_name = reference
                .schema()
                .unwrap_or(&config.options().catalog.default_schema);

            let schema = if let Some(schema) = cached_schemas.get_mut(schema_name) {
                schema
            } else {
                let resolved_schema = self.schema(schema_name).await?;
                let resolved_schema = resolved_schema.map(ResolvedSchemaProviderBuilder::new);
                cached_schemas.insert(schema_name.to_string(), resolved_schema);
                let Some(schema) = cached_schemas.get_mut(schema_name) else {
                    continue;
                };
                schema
            };

            let Some(schema) = schema.as_mut() else {
                continue;
            };

            schema.resolve_function(reference.table()).await?;
        }

        let cached_schemas = cached_schemas
            .into_iter()
            .filter_map(|(key, maybe_builder)| {
                maybe_builder.map(|schema_builder| (key, schema_builder.finish()))
            })
            .collect::<HashMap<_, _>>();

        Ok(Arc::new(ResolvedCatalogProvider {
            schemas: cached_schemas,
        }))
    }
}

/// A [`CatalogProvider`] backed by a pre-resolved set of schemas.
///
/// Created by [`AsyncCatalogProvider::resolve`] to cache schemas for use
/// during query planning.
#[derive(Debug)]
pub struct ResolvedCatalogProvider {
    schemas: HashMap<String, Arc<dyn FuncSchemaProvider>>,
}

impl CatalogProvider for ResolvedCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn FuncSchemaProvider>> {
        self.schemas.get(name).cloned()
    }
}
