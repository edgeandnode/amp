use std::{any::Any, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use datafusion::common::{Result, not_impl_err};

use super::schema_provider::SchemaProvider as FuncSchemaProvider;

/// Represents a catalog, comprising a number of named function schemas.
///
/// Parallel to DataFusion's [`CatalogProvider`] but for functions instead of tables.
///
/// [`CatalogProvider`]: datafusion::catalog::CatalogProvider
#[async_trait]
pub trait CatalogProvider: Debug + Sync + Send {
    /// Returns the catalog provider as [`Any`]
    /// so that it can be downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Retrieves the list of available schema names in this catalog.
    fn schema_names(&self) -> Vec<String>;

    /// Retrieves a specific schema from the catalog by name, provided it exists.
    async fn schema(&self, name: &str) -> Option<Arc<dyn FuncSchemaProvider>>;

    /// Adds a new schema to this catalog.
    ///
    /// If a schema of the same name existed before, it is replaced in
    /// the catalog and returned.
    ///
    /// By default returns a "Not Implemented" error
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
    /// By default returns a "Not Implemented" error
    fn deregister_schema(
        &self,
        _name: &str,
        _cascade: bool,
    ) -> Result<Option<Arc<dyn FuncSchemaProvider>>> {
        not_impl_err!("Deregistering new schemas is not supported")
    }
}
