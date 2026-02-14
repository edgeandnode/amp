use std::{any::Any, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    common::{Result, exec_err},
    error::DataFusionError,
};

use super::function_provider::FunctionProvider;

/// Represents a schema, comprising a number of named functions.
///
/// Parallel to DataFusion's [`SchemaProvider`] but for functions instead of tables.
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
