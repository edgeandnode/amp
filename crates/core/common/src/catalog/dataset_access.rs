use std::{future::Future, sync::Arc};

use datafusion::logical_expr::ScalarUDF;
use datasets_common::partial_reference::PartialReference;

use crate::{BoxError, Dataset};

/// Minimal trait for accessing datasets and provider configuration.
///
/// This trait provides the minimal interface required for SQL catalog building,
/// abstracting over the dataset store implementation.
pub trait DatasetAccess {
    /// Get a dataset by its reference (namespace/name@version).
    fn get_dataset(
        &self,
        reference: impl Into<PartialReference> + Send,
    ) -> impl Future<Output = Result<Arc<Dataset>, BoxError>> + Send;

    /// Create an eth_call UDF for the given dataset if applicable.
    ///
    /// Returns `None` if the dataset kind doesn't support eth_call (i.e., not EVM RPC).
    fn eth_call_for_dataset(
        &self,
        catalog_schema: &str,
        dataset: &Dataset,
    ) -> impl Future<Output = Result<Option<ScalarUDF>, BoxError>> + Send;
}
