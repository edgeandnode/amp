use std::{future::Future, sync::Arc};

use datafusion::logical_expr::ScalarUDF;
use datasets_common::{hash::Hash, reference::Reference};

use crate::{BoxError, Dataset};

/// Minimal trait for accessing datasets and provider configuration.
///
/// This trait provides the minimal interface required for SQL catalog building,
/// abstracting over the dataset store implementation.
pub trait DatasetAccess {
    /// Resolve a dataset reference to its content hash.
    ///
    /// This method resolves a dataset reference (which may contain a version, "latest", etc.)
    /// to the actual content hash of the dataset manifest.
    fn resolve_dataset_reference(
        &self,
        reference: impl AsRef<Reference> + Send,
    ) -> impl Future<Output = Result<Option<Hash>, BoxError>> + Send;

    /// Get a dataset by its content hash.
    ///
    /// This is more efficient than `get_dataset` when you already have the hash,
    /// as it bypasses reference resolution.
    fn get_dataset_by_hash(
        &self,
        hash: &Hash,
    ) -> impl Future<Output = Result<Option<Arc<Dataset>>, BoxError>> + Send;

    /// Create an eth_call UDF for the given dataset if applicable.
    ///
    /// Returns `None` if the dataset kind doesn't support eth_call (i.e., not EVM RPC).
    fn eth_call_for_dataset(
        &self,
        catalog_schema: &str,
        dataset: &Dataset,
    ) -> impl Future<Output = Result<Option<ScalarUDF>, BoxError>> + Send;
}
