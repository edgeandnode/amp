use std::{future::Future, sync::Arc};

use datafusion::logical_expr::ScalarUDF;
use datasets_common::{hash_reference::HashReference, reference::Reference};

use crate::{BoxError, Dataset};

/// Minimal trait for accessing datasets and provider configuration.
///
/// This trait provides the minimal interface required for SQL catalog building,
/// abstracting over the dataset store implementation.
pub trait DatasetAccess {
    /// Resolve a dataset reference to a hash reference.
    ///
    /// This method resolves a dataset reference (which may contain a version, "latest", etc.)
    /// to a hash reference containing the fully qualified name and manifest hash.
    ///
    /// Returns `Ok(None)` if the dataset does not exist, or `Err` if resolution fails.
    fn resolve_revision(
        &self,
        reference: impl AsRef<Reference> + Send,
    ) -> impl Future<Output = Result<Option<HashReference>, BoxError>> + Send;

    /// Get a dataset by hash reference.
    ///
    /// This method loads a dataset using a hash reference (which contains both the
    /// fully qualified name and the manifest hash). Returns an error if the dataset
    /// does not exist or if loading fails.
    fn get_dataset(
        &self,
        reference: &HashReference,
    ) -> impl Future<Output = Result<Arc<Dataset>, BoxError>> + Send;

    /// Create an eth_call UDF for the given dataset if applicable.
    ///
    /// Returns `None` if the dataset kind doesn't support eth_call (i.e., not EVM RPC).
    fn eth_call_for_dataset(
        &self,
        sql_table_ref_schema: &str,
        dataset: &Dataset,
    ) -> impl Future<Output = Result<Option<ScalarUDF>, BoxError>> + Send;
}
