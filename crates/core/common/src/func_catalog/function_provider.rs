use std::{any::Any, fmt::Debug, sync::Arc};

use datafusion::logical_expr::ScalarUDF;

/// Leaf-level trait for function resolution in the catalog hierarchy.
///
/// Parallels DataFusion's [`TableProvider`] but for functions. The catalog hierarchy is:
/// ```text
/// CatalogProviderList → CatalogProvider → SchemaProvider → Arc<dyn FunctionProvider>
/// ```
///
/// [`TableProvider`]: datafusion::catalog::TableProvider
pub trait FunctionProvider: Debug + Sync + Send {
    /// Returns this provider as [`Any`] so it can be downcast to a concrete type.
    fn as_any(&self) -> &dyn Any;

    /// Returns the [`ScalarUDF`] backing this function provider.
    fn scalar_udf(&self) -> Arc<ScalarUDF>;
}

/// Default [`FunctionProvider`] wrapping a [`ScalarUDF`].
///
/// Used for the common case where no additional metadata or behavior is needed
/// beyond the UDF itself.
#[derive(Debug, Clone)]
pub struct ScalarFunctionProvider {
    udf: Arc<ScalarUDF>,
}

impl FunctionProvider for ScalarFunctionProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn scalar_udf(&self) -> Arc<ScalarUDF> {
        self.udf.clone()
    }
}

impl From<Arc<ScalarUDF>> for ScalarFunctionProvider {
    fn from(udf: Arc<ScalarUDF>) -> Self {
        Self { udf }
    }
}
