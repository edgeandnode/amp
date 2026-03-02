//! Catalog provider for datasets.
//!
//! Resolves datasets at the catalog level and creates
//! [`DatasetSchemaProvider`] instances for schema-only table access.

use std::{any::Any, collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::{
        AsyncCatalogProvider as TableAsyncCatalogProvider,
        AsyncSchemaProvider as TableAsyncSchemaProvider, CatalogProvider as TableCatalogProvider,
        SchemaProvider as TableSchemaProvider,
    },
    error::DataFusionError,
};
use datasets_common::{
    hash_reference::HashReference, partial_reference::PartialReference, reference::Reference,
};
use datasets_derived::deps::SELF_REF_KEYWORD;
use js_runtime::isolate_pool::IsolatePool;

use crate::{
    dataset_schema_provider::DatasetSchemaProvider,
    dataset_store::DatasetStore,
    func_catalog::{
        catalog_provider::{
            AsyncCatalogProvider as FuncAsyncCatalogProvider,
            CatalogProvider as FuncCatalogProvider,
        },
        schema_provider::{
            AsyncSchemaProvider as FuncAsyncSchemaProvider, SchemaProvider as FuncSchemaProvider,
        },
    },
};

/// Combined async schema provider for both tables and functions.
///
/// Blanket-implemented for any type that implements both
/// [`TableAsyncSchemaProvider`] and [`FuncAsyncSchemaProvider`].
pub trait AsyncSchemaProvider: TableAsyncSchemaProvider + FuncAsyncSchemaProvider {}
impl<T: TableAsyncSchemaProvider + FuncAsyncSchemaProvider> AsyncSchemaProvider for T {}

/// The catalog name used to register Amp dataset providers.
pub const AMP_CATALOG_NAME: &str = "amp";

/// Catalog provider for datasets.
///
/// Resolves datasets and creates [`DatasetSchemaProvider`] instances
/// that provide schema information without requiring a data store.
#[derive(Clone)]
pub struct AmpCatalogProvider {
    store: DatasetStore,
    isolate_pool: IsolatePool,
    /// Optional dependency alias overrides. When set, bare names matching
    /// a key are resolved directly to the corresponding [`HashReference`]
    /// instead of going through `PartialReference` → `Reference` → `resolve_revision`.
    dep_aliases: BTreeMap<String, HashReference>,
    /// Optional self-schema provider for the `"self"` virtual schema.
    /// Checked before store-based resolution in [`resolve_schema`].
    self_schema: Option<Arc<dyn AsyncSchemaProvider>>,
}

impl AmpCatalogProvider {
    /// Creates a new catalog provider.
    pub fn new(store: DatasetStore, isolate_pool: IsolatePool) -> Self {
        Self {
            store,
            isolate_pool,
            dep_aliases: Default::default(),
            self_schema: None,
        }
    }

    /// Sets the dependency alias map for direct resolution of bare names.
    pub fn with_dep_aliases(mut self, dep_aliases: BTreeMap<String, HashReference>) -> Self {
        self.dep_aliases = dep_aliases;
        self
    }

    /// Sets the self-schema provider for the `"self"` virtual schema.
    ///
    /// The self-schema is checked before store-based resolution in
    /// [`resolve_schema`]. It doesn't correspond to a dataset in the store
    /// but still needs to resolve tables and functions (e.g., during dump).
    pub fn with_self_schema(mut self, provider: Arc<dyn AsyncSchemaProvider>) -> Self {
        self.self_schema = Some(provider);
        self
    }

    /// Resolves a schema name to an [`AsyncSchemaProvider`].
    ///
    /// Resolution order:
    /// 1. Self-schema provider (for the `"self"` virtual schema)
    /// 2. Dep alias overrides (pinned hash, no store resolution)
    /// 3. Store lookup via `PartialReference` → `Reference` → `resolve_revision`
    ///
    /// Returns `Ok(None)` when the name doesn't match any provider.
    /// Only actual I/O or storage errors produce `Err(...)`.
    pub(crate) async fn resolve_schema(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn AsyncSchemaProvider>>, DataFusionError> {
        // 1. Self-schema (e.g., "self" for self fn/table refs during dump).
        if name == SELF_REF_KEYWORD
            && let Some(provider) = &self.self_schema
        {
            return Ok(Some(provider.clone()));
        }

        // 2. Dep alias overrides — pinned hash, no store resolution needed.
        if let Some(hash_ref) = self.dep_aliases.get(name) {
            let dataset = self
                .store
                .get_dataset(hash_ref)
                .await
                .map_err(|err| DataFusionError::External(Box::new(err)))?;

            let provider: Arc<dyn AsyncSchemaProvider> = Arc::new(DatasetSchemaProvider::new(
                name.to_string(),
                dataset,
                self.store.clone(),
                self.isolate_pool.clone(),
            ));
            return Ok(Some(provider));
        }

        // 3. Store lookup via PartialReference → Reference → resolve_revision.
        let Ok(partial_ref) = name.parse::<PartialReference>() else {
            return Ok(None);
        };

        let reference: Reference = partial_ref.into();

        let Some(hash_ref) = self
            .store
            .resolve_revision(&reference)
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?
        else {
            return Ok(None);
        };

        let dataset = self
            .store
            .get_dataset(&hash_ref)
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;

        let provider: Arc<dyn AsyncSchemaProvider> = Arc::new(DatasetSchemaProvider::new(
            name.to_string(),
            dataset,
            self.store.clone(),
            self.isolate_pool.clone(),
        ));
        Ok(Some(provider))
    }
}

impl std::fmt::Debug for AmpCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AmpCatalogProvider").finish()
    }
}

impl TableCatalogProvider for AmpCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec![]
    }

    fn schema(&self, _name: &str) -> Option<Arc<dyn TableSchemaProvider>> {
        None
    }
}

#[async_trait]
impl TableAsyncCatalogProvider for AmpCatalogProvider {
    async fn schema(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableAsyncSchemaProvider>>, DataFusionError> {
        let schema = self.resolve_schema(name).await?;
        Ok(schema.map(|s| s as _))
    }
}

impl FuncCatalogProvider for AmpCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec![]
    }

    fn schema(&self, _name: &str) -> Option<Arc<dyn FuncSchemaProvider>> {
        None
    }
}

#[async_trait]
impl FuncAsyncCatalogProvider for AmpCatalogProvider {
    async fn schema(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn FuncAsyncSchemaProvider>>, DataFusionError> {
        let schema = self.resolve_schema(name).await?;
        Ok(schema.map(|s| s as _))
    }
}
