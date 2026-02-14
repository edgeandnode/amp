use std::{any::Any, collections::BTreeMap, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use parking_lot::RwLock;

use super::catalog_provider::CatalogProvider as FuncCatalogProvider;

/// List of named function catalogs.
///
/// Parallel to DataFusion's table-side `CatalogProviderList`, but for function catalogs.
#[async_trait]
pub trait CatalogProviderList: Debug + Sync + Send {
    /// Returns the catalog list as [`Any`]
    /// so it can be downcast to specific implementations.
    fn as_any(&self) -> &dyn Any;

    /// Adds or replaces a catalog by name.
    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn FuncCatalogProvider>,
    ) -> Option<Arc<dyn FuncCatalogProvider>>;

    /// Returns all registered catalog names.
    fn catalog_names(&self) -> Vec<String>;

    /// Looks up a catalog by name.
    async fn catalog(&self, name: &str) -> Option<Arc<dyn FuncCatalogProvider>>;
}

/// In-memory function catalog list.
#[derive(Debug, Default)]
pub struct MemoryCatalogProviderList {
    catalogs: RwLock<BTreeMap<String, Arc<dyn FuncCatalogProvider>>>,
}

impl MemoryCatalogProviderList {
    /// Creates an empty catalog list.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl CatalogProviderList for MemoryCatalogProviderList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn FuncCatalogProvider>,
    ) -> Option<Arc<dyn FuncCatalogProvider>> {
        self.catalogs.write().insert(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        self.catalogs.read().keys().cloned().collect()
    }

    async fn catalog(&self, name: &str) -> Option<Arc<dyn FuncCatalogProvider>> {
        self.catalogs.read().get(name).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::func_catalog::schema_provider::SchemaProvider as FuncSchemaProvider;

    #[derive(Debug)]
    struct DummyCatalog {
        id: &'static str,
    }

    #[async_trait]
    impl FuncCatalogProvider for DummyCatalog {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema_names(&self) -> Vec<String> {
            Vec::new()
        }

        async fn schema(&self, _name: &str) -> Option<Arc<dyn FuncSchemaProvider>> {
            None
        }
    }

    #[test]
    fn register_catalog_with_new_name_returns_none() {
        //* Given
        let name = "amp".to_string();
        let catalog = Arc::new(DummyCatalog { id: "first" }) as Arc<dyn FuncCatalogProvider>;

        let list = MemoryCatalogProviderList::new();

        //* When
        let previous = list.register_catalog(name, catalog);

        //* Then
        assert!(
            previous.is_none(),
            "should return None when no catalog was previously registered"
        );
    }

    #[tokio::test]
    async fn register_catalog_with_existing_name_replaces_previous() {
        //* Given
        let name = "amp".to_string();

        let first_catalog = Arc::new(DummyCatalog { id: "first" }) as Arc<dyn FuncCatalogProvider>;
        let second_catalog =
            Arc::new(DummyCatalog { id: "second" }) as Arc<dyn FuncCatalogProvider>;

        let list = MemoryCatalogProviderList::new();
        list.register_catalog(name.clone(), first_catalog);

        //* When
        let replaced = list.register_catalog(name.clone(), second_catalog);

        //* Then
        assert!(
            replaced.is_some(),
            "should return previously registered catalog"
        );
        let current = list
            .catalog(&name)
            .await
            .expect("catalog should exist after replacement");
        let current = current
            .as_any()
            .downcast_ref::<DummyCatalog>()
            .expect("should downcast to DummyCatalog");
        assert_eq!(
            current.id, "second",
            "catalog should be the newly registered one"
        );
    }

    #[test]
    fn catalog_names_with_multiple_catalogs_returns_sorted_names() {
        //* Given
        let alpha_name = "alpha".to_string();
        let alpha_catalog = Arc::new(DummyCatalog { id: "a" }) as Arc<dyn FuncCatalogProvider>;
        let beta_name = "beta".to_string();
        let beta_catalog = Arc::new(DummyCatalog { id: "b" }) as Arc<dyn FuncCatalogProvider>;

        let list = MemoryCatalogProviderList::new();
        list.register_catalog(alpha_name, alpha_catalog);
        list.register_catalog(beta_name, beta_catalog);

        //* When
        let names = list.catalog_names();

        //* Then
        assert_eq!(
            names,
            vec!["alpha".to_string(), "beta".to_string()],
            "catalog names should be returned in sorted order"
        );
    }
}
