use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use common::metadata::segments::ResumeWatermark;

use super::ResumeStore;
use crate::Error;

/// In-memory implementation of ResumeStore.
///
/// Suitable for single-process applications but does not persist across restarts.
#[derive(Clone)]
pub struct InMemoryResumeStore {
    /// Stored watermarks, keyed by query ID
    watermarks: Arc<Mutex<HashMap<String, ResumeWatermark>>>,
}

impl InMemoryResumeStore {
    /// Create a new in-memory resume store.
    pub fn new() -> Self {
        Self {
            watermarks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Get the current number of watermarks stored in memory.
    pub fn len(&self) -> usize {
        self.watermarks.lock().unwrap().len()
    }

    /// Check if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.watermarks.lock().unwrap().is_empty()
    }

    /// Clear all stored watermarks.
    pub fn clear(&self) {
        self.watermarks.lock().unwrap().clear();
    }
}

impl Default for InMemoryResumeStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ResumeStore for InMemoryResumeStore {
    fn set_watermark(
        &mut self,
        id: &str,
        watermark: ResumeWatermark,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send {
        self.watermarks
            .lock()
            .unwrap()
            .insert(id.to_string(), watermark);
        async { Ok(()) }
    }

    fn get_watermark(
        &self,
        id: &str,
    ) -> impl std::future::Future<Output = Result<Option<ResumeWatermark>, Error>> + Send {
        let result = self.watermarks.lock().unwrap().get(id).cloned();
        async move { Ok(result) }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use common::metadata::segments::Watermark;

    use super::*;

    fn create_test_watermark(block_num: u64) -> ResumeWatermark {
        let mut map = BTreeMap::new();
        map.insert(
            "test_network".to_string(),
            Watermark {
                number: block_num,
                hash: [0u8; 32].into(),
            },
        );
        ResumeWatermark(map)
    }

    #[tokio::test]
    async fn store_and_retrieve_watermark() {
        //* Given
        let mut store = InMemoryResumeStore::new();
        let id = "test_query";
        let watermark = create_test_watermark(100);

        //* When
        store
            .set_watermark(id, watermark.clone())
            .await
            .expect("store should succeed");

        //* Then
        assert_eq!(store.len(), 1);
        assert!(!store.is_empty());

        let retrieved = store.get_watermark(id).await.expect("get should succeed");
        assert_eq!(retrieved, Some(watermark));
    }

    #[tokio::test]
    async fn retrieve_nonexistent_watermark() {
        //* Given
        let store = InMemoryResumeStore::new();

        //* When
        let retrieved = store
            .get_watermark("nonexistent")
            .await
            .expect("get should succeed");

        //* Then
        assert_eq!(retrieved, None);
    }

    #[tokio::test]
    async fn overwrite_existing_watermark() {
        //* Given
        let mut store = InMemoryResumeStore::new();
        let id = "test_query";
        let watermark1 = create_test_watermark(100);
        let watermark2 = create_test_watermark(200);

        //* When
        store
            .set_watermark(id, watermark1)
            .await
            .expect("first store should succeed");
        store
            .set_watermark(id, watermark2.clone())
            .await
            .expect("second store should succeed");

        //* Then
        assert_eq!(store.len(), 1);
        let retrieved = store.get_watermark(id).await.expect("get should succeed");
        assert_eq!(retrieved, Some(watermark2));
    }

    #[tokio::test]
    async fn multiple_queries() {
        //* Given
        let mut store = InMemoryResumeStore::new();
        let query1 = "query_1";
        let query2 = "query_2";
        let watermark1 = create_test_watermark(100);
        let watermark2 = create_test_watermark(200);

        //* When
        store
            .set_watermark(query1, watermark1.clone())
            .await
            .expect("store 1 should succeed");
        store
            .set_watermark(query2, watermark2.clone())
            .await
            .expect("store 2 should succeed");

        //* Then
        assert_eq!(store.len(), 2);

        let retrieved1 = store
            .get_watermark(query1)
            .await
            .expect("get 1 should succeed");
        assert_eq!(retrieved1, Some(watermark1));

        let retrieved2 = store
            .get_watermark(query2)
            .await
            .expect("get 2 should succeed");
        assert_eq!(retrieved2, Some(watermark2));
    }

    #[tokio::test]
    async fn clear_all_watermarks() {
        //* Given
        let mut store = InMemoryResumeStore::new();
        store
            .set_watermark("query_1", create_test_watermark(100))
            .await
            .expect("store should succeed");
        store
            .set_watermark("query_2", create_test_watermark(200))
            .await
            .expect("store should succeed");

        //* When
        store.clear();

        //* Then
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
    }
}
