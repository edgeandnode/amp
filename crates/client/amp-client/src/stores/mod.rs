use common::metadata::segments::ResumeWatermark;

use crate::Error;

/// Trait for storing and retrieving resume watermarks.
///
/// Implementations track watermarks for queries to support resumption
/// after stream disconnections or application restarts.
pub trait ResumeStore: Send + Sync {
    /// Store a watermark for a given query.
    ///
    /// # Arguments
    /// * `id` - Unique identifier for the query
    /// * `watermark` - The watermark to store
    fn set_watermark(
        &mut self,
        id: &str,
        watermark: ResumeWatermark,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;

    /// Retrieve the most recent watermark for a given query.
    ///
    /// # Arguments
    /// * `id` - Unique identifier for the query
    ///
    /// # Returns
    /// The most recent watermark, or None if no watermark exists
    fn get_watermark(
        &self,
        id: &str,
    ) -> impl std::future::Future<Output = Result<Option<ResumeWatermark>, Error>> + Send;
}

mod memory;

pub use memory::InMemoryResumeStore;
