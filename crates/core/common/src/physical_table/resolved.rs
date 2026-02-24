use metadata_db::files::FileId;
use object_store::ObjectMeta;

/// A file resolved for query execution. Contains everything the execution
/// layer needs without exposing segment/chain/block-range concepts.
#[derive(Debug, Clone)]
pub struct ResolvedFile {
    /// File identifier for metadata cache lookups.
    pub id: FileId,
    /// Object store metadata (location, size, last_modified).
    pub object: ObjectMeta,
}
