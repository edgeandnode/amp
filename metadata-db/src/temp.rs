use std::sync::LazyLock;

use pgtemp::{PgTempDB, PgTempDBBuilder};
use tokio::sync::OnceCell;

use crate::MetadataDb;

/// Whether to keep the temporary directory after the Metadata DB is dropped
///
/// This is set to `false` by default, but can be overridden by the `KEEP_TEMP_DIRS` environment
/// variable.
pub static KEEP_TEMP_DIRS: LazyLock<bool> = LazyLock::new(|| {
    std::env::var("KEEP_TEMP_DIRS")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false)
});

/// Temporary Metadata DB
///
/// This is a wrapper around the Metadata DB that creates a temporary database.
/// On drop, the database is deleted.
pub struct TempMetadataDb {
    /// Inner Metadata DB handle
    inner: MetadataDb,

    /// Temporary database handle
    ///
    /// On drop, the database is deleted.
    _temp_db: PgTempDB,
}

impl TempMetadataDb {
    /// Create a new temporary Metadata DB
    pub async fn new(keep: bool, pool_size: u32) -> Self {
        // Set C locale. To remove this `unsafe` we need:
        // https://github.com/boustrophedon/pgtemp/pull/21
        unsafe {
            std::env::set_var("LANG", "C");
        }

        let builder = PgTempDBBuilder::new().persist_data(keep);
        let pg_temp = PgTempDB::from_builder(builder);

        let data_dir = pg_temp.data_dir();
        tracing::info!("initializing temp metadata-db at: {}", data_dir.display());
        let uri = pg_temp.connection_uri();
        tracing::info!("connecting to metadata-db at: {}", uri);

        let metadata_db = MetadataDb::connect(&uri, pool_size)
            .await
            .expect("failed to connect to metadata-db");

        TempMetadataDb {
            inner: metadata_db,
            _temp_db: pg_temp,
        }
    }

    /// Get the URL of the temporary Metadata DB
    pub fn url(&self) -> &str {
        self.inner.url.as_ref()
    }
}

impl std::ops::Deref for TempMetadataDb {
    type Target = MetadataDb;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Temp metadata db for sharing among tests. It is shared with the reasoning that this helps us
/// catch more bugs, even if it is less deterministic.
static TEMP_METADATA_DB: OnceCell<TempMetadataDb> = OnceCell::const_new();

/// Get the temporary Metadata DB
///
/// This is a shared instance of the temporary Metadata DB that can be used by tests.
///
/// The `keep` parameter controls whether the temporary directory is kept after the Metadata DB is
/// dropped.
pub async fn temp_metadata_db(keep: bool, pool_size: u32) -> &'static TempMetadataDb {
    TEMP_METADATA_DB
        .get_or_init(|| async { TempMetadataDb::new(keep, pool_size).await })
        .await
}
