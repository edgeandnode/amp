use std::{ops::Deref, sync::LazyLock};

use pgtemp::{PgTempDB, PgTempDBBuilder};
use tokio::sync::OnceCell;
use tracing::info;

use crate::MetadataDb;

pub static ALLOW_TEMP_DB: LazyLock<bool> = LazyLock::new(|| {
    std::env::var("ALLOW_TEMP_DB")
        .map(|v| v.to_lowercase() == "true" || v == "1")
        .unwrap_or(true)
});

pub static KEEP_TEMP_DIRS: LazyLock<bool> =
    LazyLock::new(|| std::env::var("KEEP_TEMP_DIRS").is_ok());

pub struct TempMetadataDb {
    metadata_db: MetadataDb,

    // Handle to keep the DB alive.
    _temp_db: PgTempDB,
}

impl TempMetadataDb {
    pub async fn new(keep: bool) -> Self {
        // Set C locale
        std::env::set_var("LANG", "C");

        let builder = PgTempDBBuilder::new().persist_data(keep);
        let pg_temp = PgTempDB::from_builder(builder);
        info!(
            "initializing temp metadata db at: {}",
            pg_temp.data_dir().display()
        );

        let uri = pg_temp.connection_uri();

        info!("connecting to metadata db at: {}", uri);

        let metadata_db = MetadataDb::connect(&uri).await.unwrap();

        TempMetadataDb {
            metadata_db,
            _temp_db: pg_temp,
        }
    }

    pub fn url(&self) -> &str {
        self.metadata_db.url.as_str()
    }
}

impl Deref for TempMetadataDb {
    type Target = MetadataDb;

    fn deref(&self) -> &Self::Target {
        &self.metadata_db
    }
}

/// Temp metadata db for sharing among tests. It is shared with the reasoning that this helps us
/// catch more bugs, even if it is less deterministic.
static TEST_METADATA_DB: OnceCell<TempMetadataDb> = OnceCell::const_new();

pub async fn test_metadata_db(keep: bool) -> &'static TempMetadataDb {
    TEST_METADATA_DB
        .get_or_init(|| async { TempMetadataDb::new(keep).await })
        .await
}
