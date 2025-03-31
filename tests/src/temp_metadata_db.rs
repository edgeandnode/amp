use std::ops::Deref;

use log::info;
use metadata_db::MetadataDb;
use pgtemp::{PgTempDB, PgTempDBBuilder};
use tokio::sync::OnceCell;

pub struct TempMetadataDb {
    metadata_db: MetadataDb,

    // Handle to keep the DB alive.
    _temp_db: PgTempDB,
}

impl TempMetadataDb {
    pub async fn new() -> Self {
        let builder = PgTempDBBuilder::new();
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

pub async fn test_metadata_db() -> &'static TempMetadataDb {
    TEST_METADATA_DB.get_or_init(TempMetadataDb::new).await
}
