use super::logical::Table;

use std::sync::Arc;

use fs_err as fs;
use object_store::{
    aws::AmazonS3Builder, gcp::GoogleCloudStorageBuilder, local::LocalFileSystem, ObjectStore,
};
use url::Url;

use crate::{BoxError, Dataset};

pub struct Catalog {
    url: Url,
    object_store: Arc<dyn ObjectStore>,
    tables: Vec<PhysicalTable>,
}

#[derive(Debug, Clone)]
pub struct PhysicalTable {
    pub table: Table,

    // URL in a format understood by the object store.
    pub url: Url,
}

impl PhysicalTable {
    pub fn name(&self) -> &str {
        &self.table.name
    }

    fn resolve(base: &Url, table: &Table) -> Result<Self, BoxError> {
        let url = if base.scheme() == "file" {
            Url::from_directory_path(&format!("/{}/", &table.name)).unwrap()
        } else {
            base.join(&format!("{}/", &table.name))?
        };
        Ok(PhysicalTable {
            table: table.clone(),
            url,
        })
    }
}

impl Catalog {
    /// To obtain `url` and `object_store`, call `infer_object_store` on a path.
    ///
    /// The table names will be resolved as subdirectories of `url`.
    pub fn new(
        tables: &[Table],
        url: Url,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self, BoxError> {
        let table_urls = tables
            .iter()
            .map(|table| PhysicalTable::resolve(&url, table))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Catalog {
            url,
            object_store,
            tables: table_urls,
        })
    }

    /// Will include meta tables.
    pub fn for_dataset(dataset: &Dataset, data_location: String) -> Result<Self, BoxError> {
        let tables = {
            let mut tables = dataset.tables().to_vec();
            tables.append(&mut dataset.meta_tables());
            tables
        };
        let (url, object_store) = infer_object_store(data_location.clone())?;
        Self::new(&tables, url, object_store)
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.object_store.clone()
    }

    /// All tables in the catalog, except meta tables.
    pub fn tables(&self) -> impl Iterator<Item = &PhysicalTable> {
        self.tables.iter().filter(|table| !table.table.is_meta())
    }

    pub fn meta_tables(&self) -> impl Iterator<Item = &PhysicalTable> {
        self.tables.iter().filter(|table| table.table.is_meta())
    }
}

/// Examples of valid formats for `data_location`:
/// - Filesystem path: `relative/path/to/data/`
/// - GCS: `gs://bucket-name/`
/// - S3: `s3://bucket-name/`
fn infer_object_store(mut data_location: String) -> Result<(Url, Arc<dyn ObjectStore>), BoxError> {
    // Make sure there is a trailing slash so it's recognized as a directory.
    if !data_location.ends_with('/') {
        data_location.push('/');
    }

    if data_location.starts_with("gs://") {
        let bucket = {
            let segment = data_location.trim_start_matches("gs://").split('/').next();
            segment.ok_or("invalid GCS url")?
        };

        let store = Arc::new(
            GoogleCloudStorageBuilder::from_env()
                .with_bucket_name(bucket)
                .build()?,
        );
        Ok((Url::parse(&data_location)?, store))
    } else if data_location.starts_with("s3://") {
        let bucket = {
            let segment = data_location.trim_start_matches("s3://").split('/').next();
            segment.ok_or("invalid S3 url")?
        };

        let store = Arc::new(
            AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .build()?,
        );
        Ok((Url::parse(&data_location)?, store))
    } else {
        if fs::metadata(&data_location).is_err() {
            // Create the directory if it doesn't exist.
            fs::create_dir(&data_location)?;
        }
        let store = Arc::new(LocalFileSystem::new_with_prefix(&data_location)?);
        let path = format!("/{}", data_location);
        let url = Url::from_directory_path(path).unwrap();
        Ok((url, store))
    }
}
