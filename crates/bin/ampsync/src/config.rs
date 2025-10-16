//! Configuration management for ampsync.
//!
//! This module handles loading configuration from environment variables,
//! fetching dataset manifests, and managing database connection strings.

use std::{sync::Arc, time::Duration};

use common::BoxError;
use datasets_common::{name::Name, version_tag::VersionTag};
use datasets_derived::Manifest;

use crate::manifest;

#[derive(Clone)]
pub struct AmpsyncConfig {
    /// Ampsync database url to connect.
    pub database_url: String,
    /// Amp ArrowFlight server endpoint to connect to.
    pub amp_flight_addr: String,
    /// Amp Admin API endpoint for schema resolution.
    pub amp_admin_api_addr: String,
    /// Dataset name (from AMP_DATASET_NAME env var).
    pub dataset_name: Name,
    /// Optional dataset version (from AMP_DATASET_VERSION env var). If None, uses latest version.
    pub dataset_version: Option<VersionTag>,
    /// Interval in seconds for polling new versions (only when dataset_version is None).
    pub version_poll_interval_secs: u64,
    /// Database pool size
    pub db_pool_size: u32,
    /// Maximum duration for database operation retries.
    pub db_operation_max_retry_duration_secs: Duration,
    /// Maximum duration for connection retries
    pub db_max_retry_duration_secs: Duration,
    /// Max number of concurrent stream batches
    pub stream_max_concurrent_batches: usize,
    /// Parsed dataset manifest.
    pub manifest: Arc<Manifest>,
}

impl AmpsyncConfig {
    /// Builds the AmpsyncConfig instance from the args passed to the Sync command instance.
    ///
    /// Performs the database env validation check:
    /// - Either of these (sets) of database values must be provided:
    ///     - database_url
    ///     - database_name, database_host, database_user
    ///
    /// Fetches the Manifest instance from the admin-api
    #[allow(clippy::too_many_arguments)]
    pub async fn from_cmd(
        dataset_name: Name,
        dataset_version: Option<VersionTag>,
        amp_admin_api_addr: String,
        amp_flight_addr: String,
        version_poll_interval_secs: u64,
        database_url: Option<String>,
        database_host: Option<String>,
        database_port: u16,
        database_user: Option<String>,
        database_password: Option<String>,
        database_name: Option<String>,
        db_pool_size: u32,
        db_operation_max_retry_duration_secs: u64,
        db_max_retry_duration_secs: u64,
        stream_max_concurrent_batches: usize,
    ) -> Result<Self, BoxError> {
        // Fetch manifest from admin API (polls indefinitely until dataset is published)
        let manifest = Arc::new(
            manifest::fetch_manifest_with_startup_poll(
                &amp_admin_api_addr,
                &dataset_name,
                dataset_version.as_ref(),
            )
            .await?,
        );

        if let Some(url) = database_url {
            return Ok(Self {
                database_url: url,
                amp_flight_addr,
                amp_admin_api_addr,
                dataset_name,
                dataset_version,
                version_poll_interval_secs,
                manifest,
                db_pool_size,
                db_operation_max_retry_duration_secs: Duration::from_secs(
                    db_operation_max_retry_duration_secs,
                ),
                db_max_retry_duration_secs: Duration::from_secs(
                    db_operation_max_retry_duration_secs,
                ),
                stream_max_concurrent_batches,
            });
        }

        // Check if we have the minimum required components
        if database_user.is_none() || database_name.is_none() || database_host.is_none() {
            return Err(
                "Either DATABASE_URL or (DATABASE_USER and DATABASE_NAME AND DATABASE_HOST) must be provided".into(),
            );
        }

        // Construct the PostgreSQL URL. format: postgresql://{user}:{password}@{host}:{port}/{database}
        let mut database_url = String::from("postgresql://");

        // Add user
        database_url.push_str(&database_user.unwrap());

        // Add password if provided
        if let Some(pass) = database_password {
            database_url.push(':');
            database_url.push_str(&pass);
        }

        // Add host and port
        database_url.push('@');
        database_url.push_str(&database_host.unwrap());
        database_url.push(':');
        database_url.push_str(&database_port.to_string());

        // Add database name
        database_url.push('/');
        database_url.push_str(&database_name.unwrap());

        Ok(Self {
            database_url,
            amp_flight_addr,
            amp_admin_api_addr,
            dataset_name,
            dataset_version,
            version_poll_interval_secs,
            manifest,
            db_pool_size,
            db_operation_max_retry_duration_secs: Duration::from_secs(
                db_operation_max_retry_duration_secs,
            ),
            db_max_retry_duration_secs: Duration::from_secs(db_max_retry_duration_secs),
            stream_max_concurrent_batches,
        })
    }
}
