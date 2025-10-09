//! Configuration management for ampsync.
//!
//! This module handles loading configuration from environment variables,
//! fetching dataset manifests, and managing database connection strings.

use std::{env, sync::Arc};

use common::BoxError;
use datasets_common::{name::Name, version::Version};
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
    /// Dataset name (from DATASET_NAME env var).
    pub dataset_name: Name,
    /// Optional dataset version (from DATASET_VERSION env var). If None, uses latest version.
    pub dataset_version: Option<Version>,
    /// Interval in seconds for polling new versions (only when dataset_version is None).
    pub version_poll_interval_secs: u64,
    /// Parsed dataset manifest.
    pub manifest: Arc<Manifest>,
}

impl AmpsyncConfig {
    pub async fn from_env() -> Result<Self, BoxError> {
        // Get dataset name - required
        let dataset_name_str = env::var("DATASET_NAME")
            .map_err(|_| "DATASET_NAME environment variable is required")?;

        let dataset_name: Name = dataset_name_str
            .parse()
            .map_err(|e| format!("Invalid DATASET_NAME '{}': {}", dataset_name_str, e))?;

        // Get dataset version - optional
        let dataset_version = if let Ok(version_str) = env::var("DATASET_VERSION") {
            let parsed_version: Version = version_str
                .parse()
                .map_err(|e| format!("Invalid DATASET_VERSION '{}': {}", version_str, e))?;
            Some(parsed_version)
        } else {
            None
        };

        // Get version polling interval - only used when dataset_version is None
        const DEFAULT_VERSION_POLL_INTERVAL_SECS: u64 = 5;
        let version_poll_interval_secs = env::var("VERSION_POLL_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_VERSION_POLL_INTERVAL_SECS);

        // Get Nozzle configuration
        let amp_flight_addr =
            env::var("AMP_FLIGHT_ADDR").unwrap_or_else(|_| "http://localhost:1602".to_string());
        let amp_admin_api_addr =
            env::var("AMP_ADMIN_API_ADDR").unwrap_or_else(|_| "http://localhost:1610".to_string());

        // Fetch manifest from admin API (polls indefinitely until dataset is published)
        let manifest = Arc::new(
            manifest::fetch_manifest_with_startup_poll(
                &amp_admin_api_addr,
                &dataset_name,
                dataset_version.as_ref(),
            )
            .await?,
        );

        // First, try to get DATABASE_URL directly
        if let Ok(database_url) = env::var("DATABASE_URL") {
            return Ok(Self {
                database_url,
                amp_flight_addr,
                amp_admin_api_addr,
                dataset_name,
                dataset_version,
                version_poll_interval_secs,
                manifest,
            });
        }

        // Otherwise, try to construct from individual components
        let user = env::var("DATABASE_USER").ok();
        let password = env::var("DATABASE_PASSWORD").ok();
        let host = env::var("DATABASE_HOST").unwrap_or_else(|_| "localhost".to_string());
        let port = env::var("DATABASE_PORT")
            .unwrap_or_else(|_| "5432".to_string())
            .parse::<u16>()
            .map_err(|_| "Invalid DATABASE_PORT")?;
        let name = env::var("DATABASE_NAME").ok();

        // Check if we have the minimum required components
        if user.is_none() || name.is_none() {
            return Err(
                "Either DATABASE_URL or (DATABASE_USER and DATABASE_NAME) must be provided".into(),
            );
        }

        // Construct the PostgreSQL URL. format: postgresql://{user}:{password}@{host}:{port}/{database}
        let mut database_url = String::from("postgresql://");

        // Add user
        database_url.push_str(&user.unwrap());

        // Add password if provided
        if let Some(pass) = password {
            database_url.push(':');
            database_url.push_str(&pass);
        }

        // Add host and port
        database_url.push('@');
        database_url.push_str(&host);
        database_url.push(':');
        database_url.push_str(&port.to_string());

        // Add database name
        database_url.push('/');
        database_url.push_str(&name.unwrap());

        Ok(Self {
            database_url,
            amp_flight_addr,
            amp_admin_api_addr,
            dataset_name,
            dataset_version,
            version_poll_interval_secs,
            manifest,
        })
    }
}
