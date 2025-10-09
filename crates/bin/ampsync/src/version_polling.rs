//! Version polling for automatic dataset updates.
//!
//! This module provides background polling of the admin-api to detect when new
//! dataset versions are published, enabling zero-downtime updates.

use std::time::Duration;

use datasets_common::{name::Name, version::Version};
use tracing::{debug, info, warn};

use crate::manifest;

/// Background task that polls for new dataset versions.
///
/// Runs only when DATASET_VERSION is not specified. Polls the admin-api versions endpoint
/// at regular intervals to check if a new version is available. When a new version is detected,
/// sends it through the channel to trigger a schema reload and stream restart.
///
/// This function ONLY fetches version numbers (not schemas) for efficiency.
///
/// # Arguments
/// * `admin_api_addr` - Base URL of the admin-api service
/// * `dataset_name` - Name of the dataset to poll for
/// * `current_version` - The currently loaded version (to detect changes)
/// * `poll_interval_secs` - How often to poll (in seconds)
/// * `tx` - Channel to send new version notifications through
pub async fn version_poll_task(
    admin_api_addr: String,
    dataset_name: Name,
    mut current_version: Version,
    poll_interval_secs: u64,
    tx: tokio::sync::mpsc::Sender<Version>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(poll_interval_secs));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        // Query admin-api versions endpoint ONLY (not schema) for efficiency
        match manifest::fetch_latest_version(&admin_api_addr, &dataset_name).await {
            Ok(latest_version) => {
                if latest_version != current_version {
                    info!(
                        dataset = %dataset_name,
                        old_version = %current_version,
                        new_version = %latest_version,
                        "new_version_detected"
                    );

                    // Send the new version through the channel
                    if tx.send(latest_version.clone()).await.is_err() {
                        warn!("version_poll_channel_closed");
                        return;
                    }

                    // Update current version to avoid duplicate notifications
                    current_version = latest_version;
                } else {
                    debug!(
                        dataset = %dataset_name,
                        version = %current_version,
                        "version_unchanged"
                    );
                }
            }
            Err(e) => {
                warn!(
                    dataset = %dataset_name,
                    error = %e,
                    "version_poll_failed"
                );
                // Continue polling - transient errors shouldn't stop the task
            }
        }
    }
}
