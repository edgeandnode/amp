use std::sync::Arc;

use common::config::Config;
use metadata_db::MetadataDb;
use worker::NodeId;

pub async fn run(
    config: Config,
    metadata_db: MetadataDb,
    node_id: NodeId,
    meter: Option<monitoring::telemetry::metrics::Meter>,
) -> Result<(), worker::Error> {
    let config = Arc::new(config);

    worker::service::new(node_id, config, metadata_db, meter).await
}
