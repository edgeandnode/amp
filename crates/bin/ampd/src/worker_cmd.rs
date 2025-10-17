use std::sync::Arc;

use common::config::Config;
use metadata_db::MetadataDb;
use worker::{NodeId, Worker};

pub async fn run(
    config: Config,
    metadata_db: MetadataDb,
    node_id: NodeId,
    meter: Option<monitoring::telemetry::metrics::Meter>,
) -> Result<(), worker::Error> {
    let config = Arc::new(config);

    let worker = Worker::new(config, metadata_db, node_id, meter);
    worker.run().await
}
