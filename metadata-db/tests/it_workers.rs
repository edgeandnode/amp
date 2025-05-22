use metadata_db::{MetadataDb, ACTIVE_INTERVAL_SECS};
use pgtemp::PgTempDB;

#[tokio::test]
async fn register_worker() {
    //* Given
    let temp_db = PgTempDB::new();

    let metadata_db = MetadataDb::connect(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");

    let worker_id = "test-worker-id".parse().expect("Invalid worker ID");

    //* When
    metadata_db
        .hello_worker(&worker_id)
        .await
        .expect("Failed to register the worker");

    let active_workers = metadata_db
        .active_workers()
        .await
        .expect("Failed to get active workers");

    //* Then
    assert!(
        active_workers.contains(&worker_id),
        "Worker not found in active workers"
    );
}

#[tokio::test]
async fn detect_inactive_worker() {
    //* Given
    let temp_db = PgTempDB::new();

    let metadata_db = MetadataDb::connect(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");

    // Pre-register a worker
    let worker_id = "test-worker-id".parse().expect("Invalid worker ID");
    metadata_db
        .hello_worker(&worker_id)
        .await
        .expect("Failed to pre-register the worker");

    //* When
    // Sleep for 2 ACTIVE_INTERVAL_SECS to ensure the worker is considered inactive
    tokio::time::sleep(std::time::Duration::from_secs(
        (2 * ACTIVE_INTERVAL_SECS) as u64,
    ))
    .await;

    let active_workers = metadata_db
        .active_workers()
        .await
        .expect("Failed to get active workers");

    //* Then
    assert!(
        !active_workers.contains(&worker_id),
        "The worker should be inactive"
    );
}
