// Integration tests exercising PostgreSQL shutdown behavior with active connections.
//
// These tests require system `initdb` and `postgres` binaries on PATH.

use std::path::PathBuf;

use sqlx::postgres::PgConnection;

/// Start postgres, open a connection, initiate graceful shutdown, verify
/// the background future waits for the connection to close before completing.
#[tokio::test]
async fn graceful_shutdown_with_active_connection_blocks_until_disconnect() {
    //* Given
    let (data_dir, _tmp) = test_pgdata_dir();

    let (handle, fut) = metadata_db_postgres::service::new(data_dir)
        .await
        .expect("service::new should start postgres");
    let mut pg_task = tokio::spawn(fut);

    // Connect to postgres
    let mut conn = connect(handle.url()).await;

    // Verify connection works
    let val: i32 = fetch_scalar(&mut conn, "SELECT 1").await;
    assert_eq!(val, 1, "SELECT 1 should return exactly one row");

    //* When
    // Initiate graceful shutdown while connection is still open
    let _shutting_down = handle.graceful_shutdown();

    // Verify background future does NOT complete while connection is open
    tokio::select! {
        result = &mut pg_task => {
            panic!("Background future completed before client disconnected: {:?}", result);
        }
        _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
            // Expected: graceful shutdown should wait for active connection
        }
    };

    // Disconnect the client
    drop(conn);

    //* Then
    // Ensure background future completes successfully after client disconnects
    pg_task
        .await
        .expect("background task should not panic")
        .expect("background future should return Ok after client disconnects");
}

/// Create a temporary directory containing a `pgdata` subdirectory path.
///
/// The caller must hold the returned [`tempfile::TempDir`] guard to keep the
/// directory alive for the duration of the test.
fn test_pgdata_dir() -> (PathBuf, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("tempdir should be created");
    let data_dir = tmp.path().join("pgdata");
    (data_dir, tmp)
}

/// Open a new [`PgConnection`] to the given URL.
async fn connect(url: &str) -> PgConnection {
    use sqlx::Connection as _;
    PgConnection::connect(url)
        .await
        .expect("should connect to postgres")
}

/// Execute a SQL query and return a single scalar value.
async fn fetch_scalar<O>(conn: &mut PgConnection, sql: &str) -> O
where
    O: Send + Unpin,
    for<'r> (O,): sqlx::FromRow<'r, sqlx::postgres::PgRow>,
{
    sqlx::query_scalar(sql)
        .fetch_one(conn)
        .await
        .expect("fetch_scalar should return exactly one value")
}
