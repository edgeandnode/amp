// Integration tests exercising the managed PostgreSQL lifecycle.
//
// These tests require system `initdb` and `postgres` binaries on PATH.

use std::path::PathBuf;

use fs_err as fs;
use metadata_db_postgres::service;
use sqlx::postgres::PgConnection;

/// Start managed postgres, verify PG_VERSION exists and data_dir() matches,
/// connect via the returned URL, execute a query, then shut down gracefully.
#[tokio::test]
async fn start_with_fresh_datadir_accepts_connections() {
    //* Given
    let (data_dir, _tmp) = test_pgdata_dir();

    //* When
    let (handle, fut) = service::new(data_dir.clone())
        .await
        .expect("service::new should start postgres");
    tokio::spawn(fut);

    //* Then
    // PG_VERSION must exist after initdb
    assert!(
        data_dir.join("PG_VERSION").exists(),
        "PG_VERSION should exist in data directory after startup"
    );

    // Handle::data_dir() must match the path we provided
    assert_eq!(
        handle.data_dir(),
        data_dir.as_path(),
        "Handle::data_dir() should return the data directory passed at construction"
    );

    // Connect and execute a simple query
    let val = select_one(handle.url()).await;
    assert_eq!(val, 1, "SELECT 1 should return 1");
}

/// Start postgres, create a table and insert data, stop, restart
/// with the same datadir, and verify the data persists.
#[tokio::test]
async fn restart_with_same_datadir_preserves_data() {
    //* Given
    let (data_dir, _tmp) = test_pgdata_dir();

    // First run: create table and insert a row
    {
        let (handle, fut) = service::new(data_dir.clone())
            .await
            .expect("first service::new should start postgres");
        let pg_task = tokio::spawn(fut);

        let mut conn = connect(handle.url()).await;
        seed_test_persist(&mut conn).await;
        drop(conn);
        handle.graceful_shutdown();
        pg_task
            .await
            .expect("task should not panic")
            .expect("background service should succeed");
    }

    //* When — restart with the same data directory
    let (handle, fut) = service::new(data_dir.clone())
        .await
        .expect("second service::new should reuse existing datadir");
    tokio::spawn(fut);

    //* Then
    let name = get_test_persist_name(handle.url()).await;
    assert_eq!(name, "amp", "persisted data should match inserted value");
}

/// Start managed postgres, verify connection works, then initiate
/// force shutdown via SIGINT and verify clean termination.
#[tokio::test]
async fn force_shutdown_with_no_active_connections_terminates() {
    //* Given
    let (data_dir, _tmp) = test_pgdata_dir();

    let (handle, fut) = service::new(data_dir.clone())
        .await
        .expect("service::new should start postgres");
    tokio::spawn(fut);

    // Verify postgres is running by connecting
    let val = select_one(handle.url()).await;
    assert_eq!(val, 1, "SELECT 1 should return exactly one row");

    //* When — force shutdown (SIGINT)
    handle.force_shutdown();
}

/// Start postgres in a fresh tempdir and verify that initdb
/// creates the PG_VERSION file on the filesystem.
#[tokio::test]
async fn initdb_with_fresh_datadir_creates_pg_version() {
    //* Given
    let (data_dir, _tmp) = test_pgdata_dir();
    assert!(
        !data_dir.join("PG_VERSION").exists(),
        "PG_VERSION should not exist before startup"
    );

    //* When
    let (_handle, fut) = service::new(data_dir.clone())
        .await
        .expect("service::new should initialize and start postgres");
    tokio::spawn(fut);

    //* Then
    assert!(
        data_dir.join("PG_VERSION").exists(),
        "PG_VERSION should exist after initdb runs"
    );
    let version_content =
        fs::read_to_string(data_dir.join("PG_VERSION")).expect("PG_VERSION should be readable");
    assert!(
        !version_content.trim().is_empty(),
        "PG_VERSION should contain a non-empty version string"
    );
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

/// Execute a SQL statement that returns no rows.
async fn execute(conn: &mut PgConnection, sql: &str) {
    sqlx::query(sql)
        .execute(conn)
        .await
        .expect("execute should succeed");
}

/// Connect to the given URL and execute `SELECT 1`.
async fn select_one(url: &str) -> i32 {
    let mut conn = connect(url).await;
    sqlx::query_scalar("SELECT 1")
        .fetch_one(&mut conn)
        .await
        .expect("fetch_scalar should return exactly one value")
}

/// Create the `test_persist` table and insert a row.
async fn seed_test_persist(conn: &mut PgConnection) {
    execute(
        conn,
        "CREATE TABLE test_persist (id INT PRIMARY KEY, name TEXT)",
    )
    .await;
    execute(
        conn,
        "INSERT INTO test_persist (id, name) VALUES (1, 'amp')",
    )
    .await;
}

/// Connect to the given URL and return the name from `test_persist` where id = 1.
async fn get_test_persist_name(url: &str) -> String {
    let mut conn = connect(url).await;
    sqlx::query_scalar("SELECT name FROM test_persist WHERE id = 1")
        .fetch_one(&mut conn)
        .await
        .expect("fetch_scalar should return exactly one value")
}
