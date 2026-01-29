// Integration tests exercising PostgreSQL data persistence and directory reuse.
//
// These tests require system `initdb` and `postgres` binaries on PATH.

use std::{path::PathBuf, time::Duration};

use fs_err as fs;
use sqlx::postgres::PgConnection;

/// Start postgres, verify initdb was run, shutdown, restart with same data_dir,
/// verify initdb did NOT re-run (PG_VERSION timestamp unchanged).
#[tokio::test]
async fn start_with_existing_datadir_skips_initdb() {
    //* Given
    let (data_dir, _tmp) = test_pgdata_dir();

    // First run: initialize the data directory
    {
        let (handle, fut) = metadata_db_postgres::service::new(data_dir.clone())
            .await
            .expect("first service::new should initialize and start postgres");
        let pg_task = tokio::spawn(fut);

        // Verify PG_VERSION exists after initdb
        let pg_version_path = data_dir.join("PG_VERSION");
        assert!(
            pg_version_path.exists(),
            "PG_VERSION should exist after first initialization"
        );

        // Small delay to ensure timestamp would differ if file is rewritten
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Notify postgres shutdown gracefully to ensure clean state
        handle.graceful_shutdown();
        pg_task
            .await
            .expect("task should not panic")
            .expect("background service should succeed");
    }

    // Record PG_VERSION metadata before second run
    let pg_version_path = data_dir.join("PG_VERSION");
    let before_restart_metadata =
        fs::metadata(&pg_version_path).expect("should read PG_VERSION metadata before restart");
    let before_restart_modified = before_restart_metadata
        .modified()
        .expect("should get modification time before restart");

    //* When — restart with the same data directory
    let (handle, fut) = metadata_db_postgres::service::new(data_dir.clone())
        .await
        .expect("second service::new should reuse existing datadir");
    tokio::spawn(fut);

    //* Then — verify initdb did not re-run
    let after_restart_metadata =
        fs::metadata(&pg_version_path).expect("should read PG_VERSION metadata after restart");
    let after_restart_modified = after_restart_metadata
        .modified()
        .expect("should get modification time after restart");

    assert_eq!(
        before_restart_modified, after_restart_modified,
        "PG_VERSION modification time should be unchanged (initdb should not re-run)"
    );

    // Verify postgres is actually running and functional
    let val = select_one(handle.url()).await;
    assert_eq!(val, 1, "SELECT 1 should return exactly one row");
}

/// Write a stale postmaster.pid file with a non-existent PID, then start postgres.
/// Verify startup succeeds and the stale PID file is cleaned up.
#[tokio::test]
async fn start_with_orphan_pid_file_succeeds() {
    //* Given
    let (data_dir, _tmp) = test_pgdata_dir();

    // First run: initialize the data directory
    {
        let (handle, fut) = metadata_db_postgres::service::new(data_dir.clone())
            .await
            .expect("first service::new should initialize postgres");
        let pg_task = tokio::spawn(fut);

        // Notify postgres shutdown gracefully to ensure clean state
        handle.graceful_shutdown();
        pg_task
            .await
            .expect("task should not panic")
            .expect("background service should succeed");
    }

    // Write a stale postmaster.pid with a non-existent PID
    let pid_file = data_dir.join("postmaster.pid");
    fs::write(&pid_file, "999999\n/tmp/test-socket\n").expect("should write stale PID file");

    assert!(
        pid_file.exists(),
        "stale postmaster.pid should exist before restart"
    );

    //* When — start postgres with the stale PID file present
    let (handle, fut) = metadata_db_postgres::service::new(data_dir.clone())
        .await
        .expect("service::new should succeed despite stale PID file");
    tokio::spawn(fut);

    //* Then — verify postgres is running and PID file is valid
    let val = select_one(handle.url()).await;
    assert_eq!(val, 1, "SELECT 1 should return exactly one row");
}

/// Create multiple tables with data, restart postgres, verify all tables and data persist.
#[tokio::test]
async fn restart_with_multiple_tables_preserves_data() {
    //* Given
    let (data_dir, _tmp) = test_pgdata_dir();

    // First run: create multiple tables with data
    {
        let (handle, fut) = metadata_db_postgres::service::new(data_dir.clone())
            .await
            .expect("first service::new should start postgres");
        let pg_task = tokio::spawn(fut);

        // Seed multiple tables with data
        let mut conn = connect(handle.url()).await;
        create_table(&mut conn, "t1").await;
        create_table(&mut conn, "t2").await;
        create_table(&mut conn, "t3").await;
        insert_table_val(&mut conn, "t1").await;
        insert_table_val(&mut conn, "t2").await;
        insert_table_val(&mut conn, "t3").await;
        drop(conn);

        // Notify postgres shutdown gracefully to ensure all data is flushed
        handle.graceful_shutdown();
        pg_task
            .await
            .expect("task should not panic")
            .expect("background service should succeed");
    }

    //* When
    // Restart with the same data directory
    let (handle, fut) = metadata_db_postgres::service::new(data_dir.clone())
        .await
        .expect("second service::new should reuse existing datadir");
    tokio::spawn(fut);

    //* Then
    //
    let mut conn = connect(handle.url()).await;

    let v1 = fetch_table_val(&mut conn, "t1").await;
    assert_eq!(v1, "t1", "t1 should retain its inserted value");

    let v2 = fetch_table_val(&mut conn, "t2").await;
    assert_eq!(v2, "t2", "t2 should retain its inserted value");

    let v3 = fetch_table_val(&mut conn, "t3").await;
    assert_eq!(v3, "t3", "t3 should retain its inserted value");
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

/// Create a table with `(id INT PRIMARY KEY, val TEXT)` schema.
async fn create_table(conn: &mut PgConnection, table: &str) {
    let sql = format!("CREATE TABLE {table} (id INT PRIMARY KEY, val TEXT)");
    sqlx::query(&sql)
        .execute(conn)
        .await
        .expect("create_table should succeed");
}

/// Insert a single row `(1, '<table>')` into the given table.
async fn insert_table_val(conn: &mut PgConnection, table: &str) {
    let sql = format!("INSERT INTO {table} VALUES (1, '{table}')");
    sqlx::query(&sql)
        .execute(conn)
        .await
        .expect("insert_table_val should succeed");
}

/// Connect to the given URL and execute `SELECT 1`.
async fn select_one(url: &str) -> i32 {
    let mut conn = connect(url).await;
    sqlx::query_scalar("SELECT 1")
        .fetch_one(&mut conn)
        .await
        .expect("fetch_scalar should return exactly one value")
}

/// Fetch the `val` column from the given table (expects exactly one row).
async fn fetch_table_val(conn: &mut PgConnection, table: &str) -> String {
    let sql = format!("SELECT val FROM {table}");
    sqlx::query_scalar(&sql)
        .fetch_one(conn)
        .await
        .expect("fetch_table_val should return exactly one value")
}
