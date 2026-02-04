// Integration tests exercising PostgresBuilder configuration options.
//
// These tests require system `initdb` and `postgres` binaries on PATH.

use std::{os::unix::fs::PermissionsExt as _, path::PathBuf};

use fs_err as fs;
use metadata_db_postgres::{PostgresBuilder, PostgresError, service};
use sqlx::{Connection as _, postgres::PgConnection};

/// Start postgres via PostgresBuilder with a custom config_param and verify
/// the setting is applied to the running instance via SHOW command.
#[tokio::test]
async fn start_with_custom_config_param_applies_setting() {
    //* Given
    let (data_dir, _tmp) = test_pgdata_dir();

    //* When
    let (handle, fut) = PostgresBuilder::new(data_dir)
        .config_param("max_connections", "10")
        .start()
        .await
        .expect("builder.start() should start postgres with custom config");
    tokio::spawn(fut);

    //* Then
    let max_conn = get_max_connections(handle.url()).await;
    assert_eq!(
        max_conn, "10",
        "max_connections should reflect custom config_param value"
    );
}

/// Start postgres via PostgresBuilder with locale and encoding settings,
/// then verify they were applied during initdb by querying database settings.
#[tokio::test]
async fn start_with_locale_and_encoding_applies_settings() {
    //* Given
    let (data_dir, _tmp) = test_pgdata_dir();

    //* When
    let (handle, fut) = PostgresBuilder::new(data_dir)
        .locale("C")
        .encoding("UTF8")
        .start()
        .await
        .expect("builder.start() should initialize and start postgres with locale/encoding");
    tokio::spawn(fut);

    //* Then
    // Verify locale was applied (datcollate is set by --locale during initdb)
    let lc_collate = get_datcollate(handle.url()).await;
    assert_eq!(
        lc_collate, "C",
        "datcollate should reflect locale setting from builder"
    );

    // Verify encoding was applied
    let encoding = get_server_encoding(handle.url()).await;
    assert_eq!(
        encoding, "UTF8",
        "server_encoding should reflect encoding setting from builder"
    );
}

/// Discover the system's PostgreSQL bin path via `which initdb`, then
/// explicitly pass it to the builder via bin_path() and verify startup succeeds.
#[tokio::test]
async fn start_with_explicit_bin_path_succeeds() {
    //* Given
    let (data_dir, _tmp) = test_pgdata_dir();

    // Discover system initdb location
    let initdb_path = which::which("initdb").expect("initdb should be on PATH for this test");
    let bin_dir = initdb_path
        .parent()
        .expect("initdb path should have parent directory");

    //* When
    let (handle, fut) = PostgresBuilder::new(data_dir.clone())
        .bin_path(bin_dir)
        .start()
        .await
        .expect("builder.start() should start postgres using specified bin_path");
    tokio::spawn(fut);

    //* Then
    // Verify postgres started successfully by connecting
    let val = select_one(handle.url()).await;
    assert_eq!(val, 1, "SELECT 1 should return 1");

    // Verify PG_VERSION was created (confirms initdb ran from specified bin_path)
    assert!(
        data_dir.join("PG_VERSION").exists(),
        "PG_VERSION should exist, confirming initdb from bin_path succeeded"
    );
}

/// Pass an initdb_arg to enable data checksums and verify the setting is active.
#[tokio::test]
async fn start_with_initdb_arg_applies_setting() {
    //* Given
    let (data_dir, _tmp) = test_pgdata_dir();

    //* When
    let (handle, fut) = PostgresBuilder::new(data_dir)
        .initdb_arg("--data-checksums", "")
        .start()
        .await
        .expect("builder.start() should start postgres with data checksums enabled");
    tokio::spawn(fut);

    //* Then
    let checksums = get_data_checksums(handle.url()).await;
    assert_eq!(
        checksums, "on",
        "data_checksums should be enabled via initdb_arg"
    );
}

/// Test that starting postgres with a nonexistent bin_path returns BinaryNotFound error.
#[tokio::test]
async fn start_with_nonexistent_bin_path_returns_binary_not_found() {
    //* Given
    let (data_dir, _tmp) = test_pgdata_dir();

    //* When
    let result = PostgresBuilder::new(data_dir)
        .bin_path("/nonexistent")
        .start()
        .await;

    //* Then
    let err = result
        .err()
        .expect("starting postgres with nonexistent bin_path should fail");

    assert!(
        matches!(err, PostgresError::BinaryNotFound { .. }),
        "expected BinaryNotFound error, got {:?}",
        err
    );
}

/// Test that starting postgres with an unwritable data directory returns an error.
#[tokio::test]
async fn start_with_unwritable_datadir_returns_error() {
    //* Given
    let tmp = tempfile::tempdir().expect("tempdir should be created");
    let readonly_parent = tmp.path().join("readonly");
    fs::create_dir(&readonly_parent).expect("should create readonly parent directory");

    // Make parent directory read-only
    let mut perms = fs::metadata(&readonly_parent)
        .expect("should get metadata")
        .permissions();
    perms.set_mode(0o444); // Read-only
    fs::set_permissions(&readonly_parent, perms).expect("should set read-only permissions");

    let data_dir = readonly_parent.join("pgdata");

    //* When
    let result = service::new(data_dir).await;

    //* Then
    let err = result
        .err()
        .expect("starting postgres with unwritable data directory should fail");

    assert!(
        matches!(err, PostgresError::CreateDataDir { .. }),
        "expected CreateDataDir error for unwritable parent, got {:?}",
        err
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

/// Connect to the given URL and return the `max_connections` setting.
async fn get_max_connections(url: &str) -> String {
    let mut conn = PgConnection::connect(url)
        .await
        .expect("should connect to postgres");
    sqlx::query_scalar("SHOW max_connections")
        .fetch_one(&mut conn)
        .await
        .expect("fetch_scalar should return exactly one value")
}

/// Connect to the given URL and return the `server_encoding` setting.
async fn get_server_encoding(url: &str) -> String {
    let mut conn = PgConnection::connect(url)
        .await
        .expect("should connect to postgres");
    sqlx::query_scalar("SHOW server_encoding")
        .fetch_one(&mut conn)
        .await
        .expect("fetch_scalar should return exactly one value")
}

/// Connect to the given URL and execute `SELECT 1`.
async fn select_one(url: &str) -> i32 {
    let mut conn = PgConnection::connect(url)
        .await
        .expect("should connect to postgres");
    sqlx::query_scalar("SELECT 1")
        .fetch_one(&mut conn)
        .await
        .expect("fetch_scalar should return exactly one value")
}

/// Connect to the given URL and return the `data_checksums` setting.
async fn get_data_checksums(url: &str) -> String {
    let mut conn = PgConnection::connect(url)
        .await
        .expect("should connect to postgres");
    sqlx::query_scalar("SHOW data_checksums")
        .fetch_one(&mut conn)
        .await
        .expect("fetch_scalar should return exactly one value")
}

/// Connect to the given URL and return the `datcollate` of the current database.
async fn get_datcollate(url: &str) -> String {
    let mut conn = PgConnection::connect(url)
        .await
        .expect("should connect to postgres");
    sqlx::query_scalar("SELECT datcollate FROM pg_database WHERE datname = current_database()")
        .fetch_one(&mut conn)
        .await
        .expect("fetch_scalar should return exactly one value")
}
