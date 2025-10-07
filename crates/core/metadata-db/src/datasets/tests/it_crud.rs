//! Core dataset CRUD operations tests

use pgtemp::PgTempDB;

use crate::{
    conn::DbConn,
    datasets::{self, Name, Version},
};

#[tokio::test]
async fn insert_with_valid_data_succeeds() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let owner = "test_owner";
    let name = Name::from_ref_unchecked("test_dataset");
    let version = "1.0.0"
        .parse::<Version>()
        .expect("should parse valid version");
    let manifest_path = "test_dataset__1_0_0.json";

    //* When
    let result = datasets::insert(&mut *conn, owner, name, version, manifest_path).await;

    //* Then
    assert!(
        result.is_ok(),
        "dataset insertion should succeed with valid data"
    );
}

#[tokio::test]
async fn insert_with_prerelease_version_succeeds() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let owner = "test_owner";
    let name = Name::from_ref_unchecked("prerelease_dataset");
    let version = "1.0.0-alpha.1"
        .parse::<Version>()
        .expect("should parse prerelease version");
    let manifest_path = "prerelease_dataset__1_0_0-alpha.1.json";

    //* When
    let result = datasets::insert(
        &mut *conn,
        owner,
        name.clone(),
        version.clone(),
        manifest_path,
    )
    .await;

    //* Then
    assert!(
        result.is_ok(),
        "prerelease version insertion should succeed"
    );

    // Verify dataset exists
    let exists = datasets::exists_by_name_and_version(&mut *conn, name, version)
        .await
        .expect("should check prerelease version existence");
    assert!(exists, "prerelease version should exist");
}

#[tokio::test]
async fn insert_with_duplicate_name_and_version_fails() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let owner = "test_owner";
    let name = Name::from_ref_unchecked("duplicate_dataset");
    let version = "1.0.0"
        .parse::<Version>()
        .expect("should parse valid version");
    let manifest_path = "duplicate_dataset__1_0_0.json";

    // Insert first dataset
    let first_result = datasets::insert(
        &mut *conn,
        owner,
        name.clone(),
        version.clone(),
        manifest_path,
    )
    .await;
    assert!(
        first_result.is_ok(),
        "first dataset insertion should succeed"
    );

    //* When
    // Try to insert with same name and version
    let second_result = datasets::insert(&mut *conn, owner, name, version, manifest_path).await;

    //* Then
    assert!(
        second_result.is_err(),
        "duplicate dataset insertion should fail"
    );
}

#[tokio::test]
async fn get_by_name_and_version_with_details_returns_existing_dataset() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let owner = "test_owner";
    let name = Name::from_ref_unchecked("existing_dataset");
    let version = "2.1.0"
        .parse::<Version>()
        .expect("should parse valid version");
    let manifest_path = "existing_dataset__2_1_0.json";

    // Insert dataset first
    let insert_result = datasets::insert(
        &mut *conn,
        owner,
        name.clone(),
        version.clone(),
        manifest_path,
    )
    .await;
    assert!(insert_result.is_ok(), "dataset insertion should succeed");

    //* When
    let result =
        datasets::get_by_name_and_version_with_details(&mut *conn, name.clone(), version).await;

    //* Then
    assert!(result.is_ok(), "retrieval should succeed");
    let dataset = result.expect("should return valid result");
    assert!(dataset.is_some(), "dataset should exist");

    let dataset = dataset.expect("dataset should be found");
    assert_eq!(dataset.owner, owner, "owner should match");
    assert_eq!(dataset.name, name, "name should match");
    assert_eq!(dataset.version.major, 2, "major version should match");
    assert_eq!(dataset.version.minor, 1, "minor version should match");
    assert_eq!(dataset.version.patch, 0, "patch version should match");
    assert_eq!(
        dataset.manifest_path, manifest_path,
        "manifest path should match"
    );
}

#[tokio::test]
async fn get_by_name_and_version_with_details_returns_none_for_nonexistent_dataset() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let name = Name::from_ref_unchecked("nonexistent_dataset");
    let version = "1.0.0"
        .parse::<Version>()
        .expect("should parse valid version");

    //* When
    let result = datasets::get_by_name_and_version_with_details(&mut *conn, name, version).await;

    //* Then
    assert!(
        result.is_ok(),
        "query should succeed even if dataset not found"
    );
    let dataset = result.expect("should return valid result");
    assert!(dataset.is_none(), "nonexistent dataset should return None");
}

#[tokio::test]
async fn exists_by_name_and_version_returns_true_for_existing_dataset() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let owner = "test_owner";
    let name = Name::from_ref_unchecked("exists_dataset");
    let version = "1.5.2"
        .parse::<Version>()
        .expect("should parse valid version");
    let manifest_path = "exists_dataset__1_5_2.json";

    // Insert dataset first
    let insert_result = datasets::insert(
        &mut *conn,
        owner,
        name.clone(),
        version.clone(),
        manifest_path,
    )
    .await;
    assert!(insert_result.is_ok(), "dataset insertion should succeed");

    //* When
    let result = datasets::exists_by_name_and_version(&mut *conn, name, version).await;

    //* Then
    assert!(result.is_ok(), "existence check should succeed");
    let exists = result.expect("should return valid result");
    assert!(exists, "existing dataset should return true");
}

#[tokio::test]
async fn exists_by_name_and_version_returns_false_for_nonexistent_dataset() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let name = Name::from_ref_unchecked("nonexistent_dataset");
    let version = "99.99.99"
        .parse::<Version>()
        .expect("should parse valid version");

    //* When
    let result = datasets::exists_by_name_and_version(&mut *conn, name, version).await;

    //* Then
    assert!(result.is_ok(), "existence check should succeed");
    let exists = result.expect("should return valid result");
    assert!(!exists, "nonexistent dataset should return false");
}

#[tokio::test]
async fn get_manifest_by_name_and_version_returns_manifest_for_existing_dataset() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let owner = "test_owner";
    let name = Name::from_ref_unchecked("manifest_dataset");
    let version = "3.0.1"
        .parse::<Version>()
        .expect("should parse valid version");
    let manifest_path = "manifest_dataset__3_0_1.json";

    // Insert dataset first
    let insert_result = datasets::insert(
        &mut *conn,
        owner,
        name.clone(),
        version.clone(),
        manifest_path,
    )
    .await;
    assert!(insert_result.is_ok(), "dataset insertion should succeed");

    //* When
    let result = datasets::get_manifest_path_by_name_and_version(&mut *conn, name, version).await;

    //* Then
    assert!(result.is_ok(), "manifest retrieval should succeed");
    let manifest = result.expect("should return valid result");
    assert!(manifest.is_some(), "manifest should exist");

    let manifest = manifest.expect("manifest should be found");
    assert_eq!(manifest, manifest_path, "manifest path should match");
}

#[tokio::test]
async fn get_manifest_by_name_and_version_returns_none_for_nonexistent_dataset() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let name = Name::from_ref_unchecked("no_manifest_dataset");
    let version = "1.0.0"
        .parse::<Version>()
        .expect("should parse valid version");

    //* When
    let result = datasets::get_manifest_path_by_name_and_version(&mut *conn, name, version).await;

    //* Then
    assert!(
        result.is_ok(),
        "manifest query should succeed even if dataset not found"
    );
    let manifest = result.expect("should return valid result");
    assert!(
        manifest.is_none(),
        "nonexistent dataset should return None manifest"
    );
}

#[tokio::test]
async fn get_latest_version_by_name_returns_none_for_nonexistent_dataset() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let name = Name::from_ref_unchecked("no_versions_dataset");

    //* When
    let result = datasets::get_latest_version_by_name_with_details(&mut *conn, name).await;

    //* Then
    assert!(
        result.is_ok(),
        "latest version query should succeed even if dataset not found"
    );
    let latest_version = result.expect("should return valid result");
    assert!(
        latest_version.is_none(),
        "nonexistent dataset should return None version"
    );
}

#[tokio::test]
async fn get_latest_version_by_name_returns_highest_version() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let owner = "test_owner";
    let name = Name::from_ref_unchecked("versioned_dataset");

    // Insert multiple versions
    let version_1_0_0 = "1.0.0"
        .parse::<Version>()
        .expect("should parse version 1.0.0");
    let version_1_2_0 = "1.2.0"
        .parse::<Version>()
        .expect("should parse version 1.2.0");
    let version_2_0_0 = "2.0.0"
        .parse::<Version>()
        .expect("should parse version 2.0.0");
    let version_1_10_0 = "1.10.0"
        .parse::<Version>()
        .expect("should parse version 1.10.0");

    let insert1 = datasets::insert(
        &mut *conn,
        owner,
        name.clone(),
        version_1_0_0,
        "versioned_dataset__1_0_0.json",
    )
    .await;
    let insert2 = datasets::insert(
        &mut *conn,
        owner,
        name.clone(),
        version_1_2_0,
        "versioned_dataset__1_2_0.json",
    )
    .await;
    let insert3 = datasets::insert(
        &mut *conn,
        owner,
        name.clone(),
        version_2_0_0,
        "versioned_dataset__2_0_0.json",
    )
    .await;
    let insert4 = datasets::insert(
        &mut *conn,
        owner,
        name.clone(),
        version_1_10_0,
        "versioned_dataset__1_10_0.json",
    )
    .await;

    assert!(insert1.is_ok(), "first dataset insertion should succeed");
    assert!(insert2.is_ok(), "second dataset insertion should succeed");
    assert!(insert3.is_ok(), "third dataset insertion should succeed");
    assert!(insert4.is_ok(), "fourth dataset insertion should succeed");

    //* When
    let result = datasets::get_latest_version_by_name_with_details(&mut *conn, name.clone()).await;

    //* Then
    assert!(result.is_ok(), "latest version retrieval should succeed");
    let latest_dataset = result.expect("should return valid result");
    assert!(latest_dataset.is_some(), "latest version should exist");

    let latest_dataset = latest_dataset.expect("latest version should be found");
    assert_eq!(
        latest_dataset.name, "versioned_dataset",
        "dataset name should match"
    );
    assert_eq!(latest_dataset.owner, owner, "dataset owner should match");
    assert_eq!(
        latest_dataset.version.major, 2,
        "latest version major should be 2"
    );
    assert_eq!(
        latest_dataset.version.minor, 0,
        "latest version minor should be 0"
    );
    assert_eq!(
        latest_dataset.version.patch, 0,
        "latest version patch should be 0"
    );
    assert_eq!(
        latest_dataset.manifest_path, "versioned_dataset__2_0_0.json",
        "manifest path should match"
    );
}
