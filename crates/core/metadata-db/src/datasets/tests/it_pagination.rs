//! Pagination tests for dataset listing

use pgtemp::PgTempDB;
use rand::seq::SliceRandom;

use crate::{
    conn::DbConn,
    datasets::{self, Name, Version, VersionOwned},
};

#[tokio::test]
async fn list_first_page_when_empty_returns_empty_list() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    //* When
    let result = datasets::list_first_page(&mut *conn, 10)
        .await
        .expect("Failed to list datasets");

    //* Then
    assert!(
        result.is_empty(),
        "should return empty list when no datasets exist"
    );
}

#[tokio::test]
async fn list_first_page_respects_limit_and_ordering() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    // Create 7 datasets with mixed names and versions to test both limit and ordering
    // - 3 entries: test_dataset_1 with semver edge cases (10.0.0, 2.0.0, 1.10.0)
    // - 2 entries: test_dataset_2, test_dataset_3 (both v3.0.0)
    // - 2 entries: test_dataset_4 (v5.0.0, v4.0.0)
    let mut datasets = vec![
        ("test_dataset_1", "10.0.0"), // #1:  Highest version - tests 10 > 2
        ("test_dataset_1", "2.0.0"),  // #2:  Mid version - tests 2 > 1.10
        ("test_dataset_1", "1.10.0"), // #3: Tests proper semver ordering vs lexicographic
        ("test_dataset_2", "3.0.0"),  // #4
        ("test_dataset_3", "3.0.0"),  // #5
        ("test_dataset_4", "5.0.0"),
        ("test_dataset_4", "4.0.0"),
    ];

    // Shuffle to ensure insertion order does not affect retrieval order
    datasets.shuffle(&mut rand::rng());

    // Insert datasets into the database
    for (name, version_str) in datasets.iter() {
        let owner = "test_owner";
        let name = Name::from_ref(*name);
        let version = test_version(version_str);
        let manifest_path = test_manifest_path(&name, &version);

        datasets::insert(&mut *conn, owner, name, version, &manifest_path)
            .await
            .expect("should insert dataset");
    }

    //* When
    // Test limit functionality
    let result = datasets::list_first_page(&mut *conn, 5)
        .await
        .expect("Failed to list datasets with limit");

    //* Then - Verify limit is respected and ordering is correct
    assert_eq!(result.len(), 5, "should respect limit parameter");

    // Verify datasets are returned in correct order (dataset ASC, version DESC)

    // Assert first item: #1 - test_dataset_1 v10.0.0
    assert_eq!(
        result[0].name, "test_dataset_1",
        "item #1 should be test_dataset_1"
    );
    assert_eq!(
        result[0].version.to_string(),
        "10.0.0",
        "item #1 should be version 10.0.0"
    );

    // Assert second item: #2 - test_dataset_1 v2.0.0
    assert_eq!(
        result[1].name, "test_dataset_1",
        "item #2 should be test_dataset_1"
    );
    assert_eq!(
        result[1].version.to_string(),
        "2.0.0",
        "item #2 should be version 2.0.0"
    );

    // Assert third item: #3 - test_dataset_1 v1.10.0 (semver ordering test)
    assert_eq!(
        result[2].name, "test_dataset_1",
        "item #3 should be test_dataset_1"
    );
    assert_eq!(
        result[2].version.to_string(),
        "1.10.0",
        "item #3 should be version 1.10.0 (semver ordering, not lexicographic)"
    );

    // Assert fourth item: #4 - test_dataset_2 v3.0.0
    assert_eq!(
        result[3].name, "test_dataset_2",
        "item #4 should be test_dataset_2"
    );
    assert_eq!(
        result[3].version.to_string(),
        "3.0.0",
        "item #4 should be version 3.0.0"
    );

    // Assert fifth item: #5 - test_dataset_3 v3.0.0
    assert_eq!(
        result[4].name, "test_dataset_3",
        "item #5 should be test_dataset_3"
    );
    assert_eq!(
        result[4].version.to_string(),
        "3.0.0",
        "item #5 should be version 3.0.0"
    );
}
#[tokio::test]
async fn list_next_page_uses_cursor() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    // Create 7 datasets with specific structure:
    // - 3 entries: test_dataset_1 (v3.0.0, v2.0.0, v1.0.0)
    // - 2 entries: test_dataset_2, test_dataset_3 (both v3.0.0)
    // - 2 entries: test_dataset_4 (v5.0.0, v4.0.0)
    let mut datasets = vec![
        ("test_dataset_1", "3.0.0"), // <- CURSOR
        ("test_dataset_1", "2.0.0"), // #1
        ("test_dataset_1", "1.0.0"), // #2
        ("test_dataset_2", "3.0.0"), // #3
        ("test_dataset_3", "3.0.0"), // #4
        ("test_dataset_4", "5.0.0"), // #5
        ("test_dataset_4", "4.0.0"),
    ];
    let cursor = (Name::from_ref("test_dataset_1"), test_version("3.0.0"));

    // Shuffle to ensure insertion order does not affect retrieval order
    datasets.shuffle(&mut rand::rng());

    // Insert datasets into the database
    for (name, version_str) in datasets.iter() {
        let owner = "test_owner";
        let name = Name::from_ref(*name);
        let version = test_version(version_str);
        let manifest_path = test_manifest_path(&name, &version);

        datasets::insert(&mut *conn, owner, name, version, &manifest_path)
            .await
            .expect("should insert dataset");
    }

    //* When
    // Get first page (limit 5) to establish cursor
    let result = datasets::list_next_page(&mut *conn, 5, cursor)
        .await
        .expect("Failed to list second page");

    //* Then
    // Should return 5 items (all items before cursor test_dataset_1 v3.0.0)
    assert_eq!(result.len(), 5, "second page should have 5 items");

    // Assert first item: #1 - test_dataset_1 v2.0.0
    assert_eq!(
        result[0].name, "test_dataset_1",
        "item #1 should be test_dataset_1"
    );
    assert_eq!(
        result[0].version.to_string(),
        "2.0.0",
        "item #1 should be version 2.0.0"
    );

    // Assert second item: #2 - test_dataset_1 v1.0.0
    assert_eq!(
        result[1].name, "test_dataset_1",
        "item #2 should be test_dataset_1"
    );
    assert_eq!(
        result[1].version.to_string(),
        "1.0.0",
        "item #2 should be version 1.0.0"
    );

    // Assert third item: #3 - test_dataset_2 v3.0.0
    assert_eq!(
        result[2].name, "test_dataset_2",
        "item #3 should be test_dataset_2"
    );
    assert_eq!(
        result[2].version.to_string(),
        "3.0.0",
        "item #3 should be version 3.0.0"
    );

    // Assert fourth item: #4 - test_dataset_3 v3.0.0
    assert_eq!(
        result[3].name, "test_dataset_3",
        "item #4 should be test_dataset_3"
    );
    assert_eq!(
        result[3].version.to_string(),
        "3.0.0",
        "item #4 should be version 3.0.0"
    );

    // Assert fifth item: #5 - test_dataset_4 v5.0.0
    assert_eq!(
        result[4].name, "test_dataset_4",
        "item #5 should be test_dataset_4"
    );
    assert_eq!(
        result[4].version.to_string(),
        "5.0.0",
        "item #5 should be version 5.0.0"
    );
}

#[tokio::test]
async fn list_versions_by_name_first_page_when_empty_returns_empty_list() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    //* When
    let result = datasets::list_versions_by_name_first_page(
        &mut *conn,
        Name::from_ref("nonexistent_dataset"),
        10,
    )
    .await
    .expect("Failed to list versions");

    //* Then
    assert!(
        result.is_empty(),
        "should return empty list for nonexistent dataset"
    );
}

#[tokio::test]
async fn list_versions_by_name_first_page_respects_limit_and_order() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let owner = "test_owner";
    let dataset_name = Name::from_ref("versioned_dataset");

    // Create 7 versions with semver edge cases to test both limit and ordering
    // Using versions that test proper semver ordering vs lexicographic
    let mut versions = vec![
        "10.0.0", // #1: Highest version - tests 10 > 2
        "2.0.0",  // #2: Mid version - tests 2 > 1.10
        "1.10.0", // #3: Tests proper semver ordering vs lexicographic
        "1.9.0",  // #4: Tests 1.9 < 1.10 in semver
        "1.2.10", // #5: Tests patch version ordering
        "1.2.3",  //
        "1.2.2",  //
    ];

    // Shuffle to ensure insertion order does not affect retrieval order
    versions.shuffle(&mut rand::rng());

    // Insert versions into the database
    for version_str in versions.iter() {
        let version = test_version(version_str);
        let manifest_path = test_manifest_path(&dataset_name, &version);

        datasets::insert(
            &mut *conn,
            owner,
            dataset_name.clone(),
            version,
            &manifest_path,
        )
        .await
        .expect("should insert dataset");
    }

    //* When
    // Test limit functionality
    let limited_result =
        datasets::list_versions_by_name_first_page(&mut *conn, dataset_name.clone(), 5)
            .await
            .expect("Failed to list versions with limit");

    //* Then - Verify limit is respected and ordering is correct
    assert_eq!(limited_result.len(), 5, "should respect limit parameter");

    // Verify versions are returned in correct order (version DESC)
    // Should get first 5 items in descending order

    // Assert first item: #1 - v10.0.0
    assert_eq!(
        limited_result[0].to_string(),
        "10.0.0",
        "item #1 should be version 10.0.0"
    );

    // Assert second item: #2 - v2.0.0
    assert_eq!(
        limited_result[1].to_string(),
        "2.0.0",
        "item #2 should be version 2.0.0"
    );

    // Assert third item: #3 - v1.10.0 (semver ordering test)
    assert_eq!(
        limited_result[2].to_string(),
        "1.10.0",
        "item #3 should be version 1.10.0 (semver ordering, not lexicographic)"
    );

    // Assert fourth item: #4 - v1.9.0
    assert_eq!(
        limited_result[3].to_string(),
        "1.9.0",
        "item #4 should be version 1.9.0"
    );

    // Assert fifth item: #5 - v1.2.10
    assert_eq!(
        limited_result[4].to_string(),
        "1.2.10",
        "item #5 should be version 1.2.10"
    );
}

#[tokio::test]
async fn list_versions_by_name_next_page_uses_cursor() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = DbConn::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let owner = "test_owner";
    let dataset_name = Name::from_ref("paginated_dataset");

    // Create 7 versions with specific structure:
    // - Test cursor pagination with semver edge cases
    // - Cursor will be the first item (highest version)
    let mut versions = vec![
        "10.0.0", // <- CURSOR (first/highest version)
        "2.0.0",  // #1: Before cursor
        "1.10.0", // #2: Before cursor
        "1.9.0",  // #3: Before cursor
        "1.2.10", // #4: Before cursor
        "1.2.3",  // #5: Before cursor
        "1.0.0",  //
    ];
    let cursor_version = test_version("10.0.0");

    // Shuffle to ensure insertion order does not affect retrieval order
    versions.shuffle(&mut rand::rng());

    // Insert versions into the database
    for version_str in versions.iter() {
        let version = test_version(version_str);
        let manifest_path = test_manifest_path(&dataset_name, &version);

        datasets::insert(
            &mut *conn,
            owner,
            dataset_name.clone(),
            version,
            &manifest_path,
        )
        .await
        .expect("should insert dataset");
    }

    //* When
    // Get next page using cursor (limit 5)
    let result = datasets::list_versions_by_name_next_page(
        &mut *conn,
        dataset_name.clone(),
        5,
        cursor_version.clone(),
    )
    .await
    .expect("Failed to list second page");

    //* Then
    // Should return 5 items (all versions before cursor v10.0.0)
    assert_eq!(result.len(), 5, "should have 5 items");

    // Assert first item: #1 - v2.0.0
    assert_eq!(
        result[0].to_string(),
        "2.0.0",
        "item #1 should be version 2.0.0"
    );

    // Assert second item: #2 - v1.10.0
    assert_eq!(
        result[1].to_string(),
        "1.10.0",
        "item #2 should be version 1.10.0"
    );

    // Assert third item: #3 - v1.9.0
    assert_eq!(
        result[2].to_string(),
        "1.9.0",
        "item #3 should be version 1.9.0"
    );

    // Assert fourth item: #4 - v1.2.10
    assert_eq!(
        result[3].to_string(),
        "1.2.10",
        "item #4 should be version 1.2.10"
    );

    // Assert fifth item: #5 - v1.2.3
    assert_eq!(
        result[4].to_string(),
        "1.2.3",
        "item #5 should be version 1.2.3"
    );
}

/// Helper to parse version strings
fn test_version(version: &str) -> VersionOwned {
    version.parse().expect("should parse valid version")
}

/// Helper to generate manifest path strings from the dataset name and version
fn test_manifest_path(name: &Name<'_>, version: &Version<'_>) -> String {
    format!("{}__{}.json", name, version.to_string().replace('.', "-"))
}
