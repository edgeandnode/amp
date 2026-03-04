//! Pagination tests for location listing

use crate::{
    datasets::{DatasetName, DatasetNamespace},
    manifests::ManifestHash,
    physical_table::{self, TableName},
    physical_table_revision::{self, LocationId, TablePath},
    tests::helpers::{register_table_and_revision, setup_test_db},
};

#[tokio::test]
async fn list_locations_first_page_when_empty() {
    //* Given
    let (_db, conn) = setup_test_db().await;

    //* When
    let locations = physical_table_revision::list(&conn, 10, None as Option<LocationId>)
        .await
        .expect("Failed to list locations");

    //* Then
    assert!(
        locations.is_empty(),
        "list should return empty result when no locations exist"
    );
}

#[tokio::test]
async fn list_locations_first_page_respects_limit() {
    //* Given
    let (_db, conn) = setup_test_db().await;

    let namespace = DatasetNamespace::from_ref_unchecked("test-namespace");
    let name = DatasetName::from_ref_unchecked("test-dataset");
    let hash = ManifestHash::from_ref_unchecked(
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    );

    // Create 5 locations with unique table names to avoid unique constraint violation
    for i in 0..5 {
        let table_name = TableName::from_owned_unchecked(format!("test_table_{}", i));
        let path = TablePath::from_owned_unchecked(format!(
            "test-dataset/test_table_{}/revision-{}",
            i, i
        ));
        let location_id =
            register_table_and_revision(&conn, &namespace, &name, &hash, &table_name, &path)
                .await
                .expect("Failed to insert location");
        physical_table::mark_active_by_id(
            &conn,
            location_id,
            &namespace,
            &name,
            &hash,
            &table_name,
        )
        .await
        .expect("Failed to mark location active");

        // Small delay to ensure different timestamps
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }

    //* When
    let locations = physical_table_revision::list(&conn, 3, None as Option<LocationId>)
        .await
        .expect("Failed to list locations");

    //* Then
    assert_eq!(
        locations.len(),
        3,
        "list should respect limit and return 3 locations"
    );
    assert!(
        locations[0].id > locations[1].id,
        "list should return locations in descending ID order (newest first)"
    );
    assert!(
        locations[1].id > locations[2].id,
        "list should maintain descending ID order throughout page"
    );
    for location in &locations {
        let meta: serde_json::Value = serde_json::from_str(location.metadata.as_str())
            .expect("metadata should be valid JSON");
        assert!(
            meta["table_name"]
                .as_str()
                .expect("table_name should be a JSON string")
                .starts_with("test_table_"),
            "list should return locations with correct table_name prefix"
        );
        assert!(
            location.active,
            "list should return locations with persisted active status"
        );
    }
}

#[tokio::test]
async fn list_locations_next_page_uses_cursor() {
    //* Given
    let (_db, conn) = setup_test_db().await;

    let namespace = DatasetNamespace::from_ref_unchecked("test-namespace");
    let name = DatasetName::from_ref_unchecked("test-dataset");
    let hash = ManifestHash::from_ref_unchecked(
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    );

    // Create 10 locations with unique table names to avoid unique constraint violation
    let mut all_location_ids = Vec::new();
    for i in 0..10 {
        let table_name = TableName::from_owned_unchecked(format!("test_table_page_{}", i));
        let path = TablePath::from_owned_unchecked(format!(
            "test-dataset/test_table_page_{}/page-revision-{}",
            i, i
        ));
        let location_id =
            register_table_and_revision(&conn, &namespace, &name, &hash, &table_name, &path)
                .await
                .expect("Failed to insert location");
        all_location_ids.push(location_id);
        physical_table::mark_active_by_id(
            &conn,
            location_id,
            &namespace,
            &name,
            &hash,
            &table_name,
        )
        .await
        .expect("Failed to mark location active");

        // Small delay to ensure different timestamps
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }

    // Get the first page to establish cursor
    let first_page = physical_table_revision::list(&conn, 3, None as Option<LocationId>)
        .await
        .expect("Failed to list first page");
    let cursor = first_page
        .last()
        .expect("First page should not be empty")
        .id;

    //* When
    let second_page = physical_table_revision::list(&conn, 3, Some(cursor))
        .await
        .expect("Failed to list second page");

    //* Then
    assert_eq!(
        second_page.len(),
        3,
        "list should respect limit for subsequent pages"
    );
    // Verify no overlap with first page
    let first_page_ids: Vec<_> = first_page.iter().map(|l| l.id).collect();
    for location in &second_page {
        assert!(
            !first_page_ids.contains(&location.id),
            "list should not return locations from previous page (no overlap)"
        );
    }
    // Verify ordering
    assert!(
        second_page[0].id > second_page[1].id,
        "list should maintain descending ID order on second page"
    );
    assert!(
        second_page[1].id > second_page[2].id,
        "list should maintain descending ID order throughout second page"
    );
    // Verify cursor worked correctly
    assert!(
        cursor > second_page[0].id,
        "list should use cursor to exclude locations with ID >= cursor"
    );
}
